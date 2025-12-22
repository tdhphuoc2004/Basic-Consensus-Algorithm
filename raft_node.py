"""
Raft Consensus Algorithm - Main Node Implementation
Orchestrates all Raft components: election, replication, and client interaction.
"""

import grpc
import time
import random
import threading
from concurrent import futures
from typing import Dict, List, Set, Optional

import raft_pb2
import raft_pb2_grpc

from raft_state import NodeState, LogEntry, NodeConfig
from raft_election import ElectionManager
from raft_replication import ReplicationManager
from raft_client import ClientManager
from raft_statemachine import KeyValueStore
from raft_storage import RaftStorage


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    """Raft consensus algorithm node implementation."""

    def __init__(self, node_id: int, peers: List[int], config: NodeConfig = None):
        """Initialize a Raft node."""
        self.node_id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.cluster_size = len(peers)  # Total cluster size (including self) for quorum calculation
        self.config = config or NodeConfig()
        self.storage = RaftStorage(node_id)
        self.current_term, self.voted_for, self.log = self.storage.load_persistent_state()
        self.commit_index = 0
        self.last_applied = -1  # -1 means nothing applied yet
        self.state = NodeState.FOLLOWER
        self.votes_received: Set[int] = set()
        self.blocked_peers: Set[int] = set()
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}
        self.known_leader_id: Optional[int] = None  # Track known leader from heartbeats
        self.lock = threading.RLock()
        self.election_event = threading.Event()
        self.election_timeout = 0
        self.last_heartbeat_time = time.time()
        self._reset_election_timeout()
        self._init_leader_state()
        self.election_mgr = ElectionManager(self)
        self.replication_mgr = ReplicationManager(self)
        self.client_mgr = ClientManager(self)
        self.state_machine = KeyValueStore(node_id)

    def _reset_election_timeout(self):
        """Reset election timeout to a random value (Raft paper Section 5.2).
        
        Election timeout is randomly chosen from [election_timeout_min, election_timeout_max] seconds.
        This randomization reduces the chance of split votes.
        """
        timeout_sec = random.uniform(
            self.config.election_timeout_min,
            self.config.election_timeout_max
        )
        self.election_timeout = time.time() + timeout_sec

    def _init_leader_state(self):
        """Initialize leader-specific state."""
        for peer in self.peers:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = -1  # -1 means nothing matched yet
        
        if len(self.log) > 0 and self.log[-1].term < self.current_term:
            noop_entry = LogEntry(term=self.current_term, command="NO-OP")
            self.log.append(noop_entry)
            self.storage.save_persistent_state(
                self.current_term, self.voted_for, self.log
            )

    def RequestVote(self, request: raft_pb2.VoteRequest, context) -> raft_pb2.VoteResponse:
        return self.election_mgr.handle_request_vote(request, context)

    def PreVote(self, request: raft_pb2.PreVoteRequest, context) -> raft_pb2.PreVoteResponse:
        return self.election_mgr.handle_pre_vote(request, context)

    def AppendEntries(self, request: raft_pb2.AppendEntriesRequest, context) -> raft_pb2.AppendEntriesResponse:
        return self.replication_mgr.handle_append_entries(request, context)

    def ClientRequest(self, request: raft_pb2.ClientRequestMessage, context) -> raft_pb2.ClientRequestResponse:
        return self.client_mgr.handle_client_request(request, context)

    def SimulatePartition(self, request: raft_pb2.PartitionRequest, context) -> raft_pb2.PartitionResponse:
        return self.client_mgr.handle_simulate_partition(request, context)

    def HealPartition(self, request: raft_pb2.HealPartitionRequest, context) -> raft_pb2.HealPartitionResponse:
        return self.client_mgr.handle_heal_partition(request, context)

    def GetPartitionStatus(self, request: raft_pb2.PartitionStatusRequest, context) -> raft_pb2.PartitionStatusResponse:
        return self.client_mgr.handle_partition_status(request, context)

    def _is_peer_blocked(self, peer_id: int) -> bool:
        """Check if a peer is blocked (network partition simulation)."""
        with self.lock:
            return peer_id in self.blocked_peers

    def _run_election_loop(self):
        """Main election loop managing state transitions."""
        while True:
            try:
                # Check if stopped without holding lock long
                is_stopped = False
                acquired = self.lock.acquire(timeout=0.05)
                if acquired:
                    try:
                        is_stopped = (self.state == NodeState.STOPPED)
                    finally:
                        self.lock.release()
                else:
                    # Could not acquire lock, skip this iteration
                    time.sleep(0.05)
                    continue
                
                if is_stopped:
                    # Sleep longer when stopped to reduce CPU and lock contention
                    time.sleep(0.5)
                    continue
                
                # Use timeout for main lock acquisition too
                acquired = self.lock.acquire(timeout=0.1)
                if not acquired:
                    time.sleep(0.05)
                    continue
                try:
                    current_state = self.state
                    election_timeout_reached = time.time() >= self.election_timeout
                finally:
                    self.lock.release()

                if current_state == NodeState.FOLLOWER and election_timeout_reached:
                    # Use Pre-Vote to avoid disrupting cluster with stale term
                    self.election_mgr.start_election()
                    # If pre-vote succeeded and became candidate, send vote requests
                    with self.lock:
                        is_candidate = self.state == NodeState.CANDIDATE
                    if is_candidate:
                        for peer in self.peers:
                            threading.Thread(target=self.election_mgr.send_request_vote, args=(peer,), daemon=True).start()

                elif current_state == NodeState.CANDIDATE:
                    if self.election_mgr.check_election_won():
                        self.election_mgr.become_leader()
                        for peer in self.peers:
                            threading.Thread(target=self.replication_mgr.send_heartbeat, args=(peer,), daemon=True).start()
                    elif election_timeout_reached:
                        # Use Pre-Vote to avoid disrupting cluster with stale term
                        self.election_mgr.start_election()
                        with self.lock:
                            is_candidate = self.state == NodeState.CANDIDATE
                        if is_candidate:
                            for peer in self.peers:
                                threading.Thread(target=self.election_mgr.send_request_vote, args=(peer,), daemon=True).start()

                elif current_state == NodeState.LEADER:
                    if time.time() - self.last_heartbeat_time >= self.config.heartbeat_interval:
                        for peer in self.peers:
                            threading.Thread(target=self.replication_mgr.send_append_entries, args=(peer,), daemon=True).start()
                        acquired = self.lock.acquire(timeout=0.1)
                        if acquired:
                            try:
                                self.last_heartbeat_time = time.time()
                            finally:
                                self.lock.release()

                time.sleep(0.05)
            except Exception:
                time.sleep(0.1)

    def get_state(self) -> Dict:
        """Get current state of the node for monitoring."""
        # Use timeout to avoid blocking when node is busy
        acquired = self.lock.acquire(timeout=0.1)
        if not acquired:
            # Return best-effort state without lock
            return {
                "node_id": self.node_id,
                "state": getattr(self.state, 'value', 'busy'),
                "term": getattr(self, 'current_term', 0),
                "voted_for": None,
                "votes_received": 0,
                "is_leader": False,
                "log_length": 0,
                "commit_index": -1,
                "last_applied": -1,
                "state_machine_size": 0,
            }
        try:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "term": self.current_term,
                "voted_for": self.voted_for,
                "votes_received": len(self.votes_received) if self.state == NodeState.CANDIDATE else 0,
                "is_leader": self.state == NodeState.LEADER,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "state_machine_size": self.state_machine.get_size(),
            }
        finally:
            self.lock.release()

    def start(self, port: int = None):
        """Start the Raft node server and election loop."""
        if port is None:
            port = 5000 + self.node_id

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, server)
        server.add_insecure_port(f"127.0.0.1:{port}")
        server.start()
        
        self.grpc_server = server

        election_thread = threading.Thread(target=self._run_election_loop)
        election_thread.daemon = True
        election_thread.start()

        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            server.stop(0)
        except Exception:
            pass

    def restart_server(self, port: int = None):
        """Restart the gRPC server after a node is revived."""
        if port is None:
            port = 5000 + self.node_id

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, server)
        server.add_insecure_port(f"127.0.0.1:{port}")
        server.start()
        
        self.grpc_server = server
