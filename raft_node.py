"""
Raft Consensus Algorithm - Main Node Implementation
Orchestrates all Raft components: election, replication, and client interaction.

FAILURE THRESHOLD EXPLANATION (Quorum Requirement):
===================================================
For a cluster to make progress (commit new log entries), it must have a QUORUM
of active nodes. A quorum is defined as: ceil((N + 1) / 2) nodes, where N is
the total number of nodes.

Examples:
- 5 nodes: quorum = 3. If 3+ are active, can commit. If 2 or fewer are active, CANNOT.
- 6 nodes: quorum = 4. If 4+ are active, can commit. If 3 or fewer are active, CANNOT.

WHY THIS MATTERS:
- A leader must be in the quorum to commit new entries (it needs majority support).
- If a partition occurs and the leader ends up in the minority partition, it CANNOT
  make progress (even though it's still a "leader" in its own partition).
- If followers are in the majority partition without the original leader, they will
  hold a new election and elect a NEW leader from the majority.

RECONNECTION (CATCH-UP):
When a node reconnects after being offline:
1. The leader tries to send AppendEntries with prevLogIndex/prevLogTerm.
2. If the follower's log doesn't match, the leader receives success=False.
3. The leader decrements nextIndex for that follower and RETRIES.
4. This continues until the leader finds the common point in logs.
5. Then the leader sends all missing entries to bring the follower's log up to date.
6. Once replicated to majority, those entries are committed.
"""

import grpc
import time
import random
import threading
import logging
from concurrent import futures
from typing import Dict, List, Set, Optional

import raft_pb2
import raft_pb2_grpc

from raft_state import NodeState, LogEntry, NodeConfig
from raft_election import ElectionManager
from raft_replication import ReplicationManager
from raft_client import ClientManager

# Configure logging
logger = logging.getLogger(__name__)


class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    """
    Raft consensus algorithm node implementation.
    
    Orchestrates three main components:
    1. ElectionManager - Handles leader election and term management
    2. ReplicationManager - Handles log replication and consistency
    3. ClientManager - Handles client requests and network partitions
    """

    def __init__(self, node_id: int, peers: List[int], config: NodeConfig = None):
        """
        Initialize a Raft node.

        Args:
            node_id: Unique identifier for this node
            peers: List of peer node IDs in the cluster
            config: Configuration object for election/heartbeat timings
        """
        self.node_id = node_id
        self.peers = [p for p in peers if p != node_id]  # Exclude self from peers
        self.config = config or NodeConfig()

        # ===== PERSISTENT STATE (on all servers, must survive reboots) =====
        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.log: List[LogEntry] = []  # List of log entries

        # ===== VOLATILE STATE (on all servers) =====
        self.commit_index = 0  # Index of highest log entry known to be committed
        self.last_applied = 0  # Index of highest log entry applied to state machine

        # ===== VOLATILE STATE (Leader only, reinitialized after election) =====
        self.next_index: Dict[int, int] = {}  # For each server: index of next log to send
        self.match_index: Dict[int, int] = {}  # For each server: index of highest log replicated

        # ===== NODE STATE =====
        self.state = NodeState.FOLLOWER
        self.votes_received: Set[int] = set()  # Votes received in current election

        # ===== NETWORK PARTITION SIMULATION =====
        self.blocked_peers: Set[int] = set()  # Set of peer IDs we cannot reach

        # ===== THREAD SYNCHRONIZATION =====
        self.lock = threading.RLock()
        self.election_event = threading.Event()

        # ===== TIMING =====
        self.election_timeout = 0
        self.last_heartbeat_time = time.time()

        self._reset_election_timeout()
        self._init_leader_state()

        # ===== MODULE MANAGERS =====
        self.election_mgr = ElectionManager(self)
        self.replication_mgr = ReplicationManager(self)
        self.client_mgr = ClientManager(self)

    def _reset_election_timeout(self):
        """Reset election timeout to a random value"""
        self.election_timeout = (
            time.time()
            + random.uniform(
                self.config.election_timeout_min, self.config.election_timeout_max
            )
        )

    def _init_leader_state(self):
        """
        Initialize leader-specific state.
        Called when transitioning to LEADER state.
        """
        for peer in self.peers:
            self.next_index[peer] = len(self.log)
            self.match_index[peer] = 0

    # =========================================================================
    # RPC HANDLERS (gRPC Service Implementation)
    # =========================================================================

    def RequestVote(self, request: raft_pb2.VoteRequest, context) -> raft_pb2.VoteResponse:
        """Handle RequestVote RPC - delegated to ElectionManager"""
        return self.election_mgr.handle_request_vote(request, context)

    def AppendEntries(
        self, request: raft_pb2.AppendEntriesRequest, context
    ) -> raft_pb2.AppendEntriesResponse:
        """Handle AppendEntries RPC - delegated to ReplicationManager"""
        return self.replication_mgr.handle_append_entries(request, context)

    def ClientRequest(
        self, request: raft_pb2.ClientRequestMessage, context
    ) -> raft_pb2.ClientRequestResponse:
        """Handle ClientRequest RPC - delegated to ClientManager"""
        return self.client_mgr.handle_client_request(request, context)

    def SimulatePartition(
        self, request: raft_pb2.PartitionRequest, context
    ) -> raft_pb2.PartitionResponse:
        """Handle SimulatePartition RPC - delegated to ClientManager"""
        return self.client_mgr.handle_simulate_partition(request, context)

    # =========================================================================
    # NETWORK PARTITION HELPERS
    # =========================================================================

    def _is_peer_blocked(self, peer_id: int) -> bool:
        """Check if a peer is blocked (network partition simulation)"""
        with self.lock:
            return peer_id in self.blocked_peers

    # =========================================================================
    # MAIN ELECTION LOOP
    # =========================================================================

    def _run_election_loop(self):
        """
        Main election loop that runs on all nodes (Raft paper, Figure 2).
        
        STATE MACHINE:
        1. FOLLOWER: Wait for heartbeat. If timeout, become CANDIDATE and start election.
        2. CANDIDATE: Wait for votes. If win majority, become LEADER. If timeout, start new election.
        3. LEADER: Send heartbeats periodically. Continue until higher term or partition.
        """
        while True:
            try:
                with self.lock:
                    current_state = self.state
                    current_term = self.current_term
                    
                    # Check if node has been stopped (for failure simulation)
                    if current_state == NodeState.STOPPED:
                        time.sleep(0.1)
                        continue
                    
                    election_timeout_reached = time.time() >= self.election_timeout

                # FOLLOWER: Check for election timeout
                if (
                    current_state == NodeState.FOLLOWER
                    and election_timeout_reached
                ):
                    self.election_mgr.become_candidate()
                    # Send vote requests to all peers
                    for peer in self.peers:
                        thread = threading.Thread(
                            target=self.election_mgr.send_request_vote, args=(peer,)
                        )
                        thread.daemon = True
                        thread.start()

                # CANDIDATE: Check if won election or timeout
                elif current_state == NodeState.CANDIDATE:
                    if self.election_mgr.check_election_won():
                        self.election_mgr.become_leader()
                        # Start sending heartbeats
                        for peer in self.peers:
                            thread = threading.Thread(
                                target=self.replication_mgr.send_heartbeat, args=(peer,)
                            )
                            thread.daemon = True
                            thread.start()
                    elif election_timeout_reached:
                        # Election timeout, start new election
                        self.election_mgr.become_candidate()
                        for peer in self.peers:
                            thread = threading.Thread(
                                target=self.election_mgr.send_request_vote, args=(peer,)
                            )
                            thread.daemon = True
                            thread.start()

                # LEADER: Send periodic heartbeats and replicate logs
                elif current_state == NodeState.LEADER:
                    if (
                        time.time() - self.last_heartbeat_time
                        >= self.config.heartbeat_interval
                    ):
                        for peer in self.peers:
                            thread = threading.Thread(
                                target=self.replication_mgr.send_append_entries, args=(peer,)
                            )
                            thread.daemon = True
                            thread.start()
                        with self.lock:
                            self.last_heartbeat_time = time.time()

                time.sleep(0.05)  # Small sleep to prevent CPU spinning

            except Exception as e:
                print(f"[ERROR] Node {self.node_id} election loop error: {e}")
                time.sleep(0.1)

    # =========================================================================
    # STATE INSPECTION
    # =========================================================================

    def get_state(self) -> Dict:
        """Get current state of the node for monitoring/visualization"""
        with self.lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "term": self.current_term,
                "voted_for": self.voted_for,
                "votes_received": len(self.votes_received)
                if self.state == NodeState.CANDIDATE
                else 0,
                "is_leader": self.state == NodeState.LEADER,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
            }

    # =========================================================================
    # SERVER STARTUP
    # =========================================================================

    def start(self, port: int = None):
        """
        Start the Raft node server and election loop.

        Args:
            port: Port to listen on (default: 5000 + node_id)
        """
        if port is None:
            port = 5000 + self.node_id

        # Create gRPC server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, server)
        server.add_insecure_port(f"[::]:{port}")
        server.start()

        print(f"[STARTUP] Node {self.node_id} started on port {port}")

        # Start election loop in separate thread
        election_thread = threading.Thread(target=self._run_election_loop)
        election_thread.daemon = True
        election_thread.start()

        # Wait for termination
        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            print(f"[SHUTDOWN] Node {self.node_id} shutting down...")
            server.stop(0)
