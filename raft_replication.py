"""
Raft Log Replication Module
Handles log replication, consistency checking, and quorum commit logic.
"""

import logging
from typing import List

import raft_pb2
import raft_pb2_grpc
from raft_state import NodeState, LogEntry

logger = logging.getLogger(__name__)


class ReplicationManager:
    """Manages log replication and consistency"""
    
    def __init__(self, node):
        self.node = node
    
    def handle_append_entries(
        self, request: raft_pb2.AppendEntriesRequest, context
    ) -> raft_pb2.AppendEntriesResponse:
        """
        Handle AppendEntries RPC (Raft paper, Figure 2, Receiver).
        
        Used for both heartbeats (empty entries) and log replication.
        
        Rules:
        1. Reply false if request.term < currentTerm
        2. Reply false if log doesn't contain entry at prevLogIndex with prevLogTerm
        3. If existing entry conflicts, delete it and all following entries
        4. Append any new entries not already in the log
        5. If leaderCommit > commitIndex, update commitIndex
        """
        response = raft_pb2.AppendEntriesResponse()

        with self.node.lock:
            # Rule 1: Reply false if request.term < currentTerm
            if request.term < self.node.current_term:
                response.term = self.node.current_term
                response.success = False
                response.lastLogIndex = len(self.node.log) - 1 if self.node.log else -1
                return response

            # If we get a heartbeat from higher term, become follower
            if request.term > self.node.current_term:
                from raft_election import ElectionManager
                ElectionManager(self.node).become_follower(request.term)
            elif self.node.state != NodeState.FOLLOWER:
                self.node.state = NodeState.FOLLOWER

            self.node._reset_election_timeout()
            import time
            self.node.last_heartbeat_time = time.time()

            # Rule 2: Consistency check - verify prevLogIndex and prevLogTerm match
            prev_log_index = request.prevLogIndex
            prev_log_term = request.prevLogTerm

            if prev_log_index >= 0:
                # Check: does our log have an entry at prevLogIndex?
                if prev_log_index >= len(self.node.log):
                    # Our log is too short
                    response.term = self.node.current_term
                    response.success = False
                    response.lastLogIndex = len(self.node.log) - 1 if self.node.log else -1
                    return response
                
                # Check: does the entry at prevLogIndex have the right term?
                if self.node.log[prev_log_index].term != prev_log_term:
                    # Log conflict! Delete conflicting entry and all that follow
                    self.node.log = self.node.log[:prev_log_index]
                    response.term = self.node.current_term
                    response.success = False
                    response.lastLogIndex = len(self.node.log) - 1 if self.node.log else -1
                    return response

            # Rule 4: Append any new entries not already in the log
            for i, entry in enumerate(request.entries):
                new_log_index = prev_log_index + 1 + i
                new_entry = LogEntry(term=entry.term, command=entry.command)
                
                if new_log_index >= len(self.node.log):
                    # New entry, append it
                    self.node.log.append(new_entry)
                else:
                    # Entry exists, check for conflict (Rule 3)
                    if self.node.log[new_log_index].term != entry.term:
                        # Conflict: delete this and all following entries
                        self.node.log = self.node.log[:new_log_index]
                        self.node.log.append(new_entry)

            # Rule 5: Update commitIndex if leader is ahead
            if request.leaderCommit > self.node.commit_index:
                last_new_index = (
                    prev_log_index + len(request.entries)
                    if request.entries
                    else prev_log_index
                )
                self.node.commit_index = min(request.leaderCommit, last_new_index)
                
                if self.node.commit_index > self.node.last_applied:
                    msg = (
                        f"[TERM {self.node.current_term}] Node {self.node.node_id} committed up to "
                        f"index {self.node.commit_index} (last_applied={self.node.last_applied})"
                    )
                    print(msg)
                    logger.info(msg)

            response.term = self.node.current_term
            response.success = True
            response.lastLogIndex = len(self.node.log) - 1 if self.node.log else -1

        return response
    
    def send_append_entries(self, peer_id: int, up_to_index: int = -1):
        """
        Send AppendEntries RPC to peer (Raft paper, Figure 2, Sender).
        
        SENDER SIDE LOG REPLICATION:
        1. Send entries from nextIndex[peer] onwards
        2. If peer rejects, decrement nextIndex and retry (CATCH-UP)
        3. If peer accepts, update nextIndex and matchIndex
        4. Check if we can commit new entries to majority
        """
        # Check if peer is partitioned (blocked)
        if self.node._is_peer_blocked(peer_id):
            return

        try:
            import grpc
            channel = grpc.insecure_channel(
                f"localhost:{5000 + peer_id}",
                options=[("grpc.keepalive_time_ms", 10000)],
            )
            stub = raft_pb2_grpc.RaftServiceStub(channel)

            with self.node.lock:
                if self.node.state != NodeState.LEADER:
                    channel.close()
                    return

                next_idx = self.node.next_index[peer_id]

                # Prepare entries to send
                entries_to_send = []
                if up_to_index == -1 or up_to_index >= next_idx:
                    entries_to_send = self.node.log[next_idx:]

                # Prepare prevLogIndex and prevLogTerm for consistency check
                prev_log_index = next_idx - 1
                prev_log_term = 0
                if prev_log_index >= 0 and prev_log_index < len(self.node.log):
                    prev_log_term = self.node.log[prev_log_index].term

                # Convert LogEntry to protobuf
                pb_entries = [
                    raft_pb2.LogEntry(term=entry.term, command=entry.command)
                    for entry in entries_to_send
                ]

                request = raft_pb2.AppendEntriesRequest(
                    term=self.node.current_term,
                    leaderId=self.node.node_id,
                    prevLogIndex=prev_log_index,
                    prevLogTerm=prev_log_term,
                    entries=pb_entries,
                    leaderCommit=self.node.commit_index,
                )

            response = stub.AppendEntries(
                request, timeout=self.node.config.rpc_timeout
            )

            with self.node.lock:
                if response.term > self.node.current_term:
                    # Higher term seen, become follower
                    from raft_election import ElectionManager
                    ElectionManager(self.node).become_follower(response.term)
                elif self.node.state == NodeState.LEADER:
                    if response.success:
                        # SUCCESS: Peer accepted the entries
                        new_match_index = next_idx + len(entries_to_send) - 1
                        self.node.match_index[peer_id] = new_match_index
                        self.node.next_index[peer_id] = new_match_index + 1

                        # Check if we can commit new entries
                        self._update_commit_index()

                        if entries_to_send:
                            msg = (
                                f"[TERM {self.node.current_term}] Peer {peer_id} "
                                f"replicated entries, nextIndex now {self.node.next_index[peer_id]}"
                            )
                            logger.info(msg)
                    else:
                        # FAILURE: Log conflict, decrement nextIndex and retry
                        self.node.next_index[peer_id] = max(0, self.node.next_index[peer_id] - 1)
                        msg = (
                            f"[TERM {self.node.current_term}] Peer {peer_id} rejected "
                            f"AppendEntries, decrement nextIndex to {self.node.next_index[peer_id]}"
                        )
                        logger.info(msg)

            channel.close()
        except Exception:
            # Silently ignore connection errors (will retry on next loop)
            pass
    
    def _update_commit_index(self):
        """
        Update commitIndex if a new entry is replicated to majority.
        
        QUORUM COMMIT:
        For each log index, if replicated to majority AND from current term,
        it's safe to commit.
        """
        quorum = (len(self.node.peers) + 1) // 2 + 1

        for idx in range(self.node.commit_index + 1, len(self.node.log)):
            count = 1  # Count self
            for peer_id in self.node.peers:
                if self.node.match_index.get(peer_id, 0) >= idx:
                    count += 1

            # Can only commit entries from current term
            if count >= quorum and self.node.log[idx].term == self.node.current_term:
                self.node.commit_index = idx
                msg = (
                    f"[TERM {self.node.current_term}] Leader Node {self.node.node_id} "
                    f"advanced commitIndex to {idx} (quorum={quorum}, count={count})"
                )
                print(msg)
                logger.info(msg)
            else:
                # Can't skip commits, so stop at first uncommittable entry
                break
    
    def send_heartbeat(self, peer_id: int):
        """Send a heartbeat (empty AppendEntries) to peer"""
        self.send_append_entries(peer_id)
