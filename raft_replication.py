"""
Raft Log Replication Module
Handles log replication, consistency checking, and quorum commit logic.
Single Responsibility: Replication logic only.
"""

import time
import grpc

import raft_pb2
import raft_pb2_grpc
from raft_state import NodeState, LogEntry


class ReplicationManager:
    """Manages log replication and consistency"""
    
    def __init__(self, node):
        self.node = node
    
    def handle_append_entries(
        self, request: raft_pb2.AppendEntriesRequest, context
    ) -> raft_pb2.AppendEntriesResponse:
        """Handle AppendEntries RPC (Raft paper, Figure 2, Receiver)."""
        response = raft_pb2.AppendEntriesResponse()

        with self.node.lock:
            if self.node.state == NodeState.STOPPED:
                response.term = self.node.current_term
                response.success = False
                response.lastLogIndex = len(self.node.log) - 1 if self.node.log else -1
                return response
            
            if request.term < self.node.current_term:
                response.term = self.node.current_term
                response.success = False
                response.lastLogIndex = len(self.node.log) - 1 if self.node.log else -1
                return response

            if request.term > self.node.current_term:
                from raft_election import ElectionManager
                ElectionManager(self.node).become_follower(request.term)
            elif self.node.state != NodeState.FOLLOWER:
                self.node.state = NodeState.FOLLOWER

            self.node._reset_election_timeout()
            self.node.last_heartbeat_time = time.time()

            prev_log_index = request.prevLogIndex
            prev_log_term = request.prevLogTerm

            if prev_log_index >= 0:
                if prev_log_index >= len(self.node.log):
                    response.term = self.node.current_term
                    response.success = False
                    response.lastLogIndex = len(self.node.log) - 1 if self.node.log else -1
                    return response
                
                if self.node.log[prev_log_index].term != prev_log_term:
                    self.node.log = self.node.log[:prev_log_index]
                    self.node.storage.save_persistent_state(
                        self.node.current_term, self.node.voted_for, self.node.log
                    )
                    response.term = self.node.current_term
                    response.success = False
                    response.lastLogIndex = len(self.node.log) - 1 if self.node.log else -1
                    return response

            for i, entry in enumerate(request.entries):
                new_log_index = prev_log_index + 1 + i
                new_entry = LogEntry(term=entry.term, command=entry.command)
                
                if new_log_index >= len(self.node.log):
                    self.node.log.append(new_entry)
                elif self.node.log[new_log_index].term != entry.term:
                    self.node.log = self.node.log[:new_log_index]
                    self.node.log.append(new_entry)
            
            if request.entries:
                self.node.storage.save_persistent_state(
                    self.node.current_term, self.node.voted_for, self.node.log
                )

            if request.leaderCommit > self.node.commit_index:
                last_new_index = (
                    prev_log_index + len(request.entries)
                    if request.entries
                    else prev_log_index
                )
                self.node.commit_index = min(request.leaderCommit, last_new_index)
                self._apply_committed_entries()
                self.node.storage.save_state_machine(self.node.state_machine.get_data())

            response.term = self.node.current_term
            response.success = True
            response.lastLogIndex = len(self.node.log) - 1 if self.node.log else -1

        return response
    
    def send_append_entries(self, peer_id: int, up_to_index: int = -1):
        """Send AppendEntries RPC to peer."""
        if self.node._is_peer_blocked(peer_id):
            return

        try:
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
                entries_to_send = self.node.log[next_idx:] if up_to_index == -1 or up_to_index >= next_idx else []
                
                prev_log_index = next_idx - 1
                prev_log_term = self.node.log[prev_log_index].term if 0 <= prev_log_index < len(self.node.log) else 0

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

            response = stub.AppendEntries(request, timeout=self.node.config.rpc_timeout)

            with self.node.lock:
                if response.term > self.node.current_term:
                    from raft_election import ElectionManager
                    ElectionManager(self.node).become_follower(response.term)
                elif self.node.state == NodeState.LEADER:
                    if response.success:
                        new_match_index = next_idx + len(entries_to_send) - 1
                        self.node.match_index[peer_id] = new_match_index
                        self.node.next_index[peer_id] = new_match_index + 1
                        self._update_commit_index()
                    else:
                        self.node.next_index[peer_id] = max(0, self.node.next_index[peer_id] - 1)

            channel.close()
        except Exception:
            pass
    
    def _apply_committed_entries(self):
        """Apply all committed but not yet applied entries to state machine."""
        while self.node.last_applied < self.node.commit_index:
            self.node.last_applied += 1
            entry = self.node.log[self.node.last_applied]
            self.node.state_machine.apply_command(entry.command)
    
    def _update_commit_index(self):
        """Update commitIndex if a new entry is replicated to majority."""
        quorum = (len(self.node.peers) + 1) // 2 + 1

        for idx in range(self.node.commit_index + 1, len(self.node.log)):
            count = 1
            for peer_id in self.node.peers:
                if self.node.match_index.get(peer_id, 0) >= idx:
                    count += 1

            if count >= quorum and self.node.log[idx].term == self.node.current_term:
                self.node.commit_index = idx
                self._apply_committed_entries()
            else:
                break
    
    def send_heartbeat(self, peer_id: int):
        """Send a heartbeat (empty AppendEntries) to peer."""
        self.send_append_entries(peer_id)
