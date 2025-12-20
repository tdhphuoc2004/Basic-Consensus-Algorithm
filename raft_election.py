"""
Raft Election Module
Handles leader election, voting, and term management.
Single Responsibility: Election logic only.
"""

import time
import grpc

import raft_pb2
import raft_pb2_grpc
from raft_state import NodeState


class ElectionManager:
    """Manages leader election logic"""
    
    def __init__(self, node):
        self.node = node
    
    def become_candidate(self):
        """Transition to CANDIDATE state and start election."""
        with self.node.lock:
            self.node.state = NodeState.CANDIDATE
            self.node.current_term += 1
            self.node.voted_for = self.node.node_id
            self.node.votes_received = {self.node.node_id}
            self.node._reset_election_timeout()
            self.node.storage.save_persistent_state(
                self.node.current_term, self.node.voted_for, self.node.log
            )
    
    def become_follower(self, term: int):
        """Transition to FOLLOWER state (STOPPED nodes remain dead)."""
        with self.node.lock:
            if self.node.state == NodeState.STOPPED:
                return
            
            if self.node.current_term < term:
                self.node.current_term = term
                self.node.state = NodeState.FOLLOWER
                self.node.voted_for = None
                self.node._reset_election_timeout()
                self.node.storage.save_persistent_state(
                    self.node.current_term, self.node.voted_for, self.node.log
                )
    
    def become_leader(self):
        """Transition to LEADER state and initialize leader-specific state."""
        with self.node.lock:
            if self.node.state != NodeState.LEADER:
                self.node.state = NodeState.LEADER
                self.node._init_leader_state()
                self.node.last_heartbeat_time = time.time()
                return True
        return False
    
    def check_election_won(self) -> bool:
        """Check if candidate has won election (received quorum of votes)."""
        with self.node.lock:
            total_nodes = len(self.node.peers) + 1
            majority = (total_nodes // 2) + 1
            return len(self.node.votes_received) >= majority
    
    def handle_request_vote(self, request: raft_pb2.VoteRequest, context) -> raft_pb2.VoteResponse:
        """Handle RequestVote RPC (Raft paper, Figure 2, Receiver)."""
        response = raft_pb2.VoteResponse()

        with self.node.lock:
            if self.node.state == NodeState.STOPPED:
                response.term = self.node.current_term
                response.voteGranted = False
                return response
            
            if request.term < self.node.current_term:
                response.term = self.node.current_term
                response.voteGranted = False
                return response

            if request.term > self.node.current_term:
                self.become_follower(request.term)

            if self.node.voted_for is None or self.node.voted_for == request.candidateId:
                self.node.voted_for = request.candidateId
                self.node._reset_election_timeout()
                self.node.storage.save_persistent_state(
                    self.node.current_term, self.node.voted_for, self.node.log
                )
                response.voteGranted = True
            else:
                response.voteGranted = False

            response.term = self.node.current_term

        return response
    
    def send_request_vote(self, peer_id: int):
        """Send RequestVote RPC to a peer during election."""
        if self.node._is_peer_blocked(peer_id):
            return

        try:
            channel = grpc.insecure_channel(
                f"localhost:{5000 + peer_id}",
                options=[("grpc.keepalive_time_ms", 10000)],
            )
            stub = raft_pb2_grpc.RaftServiceStub(channel)

            with self.node.lock:
                if self.node.state != NodeState.CANDIDATE:
                    channel.close()
                    return

                request = raft_pb2.VoteRequest(
                    term=self.node.current_term,
                    candidateId=self.node.node_id,
                    lastLogIndex=len(self.node.log) - 1,
                    lastLogTerm=self.node.log[-1].term if self.node.log else 0,
                )

            response = stub.RequestVote(request, timeout=self.node.config.rpc_timeout)

            with self.node.lock:
                if response.term > self.node.current_term:
                    self.become_follower(response.term)
                elif response.voteGranted and self.node.state == NodeState.CANDIDATE:
                    self.node.votes_received.add(peer_id)

            channel.close()
        except Exception:
            pass
