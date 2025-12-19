"""
Raft Election Module
Handles leader election, voting, and term management.
"""

import threading
import logging
from typing import List, Set

import raft_pb2
import raft_pb2_grpc
from raft_state import NodeState

logger = logging.getLogger(__name__)


class ElectionManager:
    """Manages leader election logic"""
    
    def __init__(self, node):
        self.node = node
    
    def become_candidate(self):
        """
        Transition to CANDIDATE state and start election.
        Increments term and votes for self.
        """
        with self.node.lock:
            self.node.state = NodeState.CANDIDATE
            self.node.current_term += 1
            self.node.voted_for = self.node.node_id
            self.node.votes_received = {self.node.node_id}
            self.node._reset_election_timeout()

        msg = (
            f"[TERM {self.node.current_term}] Node {self.node.node_id} became CANDIDATE "
            f"(votes: {len(self.node.votes_received)}/{len(self.node.peers) + 1})"
        )
        print(msg)
        logger.info(msg)
    
    def become_follower(self, term: int):
        """
        Transition to FOLLOWER state.
        Called when receiving heartbeat from higher term or discovering higher term.
        """
        with self.node.lock:
            if self.node.current_term < term:
                old_state = self.node.state
                self.node.current_term = term
                self.node.state = NodeState.FOLLOWER
                self.node.voted_for = None
                self.node._reset_election_timeout()
                msg = f"[TERM {self.node.current_term}] Node {self.node.node_id} reverted to FOLLOWER"
                print(msg)
                logger.info(msg)
    
    def become_leader(self):
        """
        Transition to LEADER state.
        Initialize leader-specific state (nextIndex, matchIndex).
        """
        with self.node.lock:
            if self.node.state != NodeState.LEADER:
                self.node.state = NodeState.LEADER
                self.node._init_leader_state()
                self.node.last_heartbeat_time = __import__('time').time()
                msg = f"[TERM {self.node.current_term}] Node {self.node.node_id} became LEADER"
                print(msg)
                logger.info(msg)
                return True
        return False
    
    def check_election_won(self) -> bool:
        """Check if candidate has won election (received quorum of votes)"""
        with self.node.lock:
            total_nodes = len(self.node.peers) + 1
            majority = (total_nodes // 2) + 1
            votes = len(self.node.votes_received)
            return votes >= majority
    
    def handle_request_vote(self, request: raft_pb2.VoteRequest, context) -> raft_pb2.VoteResponse:
        """
        Handle RequestVote RPC (Raft paper, Figure 2, Receiver).
        
        Rules:
        1. Reply false if request.term < currentTerm
        2. If request.term > currentTerm, become follower with new term
        3. If votedFor is null or candidateId, vote for candidate
        """
        response = raft_pb2.VoteResponse()

        with self.node.lock:
            # Rule 1: Reply false if request.term < currentTerm
            if request.term < self.node.current_term:
                response.term = self.node.current_term
                response.voteGranted = False
                return response

            # Rule 2: If request.term > currentTerm, become follower
            if request.term > self.node.current_term:
                self.become_follower(request.term)

            # Rule 3: Vote for candidate if votedFor is null or candidateId
            if (self.node.voted_for is None or self.node.voted_for == request.candidateId):
                self.node.voted_for = request.candidateId
                self.node._reset_election_timeout()
                response.voteGranted = True
                msg = (
                    f"[TERM {self.node.current_term}] Node {self.node.node_id} voted for "
                    f"Node {request.candidateId}"
                )
                print(msg)
                logger.info(msg)
            else:
                response.voteGranted = False
                msg = (
                    f"[TERM {self.node.current_term}] Node {self.node.node_id} rejected vote "
                    f"from Node {request.candidateId} (already voted for {self.node.voted_for})"
                )
                print(msg)
                logger.info(msg)

            response.term = self.node.current_term

        return response
    
    def send_request_vote(self, peer_id: int):
        """
        Send RequestVote RPC to a peer during election.
        Retry silently if peer is unreachable or blocked.
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
                    total_votes = len(self.node.votes_received)
                    majority = (len(self.node.peers) + 1) // 2 + 1
                    msg = (
                        f"[TERM {self.node.current_term}] Node {self.node.node_id} received vote "
                        f"from Node {peer_id} (Total votes: {total_votes})"
                    )
                    print(msg)
                    logger.info(msg)

            channel.close()
        except Exception:
            # Silently ignore connection errors
            pass
