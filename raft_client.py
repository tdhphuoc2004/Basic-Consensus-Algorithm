"""
Raft Client Interaction Module
Handles client requests and network partition simulation.
"""

import logging

import raft_pb2
from raft_state import NodeState, LogEntry

logger = logging.getLogger(__name__)


class ClientManager:
    """Handles client interactions and partitioning"""
    
    def __init__(self, node):
        self.node = node
    
    def handle_client_request(
        self, request: raft_pb2.ClientRequestMessage, context
    ) -> raft_pb2.ClientRequestResponse:
        """
        Handle client request to append command to log.
        
        FLOW:
        1. If not leader, reject and return leader ID
        2. If leader, append command to log
        3. Replicate to followers via AppendEntries
        4. Wait until entry is COMMITTED (replicated to majority)
        5. Return success to client
        
        IMPORTANT: Wait for COMMIT, not just replication!
        """
        response = raft_pb2.ClientRequestResponse()

        with self.node.lock:
            if self.node.state != NodeState.LEADER:
                response.success = False
                response.leaderId = str(self._find_leader())
                response.message = f"Not leader. Current term: {self.node.current_term}"
                return response

            # Leader: append command to log
            new_entry = LogEntry(term=self.node.current_term, command=request.command)
            self.node.log.append(new_entry)
            new_index = len(self.node.log) - 1

            msg = (
                f"[TERM {self.node.current_term}] Leader Node {self.node.node_id} received "
                f"client request: '{request.command}' at index {new_index}"
            )
            print(msg)
            logger.info(msg)

        # Replicate to followers asynchronously
        from raft_replication import ReplicationManager
        repl_mgr = ReplicationManager(self.node)
        import threading
        for peer in self.node.peers:
            thread = threading.Thread(
                target=repl_mgr.send_append_entries, args=(peer, new_index)
            )
            thread.daemon = True
            thread.start()

        # Wait until this entry is committed
        import time
        start_time = time.time()
        while True:
            with self.node.lock:
                if self.node.commit_index >= new_index and self.node.state == NodeState.LEADER:
                    # Entry is committed!
                    response.success = True
                    response.message = f"Command committed at index {new_index}"
                    msg = (
                        f"[TERM {self.node.current_term}] Leader Node {self.node.node_id} "
                        f"committed command: '{request.command}'"
                    )
                    print(msg)
                    logger.info(msg)
                    return response

                if self.node.state != NodeState.LEADER:
                    # We lost leadership
                    response.success = False
                    response.message = "Lost leadership before commit"
                    return response

            time.sleep(0.05)

            # Timeout after 10 seconds
            if time.time() - start_time > 10:
                response.success = False
                response.message = "Timeout waiting for commit"
                return response
    
    def handle_simulate_partition(
        self, request: raft_pb2.PartitionRequest, context
    ) -> raft_pb2.PartitionResponse:
        """
        Simulate network partition by blocking communication with specific nodes.
        
        PARTITION SIMULATION:
        Add node IDs to blocked_peers set. All outgoing RPCs check this set.
        """
        response = raft_pb2.PartitionResponse()
        
        with self.node.lock:
            self.node.blocked_peers = set(request.blockedNodeIds)
            msg = (
                f"[TERM {self.node.current_term}] Node {self.node.node_id} blocking peers: "
                f"{self.node.blocked_peers}"
            )
            print(msg)
            logger.info(msg)

            response.success = True
            response.message = f"Blocked {len(self.node.blocked_peers)} peers"

        return response
    
    def _find_leader(self) -> int:
        """Best effort to find leader ID (heuristic for demo)"""
        return max(self.node.peers) if self.node.peers else None
