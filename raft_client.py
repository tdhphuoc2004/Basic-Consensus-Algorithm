"""
Raft Client Interaction Module
Handles client requests and network partition simulation.
Single Responsibility: Client request handling only.
"""

import time
import threading

import raft_pb2
from raft_state import NodeState, LogEntry


class ClientManager:
    """Handles client interactions and partitioning"""
    
    def __init__(self, node):
        self.node = node
        self.commit_timeout = 3
        self.retry_interval = 0.05
        # Special commands that don't require commit (for health checks in no-quorum situations)
        self.special_commands = {"PING", "STATUS"}
    
    def handle_client_request(
        self, request: raft_pb2.ClientRequestMessage, context
    ) -> raft_pb2.ClientRequestResponse:
        """Handle client request to append command to log."""
        response = raft_pb2.ClientRequestResponse()
        command_upper = request.command.strip().upper()
        
        # Handle special commands (PING, STATUS) - no commit required
        # These work even without quorum
        if command_upper in self.special_commands:
            return self._handle_special_command(command_upper, response)

        with self.node.lock:
            if self.node.state != NodeState.LEADER:
                response.success = False
                response.leaderId = str(self._find_leader())
                response.message = f"Not leader. Current term: {self.node.current_term}"
                return response

            new_entry = LogEntry(term=self.node.current_term, command=request.command)
            self.node.log.append(new_entry)
            new_index = len(self.node.log) - 1
            
            self.node.storage.save_persistent_state(
                self.node.current_term, self.node.voted_for, self.node.log
            )

        self._trigger_replication()

        return self._wait_for_commit(new_index, request.command, response)
    
    def _handle_special_command(
        self, command: str, response: raft_pb2.ClientRequestResponse
    ) -> raft_pb2.ClientRequestResponse:
        """Handle special commands (PING, STATUS) without commit - works without quorum."""
        with self.node.lock:
            is_leader = self.node.state == NodeState.LEADER
            leader_id = self.node.node_id if is_leader else self._find_leader()
            
            if command == "PING":
                response.success = is_leader
                response.leaderId = str(leader_id) if leader_id is not None else ""
                response.message = "PONG" if is_leader else "Not leader"
                return response
            
            elif command == "STATUS":
                response.success = is_leader
                response.leaderId = str(leader_id) if leader_id is not None else ""
                response.message = f"Node {self.node.node_id}: {self.node.state.name}, Term: {self.node.current_term}"
                return response
        
        return response
    
    def _trigger_replication(self):
        """Spawn replication threads for all peers."""
        from raft_replication import ReplicationManager
        repl_mgr = ReplicationManager(self.node)
        
        for peer in self.node.peers:
            thread = threading.Thread(
                target=repl_mgr.send_append_entries, args=(peer, -1)
            )
            thread.daemon = True
            thread.start()
    
    def _wait_for_commit(
        self, new_index: int, command: str, response: raft_pb2.ClientRequestResponse
    ) -> raft_pb2.ClientRequestResponse:
        """Wait until the entry is committed or timeout."""
        from raft_replication import ReplicationManager
        repl_mgr = ReplicationManager(self.node)
        
        start_time = time.time()
        last_replication_attempt = start_time
        
        while True:
            with self.node.lock:
                if self.node.commit_index >= new_index and self.node.state == NodeState.LEADER:
                    command_result = self._apply_and_get_result(new_index)
                    self.node.storage.save_state_machine(self.node.state_machine.get_data())
                    
                    response.success = True
                    response.message = f"Command committed at index {new_index}"
                    response.value = command_result
                    return response

                if self.node.state != NodeState.LEADER:
                    response.success = False
                    response.message = "Lost leadership before commit"
                    return response

            if time.time() - start_time > self.commit_timeout:
                response.success = False
                response.message = "Timeout waiting for commit"
                return response
            
            current_time = time.time()
            if current_time - last_replication_attempt > self.retry_interval:
                for peer in self.node.peers:
                    thread = threading.Thread(
                        target=repl_mgr.send_append_entries, args=(peer, -1)
                    )
                    thread.daemon = True
                    thread.start()
                last_replication_attempt = current_time
            
            time.sleep(0.01)
    
    def _apply_and_get_result(self, target_index: int) -> str:
        """Apply committed entries and return result of target command."""
        command_result = ""
        
        while self.node.last_applied < self.node.commit_index:
            self.node.last_applied += 1
            entry = self.node.log[self.node.last_applied]
            success, result = self.node.state_machine.apply_command(entry.command)
            
            if self.node.last_applied == target_index:
                command_result = result
        
        if not command_result and target_index < len(self.node.log):
            entry = self.node.log[target_index]
            _, command_result = self.node.state_machine.apply_command(entry.command)
        
        return command_result
    
    def handle_simulate_partition(
        self, request: raft_pb2.PartitionRequest, context
    ) -> raft_pb2.PartitionResponse:
        """Simulate network partition by blocking communication with specific nodes."""
        response = raft_pb2.PartitionResponse()
        
        with self.node.lock:
            self.node.blocked_peers = set(request.blockedNodeIds)
            response.success = True
            response.message = f"Blocked {len(self.node.blocked_peers)} peers"

        return response
    
    def _find_leader(self) -> int:
        """Best effort to find leader ID."""
        return max(self.node.peers) if self.node.peers else None