"""
Cluster Manager - Manages cluster creation and node lifecycle
Single Responsibility: Cluster management only.
"""

import time
import threading
import random
from typing import List, Optional
from raft_node import RaftNode, NodeConfig
from raft_state import NodeState


class Config:
    """Global configuration (all times in seconds)"""
    ELECTION_TIMEOUT_MIN = 1.5    # seconds
    ELECTION_TIMEOUT_MAX = 3.0    # seconds
    HEARTBEAT_INTERVAL = 0.5      # seconds - must be << election timeout
    RPC_TIMEOUT = 0.4             # seconds
    MONITOR_INTERVAL = 0.3


def wait_for_leader(nodes: List[RaftNode], timeout_ms: int = 5000, exclude_id: Optional[int] = None) -> bool:
    """Wait for a leader to be elected."""
    max_iterations = timeout_ms // 100
    for _ in range(max_iterations):
        for node in nodes:
            state = node.get_state()
            if state["is_leader"] and (exclude_id is None or state["node_id"] != exclude_id):
                return True
        time.sleep(0.1)
    return False


def create_node_config() -> NodeConfig:
    """Create NodeConfig from Config."""
    return NodeConfig(
        election_timeout_min=Config.ELECTION_TIMEOUT_MIN,
        election_timeout_max=Config.ELECTION_TIMEOUT_MAX,
        heartbeat_interval=Config.HEARTBEAT_INTERVAL,
        rpc_timeout=Config.RPC_TIMEOUT,
    )


class ClusterManager:
    """Manages cluster creation and node lifecycle"""
    
    def __init__(self):
        self.nodes: List[RaftNode] = []
        self.num_nodes = 7
        
    def initialize_cluster(self) -> bool:
        """Create a new cluster with 7 nodes."""
        if self.nodes:
            print("Cluster already exists.")
            return False
        
        print(f"Creating cluster with {self.num_nodes} nodes...")
        all_node_ids = list(range(self.num_nodes))
        config = create_node_config()
        
        for node_id in all_node_ids:
            self.nodes.append(RaftNode(node_id, all_node_ids, config))
        
        for node in self.nodes:
            threading.Thread(target=node.start, daemon=True).start()
            time.sleep(0.05)
        
        print(f"Cluster created with {self.num_nodes} nodes")
        
        print("Waiting for first election...")
        wait_for_leader(self.nodes)
        
        return True
    
    def destroy_cluster(self) -> bool:
        """Destroy the entire cluster."""
        if not self.nodes:
            print("No cluster running")
            return False
        
        for node in self.nodes:
            if hasattr(node, 'grpc_server') and node.grpc_server:
                try:
                    node.grpc_server.stop(grace=0)
                except Exception:
                    pass
        
        self.nodes.clear()
        self.num_nodes = 0
        print("Cluster destroyed")
        return True
    
    def kill_node(self, node_id: int) -> bool:
        """Stop a node (simulate failure)."""
        if not self._validate_node_id(node_id):
            return False
        
        node = self.nodes[node_id]
        was_leader = node.get_state()["is_leader"]
        
        if hasattr(node, 'grpc_server') and node.grpc_server:
            try:
                node.grpc_server.stop(grace=0)
            except Exception:
                pass
        
        with node.lock:
            node.state = NodeState.STOPPED
        
        for other_node in self.nodes:
            if other_node.node_id != node_id:
                with other_node.lock:
                    if node_id in other_node.peers:
                        other_node.peers.remove(node_id)
                    other_node.next_index.pop(node_id, None)
                    other_node.match_index.pop(node_id, None)
        
        print(f"Node {node_id} killed")
        
        if was_leader:
            print("Waiting for new leader election...")
            wait_for_leader(self.nodes, exclude_id=node_id)
        
        return True
    
    def revive_node(self, node_id: int) -> bool:
        """Revive a stopped node."""
        if not self._validate_node_id(node_id):
            return False
        
        node = self.nodes[node_id]
        state = node.get_state()
        
        if state["state"] != "stopped":
            print(f"Node {node_id} is not stopped (state: {state['state']})")
            return False
        
        leader_id = None
        for n in self.nodes:
            if n.get_state()["is_leader"]:
                leader_id = n.node_id
                break
        
        for other_node in self.nodes:
            if other_node.node_id != node_id:
                with other_node.lock:
                    if node_id not in other_node.peers:
                        other_node.peers.append(node_id)
                    other_node.next_index[node_id] = 0
                    other_node.match_index[node_id] = -1
        
        # Restart the gRPC server on the same port
        node.restart_server(port=5000 + node_id)
        
        with node.lock:
            node.state = NodeState.FOLLOWER
            node.voted_for = None
            node.votes_received = set()
            # Restore peers list to include all other alive nodes
            node.peers = [n.node_id for n in self.nodes 
                         if n.node_id != node_id and n.get_state()["state"] != "stopped"]
            node._reset_election_timeout()
        
        print(f"Node {node_id} revived")
        
        if leader_id is not None:
            self._sync_node_with_leader(node, leader_id)
        
        return True
    
    def _sync_node_with_leader(self, node: RaftNode, leader_id: int):
        """Sync a revived node with the current leader."""
        leader_node = self.nodes[leader_id]
        
        from raft_replication import ReplicationManager
        repl_mgr = ReplicationManager(leader_node)
        
        for _ in range(5):
            thread = threading.Thread(
                target=repl_mgr.send_append_entries, args=(node.node_id, -1)
            )
            thread.daemon = True
            thread.start()
            time.sleep(0.2)
        
        sync_timeout = time.time() + 2.0
        
        while time.time() < sync_timeout:
            with leader_node.lock:
                leader_log_len = len(leader_node.log)
            with node.lock:
                node_log_len = len(node.log)
            
            if node_log_len >= leader_log_len:
                break
            
            time.sleep(0.1)
        
        with leader_node.lock:
            leader_data = leader_node.state_machine.get_data()
        
        with node.lock:
            if leader_data:
                node.state_machine.data.clear()
                node.state_machine.data.update(leader_data)
                node.storage.save_state_machine(leader_data)
    
    def query_node_database(self, node_id: int) -> bool:
        """Query the state machine of a specific node."""
        if not self._validate_node_id(node_id):
            return False
        
        node = self.nodes[node_id]
        state = node.get_state()
        data = node.state_machine.get_data()
        
        print("\n" + "="*60)
        print(f"NODE {node_id} DATABASE STATE")
        print("="*60)
        print(f"State: {state['state'].upper()}")
        print(f"Term: {state['term']}")
        print(f"Log Length: {state['log_length']}")
        print(f"Commit Index: {state['commit_index']}")
        print(f"Is Leader: {state['is_leader']}")
        print(f"\nStored Data:")
        
        if data:
            for key, value in sorted(data.items()):
                print(f"  {key}: {value}")
        else:
            print("  (empty)")
        
        print("="*60 + "\n")
        return True
    
    def get_cluster_summary(self) -> Optional[dict]:
        """Get summary info about cluster."""
        if not self.nodes:
            return None
        
        leader_id = None
        num_leaders = num_followers = num_stopped = 0
        
        for node in self.nodes:
            state = node.get_state()
            if state["is_leader"]:
                leader_id = node.node_id
                num_leaders += 1
            elif state["state"] == "stopped":
                num_stopped += 1
            else:
                num_followers += 1
        
        return {
            "total_nodes": len(self.nodes),
            "leaders": num_leaders,
            "followers": num_followers,
            "stopped": num_stopped,
            "leader_id": leader_id,
        }
    
    def _validate_node_id(self, node_id: int) -> bool:
        """Validate node ID exists."""
        if not self.nodes or node_id >= len(self.nodes):
            print(f"Invalid node ID: {node_id}")
            return False
        return True

    # ========================================================================
    # NETWORK PARTITION SIMULATION
    # ========================================================================

    def create_partition(self, group_a: List[int], group_b: List[int]) -> bool:
        """
        Create a network partition between two groups of nodes.
        Nodes in group_a cannot communicate with nodes in group_b and vice versa.
        """
        if not self.nodes:
            print("No cluster running")
            return False

        # Validate all node IDs
        all_nodes = group_a + group_b
        for node_id in all_nodes:
            if not self._validate_node_id(node_id):
                return False

        # Block communication between groups
        for node_id in group_a:
            node = self.nodes[node_id]
            with node.lock:
                for blocked_id in group_b:
                    node.blocked_peers.add(blocked_id)

        for node_id in group_b:
            node = self.nodes[node_id]
            with node.lock:
                for blocked_id in group_a:
                    node.blocked_peers.add(blocked_id)

        print(f"\n{'='*60}")
        print("NETWORK PARTITION CREATED")
        print(f"{'='*60}")
        print(f"Group A (nodes {group_a}): Cannot communicate with Group B")
        print(f"Group B (nodes {group_b}): Cannot communicate with Group A")
        print(f"{'='*60}\n")
        return True

    def heal_partition(self, node_ids: Optional[List[int]] = None) -> bool:
        """
        Heal network partition. If node_ids is None, heal all partitions.
        Otherwise, clear blocked_peers only for specified nodes.
        """
        if not self.nodes:
            print("No cluster running")
            return False

        if node_ids is None:
            # Heal all partitions
            for node in self.nodes:
                with node.lock:
                    node.blocked_peers.clear()
            print("All network partitions healed")
        else:
            # Heal specific nodes
            for node_id in node_ids:
                if self._validate_node_id(node_id):
                    node = self.nodes[node_id]
                    with node.lock:
                        node.blocked_peers.clear()
            # Also remove these nodes from other nodes' blocked_peers
            for node in self.nodes:
                with node.lock:
                    for node_id in node_ids:
                        node.blocked_peers.discard(node_id)
            print(f"Healed partitions for nodes: {node_ids}")
        return True

    def get_partition_status(self) -> dict:
        """Get current partition status for all nodes."""
        status = {}
        for node in self.nodes:
            with node.lock:
                status[node.node_id] = {
                    "blocked_peers": list(node.blocked_peers),
                    "state": node.state.name,
                }
        return status

    def print_partition_status(self):
        """Print current partition status."""
        status = self.get_partition_status()
        print(f"\n{'='*60}")
        print("NETWORK PARTITION STATUS")
        print(f"{'='*60}")
        for node_id, info in status.items():
            blocked = info["blocked_peers"]
            state = info["state"]
            if blocked:
                print(f"Node {node_id} ({state}): Blocking {blocked}")
            else:
                print(f"Node {node_id} ({state}): No blocks")
        print(f"{'='*60}\n")
