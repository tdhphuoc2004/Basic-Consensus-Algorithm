"""
Raft Cluster Client
Provides a simple client interface for sending commands to the Raft cluster.
"""

import grpc
import sys
from typing import Optional, Tuple

import raft_pb2
import raft_pb2_grpc


class RaftClusterClient:
    """Client for interacting with Raft cluster"""
    
    def __init__(self, node_addresses: dict):
        """
        Initialize client with node addresses.
        
        Args:
            node_addresses: Dict mapping node_id to (host, port)
                           Example: {0: ("localhost", 5000), 1: ("localhost", 5001)}
        """
        self.node_addresses = node_addresses
        self.stubs = {}
        self.channels = {}
        self._connect_to_nodes()
    
    def _connect_to_nodes(self):
        """Create gRPC stubs for all nodes"""
        for node_id, (host, port) in self.node_addresses.items():
            try:
                channel = grpc.insecure_channel(f"{host}:{port}")
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                self.stubs[node_id] = stub
                self.channels[node_id] = channel
            except Exception as e:
                print(f"[WARNING] Could not connect to Node {node_id} at {host}:{port}: {e}")
    
    def send_command(self, command: str, target_node: Optional[int] = None) -> Tuple[bool, str]:
        """
        Send a command to the Raft cluster.
        
        Args:
            command: Command string to send (e.g., "SET key value")
            target_node: Optional specific node to send to. If None, try to find leader.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        # If target_node not specified, try all nodes to find leader
        nodes_to_try = [target_node] if target_node is not None else list(self.stubs.keys())
        
        for node_id in nodes_to_try:
            if node_id not in self.stubs:
                print(f"[ERROR] Node {node_id} not connected")
                continue
            
            try:
                request = raft_pb2.ClientRequestMessage(command=command)
                response = self.stubs[node_id].ClientRequest(request, timeout=2.0)
                
                if response.success:
                    return True, f"Command committed: {response.message}"
                else:
                    # Not leader, try next node or specified leader
                    if response.leaderId:
                        try:
                            leader_id = int(response.leaderId)
                            if leader_id in self.stubs:
                                print(f"[INFO] Node {node_id} is not leader, trying node {leader_id}")
                                continue
                        except ValueError:
                            pass
                    
                    print(f"[INFO] Node {node_id}: {response.message}")
                    
            except grpc.RpcError as e:
                print(f"[WARNING] RPC error with Node {node_id}: {e}")
            except Exception as e:
                print(f"[WARNING] Error communicating with Node {node_id}: {e}")
        
        return False, "Failed to commit command to any node"
    
    def simulate_partition(self, node_id: int, blocked_nodes: list) -> bool:
        """
        Simulate network partition by blocking communication from a node.
        
        Args:
            node_id: Node that should block communication
            blocked_nodes: List of node IDs to block
        
        Returns:
            True if successful, False otherwise
        """
        if node_id not in self.stubs:
            print(f"[ERROR] Node {node_id} not connected")
            return False
        
        try:
            request = raft_pb2.PartitionRequest(blockedNodeIds=blocked_nodes)
            response = self.stubs[node_id].SimulatePartition(request, timeout=2.0)
            
            if response.success:
                print(f"[INFO] {response.message}")
                return True
            else:
                print(f"[ERROR] {response.message}")
                return False
        except Exception as e:
            print(f"[ERROR] Failed to simulate partition: {e}")
            return False
    
    def heal_partition(self, node_id: int) -> bool:
        """
        Heal network partition by unblocking all peers.
        
        Args:
            node_id: Node to unblock peers for
        
        Returns:
            True if successful, False otherwise
        """
        return self.simulate_partition(node_id, [])
    
    def get_cluster_status(self) -> None:
        """Print status of all nodes in cluster"""
        print("\n" + "="*80)
        print("CLUSTER STATUS")
        print("="*80)
        
        for node_id, stub in self.stubs.items():
            try:
                # Try to get status via a heartbeat or other mechanism
                # For now, just show connection status
                print(f"Node {node_id}: Connected")
            except Exception as e:
                print(f"Node {node_id}: Error - {e}")
        
        print("="*80 + "\n")
    
    def close(self):
        """Close all connections"""
        for channel in self.channels.values():
            channel.close()


# ==============================================================================
# Example Usage
# ==============================================================================

if __name__ == "__main__":
    # Create client for 5-node cluster running on localhost
    node_addresses = {
        0: ("localhost", 5000),
        1: ("localhost", 5001),
        2: ("localhost", 5002),
        3: ("localhost", 5003),
        4: ("localhost", 5004),
    }
    
    client = RaftClusterClient(node_addresses)
    
    print("""
    ╔════════════════════════════════════════════════════════════════╗
    ║         RAFT CLUSTER CLIENT - USAGE EXAMPLES                   ║
    ╚════════════════════════════════════════════════════════════════╝
    """)
    
    # Example 1: Send command to cluster (finds leader automatically)
    print("\n1. Sending command to cluster (finds leader automatically):")
    success, msg = client.send_command("SET mykey myvalue")
    print(f"   Result: {msg}\n")
    
    # Example 2: Send another command
    print("2. Sending another command:")
    success, msg = client.send_command("GET mykey")
    print(f"   Result: {msg}\n")
    
    # Example 3: Simulate partition
    print("3. Simulating network partition (Node 4 blocks 0,1,2):")
    client.simulate_partition(4, [0, 1, 2])
    print()
    
    # Example 4: Try command during partition (will fail on minority)
    print("4. Trying to send command during partition:")
    success, msg = client.send_command("SET key2 value2")
    print(f"   Result: {msg}\n")
    
    # Example 5: Heal partition
    print("5. Healing partition:")
    client.heal_partition(4)
    print()
    
    # Example 6: Try command again after healing
    print("6. Trying to send command after healing partition:")
    success, msg = client.send_command("SET key3 value3")
    print(f"   Result: {msg}\n")
    
    client.close()
    print("Client closed.")
