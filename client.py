"""
Raft Cluster Client
A command-line interface to communicate with the Raft cluster.
Supports sending commands (SET/GET), checking cluster status, and simulating partitions.
"""

import grpc
import sys
import time
from typing import Optional, Tuple

import raft_pb2
import raft_pb2_grpc


class RaftClusterClient:
    """Client to interact with a Raft cluster"""
    
    def __init__(self, base_port: int = 5000, num_nodes: int = 7):
        self.base_port = base_port
        self.num_nodes = num_nodes
        self.current_leader: Optional[int] = None
        self.timeout = 5  # seconds
    
    def _get_node_address(self, node_id: int) -> str:
        """Get gRPC address for a node"""
        return f"localhost:{self.base_port + node_id}"
    
    def _create_stub(self, node_id: int) -> Tuple[raft_pb2_grpc.RaftServiceStub, grpc.Channel]:
        """Create a gRPC stub for a node"""
        address = self._get_node_address(node_id)
        channel = grpc.insecure_channel(address)
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        return stub, channel
    
    def find_leader(self) -> Optional[int]:
        """Find the current leader by querying all nodes"""
        print("\n[*] Searching for cluster leader...")
        
        # First pass: find node that claims to be leader (success=True)
        for node_id in range(self.num_nodes):
            try:
                stub, channel = self._create_stub(node_id)
                request = raft_pb2.ClientRequestMessage(command="PING")
                response = stub.ClientRequest(request, timeout=2)
                channel.close()
                
                if response.success:
                    print(f"[✓] Found leader: Node {node_id}")
                    self.current_leader = node_id
                    return node_id
            except:
                pass
        
        print("[✗] No leader found in cluster")
        return None
    
    def send_command(self, command: str, retry: bool = True) -> bool:
        """
        Send a command to the cluster leader.
        Commands: SET <key> <value>, GET <key>, DELETE <key>
        """
        if self.current_leader is None:
            if not self.find_leader():
                print("[ERROR] Cannot send command: No leader available")
                return False
        
        print(f"\n[*] Sending command: '{command}' to Node {self.current_leader}")
        
        try:
            stub, channel = self._create_stub(self.current_leader)
            request = raft_pb2.ClientRequestMessage(command=command)
            response = stub.ClientRequest(request, timeout=self.timeout)
            channel.close()
            
            if response.success:
                print(f"[✓] Command committed successfully!")
                print(f"    Message: {response.message}")
                if response.value:
                    print(f"    Value: {response.value}")
                return True
            else:
                print(f"[✗] Command failed: {response.message}")
                if response.leaderId and response.leaderId.isdigit():
                    new_leader = int(response.leaderId)
                    if new_leader != self.current_leader and retry:
                        print(f"[*] Redirecting to new leader: Node {new_leader}")
                        self.current_leader = new_leader
                        return self.send_command(command, retry=False)
                return False
                
        except grpc.RpcError as e:
            print(f"[ERROR] RPC failed: {e.code().name} - {e.details()}")
            if retry:
                print("[*] Attempting to find new leader...")
                self.current_leader = None
                if self.find_leader():
                    return self.send_command(command, retry=False)
            return False
        except Exception as e:
            print(f"[ERROR] Unexpected error: {str(e)}")
            return False
    
    def check_cluster_status(self) -> dict:
        """Check the status of all nodes in the cluster"""
        print("\n" + "=" * 60)
        print("CLUSTER STATUS")
        print("=" * 60)
        
        status = {
            "online": [],
            "offline": [],
            "leader": None
        }
        
        for node_id in range(self.num_nodes):
            try:
                stub, channel = self._create_stub(node_id)
                request = raft_pb2.ClientRequestMessage(command="STATUS")
                response = stub.ClientRequest(request, timeout=2)
                channel.close()
                
                if response.success:
                    status["online"].append(node_id)
                    status["leader"] = node_id
                    print(f"  Node {node_id}: LEADER ★")
                else:
                    status["online"].append(node_id)
                    leader_info = f"(leader: {response.leaderId})" if response.leaderId else ""
                    print(f"  Node {node_id}: FOLLOWER")
                    
            except grpc.RpcError:
                status["offline"].append(node_id)
                print(f"  Node {node_id}: OFFLINE ✗")
            except Exception as e:
                status["offline"].append(node_id)
                print(f"  Node {node_id}: ERROR ({str(e)})")
        
        print("-" * 60)
        print(f"Online: {len(status['online'])} | Offline: {len(status['offline'])}")
        print("=" * 60)
        
        return status


def print_help():
    """Print help message"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║               RAFT CLUSTER CLIENT - COMMANDS                  ║
╠══════════════════════════════════════════════════════════════╣
║  Data Commands:                                              ║
║    set <key> <value>  - Store a key-value pair              ║
║    get <key>          - Retrieve a value by key             ║
║                                                              ║
║  Cluster Commands:                                           ║
║    status             - Show cluster status                  ║
║                                                              ║
║  Other:                                                      ║
║    help               - Show this help message               ║
║    exit/quit          - Exit the client                      ║
╚══════════════════════════════════════════════════════════════╝
""")


def interactive_mode(client: RaftClusterClient):
    """Run the client in interactive mode"""
    print("\n" + "=" * 60)
    print("  RAFT CLUSTER CLIENT - Interactive Mode")
    print("  Type 'help' for available commands")
    print("=" * 60)
    
    # Find leader on startup
    client.find_leader()
    
    while True:
        try:
            user_input = input("\nraft> ").strip()
            
            if not user_input:
                continue
            
            parts = user_input.split()
            cmd = parts[0].lower()
            
            if cmd in ["exit", "quit", "q"]:
                print("Goodbye!")
                break
            
            elif cmd == "help":
                print_help()
            
            elif cmd == "status":
                client.check_cluster_status()
            
            elif cmd == "set":
                if len(parts) < 3:
                    print("[ERROR] Usage: set <key> <value>")
                else:
                    key = parts[1]
                    value = " ".join(parts[2:])
                    client.send_command(f"SET {key} {value}")
            
            elif cmd == "get":
                if len(parts) < 2:
                    print("[ERROR] Usage: get <key>")
                else:
                    client.send_command(f"GET {parts[1]}")
            
            else:
                print(f"[ERROR] Unknown command: '{cmd}'. Type 'help' for available commands.")
                
        except KeyboardInterrupt:
            print("\n\nInterrupted. Type 'exit' to quit.")
        except EOFError:
            print("\nGoodbye!")
            break


def main():
    """Main entry point"""
    # Default configuration
    base_port = 5000
    num_nodes = 7
    
    # Parse command line arguments
    args = sys.argv[1:]
    
    if "--help" in args or "-h" in args:
        print("Usage: python client.py [options]")
        print("Options:")
        print("  --port <port>    Base port number")
        print("  --nodes <n>      Number of nodes in cluster (default: 7)")
        print("  --command <cmd>  Execute a single command and exit")
        print("  --help, -h       Show this help message")
        return
    
    # Parse port
    if "--port" in args:
        idx = args.index("--port")
        if idx + 1 < len(args):
            base_port = int(args[idx + 1])
    
    # Parse nodes
    if "--nodes" in args:
        idx = args.index("--nodes")
        if idx + 1 < len(args):
            num_nodes = int(args[idx + 1])
    
    client = RaftClusterClient(base_port=base_port, num_nodes=num_nodes)
    
    # Single command mode
    if "--command" in args:
        idx = args.index("--command")
        if idx + 1 < len(args):
            command = " ".join(args[idx + 1:])
            client.find_leader()
            client.send_command(command)
            return
    
    # Interactive mode
    interactive_mode(client)


if __name__ == "__main__":
    main()