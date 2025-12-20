"""
Raft Consensus Algorithm - Interactive Control Menu
Control nodes manually, send commands, and monitor state machine.
"""

import os
from typing import Optional
from cluster_manager import ClusterManager


def clear_screen():
    """Clear the terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')


def get_node_input(prompt: str, min_val: int, max_val: int) -> Optional[int]:
    """Get validated integer input from user"""
    try:
        value = int(input(prompt).strip())
        if min_val <= value <= max_val:
            return value
        print(f"Please enter a number between {min_val} and {max_val}")
        return None
    except ValueError:
        print("Invalid input. Please enter a number.")
        return None
    except KeyboardInterrupt:
        print("\nCancelled.\n")
        return None

# ============================================================================
# INTERACTIVE MENU
# ============================================================================

class Menu:
    """Interactive menu system"""
    
    def __init__(self):
        self.manager = ClusterManager()
        self.running = True
    
    def run(self):
        """Main menu loop"""
        print("\n" + "="*80)
        print("RAFT CONSENSUS ALGORITHM - INTERACTIVE CONTROL MENU")
        print("="*80)
        print("Welcome to Raft Interactive Control")
        print("Initializing 7-node cluster...\n")
        
        self.manager.initialize_cluster()
        
        while self.running:
            self._print_menu_header()
            self._handle_menu_choice(input("Enter choice (0-5): ").strip())
    
    def _print_menu_header(self):
        """Print menu header with cluster status"""
        summary = self.manager.get_cluster_summary()
        print()
        
        if summary:
            print(f"[Cluster Status] Nodes={summary['total_nodes']} Leaders={summary['leaders']} "
                  f"Followers={summary['followers']} Stopped={summary['stopped']}")
            if summary['leader_id'] is not None:
                print(f"[Current Leader] Node {summary['leader_id']}")
            print()
        
        print("Main Menu:")
        print("  0. Clear screen")
        print("  1. Kill a node (simulate failure)")
        print("  2. Revive a node")
        print("  3. Query node database")
        print("  4. Print cluster snapshot")
        print("  5. Exit")
        print()
    
    def _handle_menu_choice(self, choice: str):
        """Handle menu choice"""
        try:
            if choice == "0":
                clear_screen()
            elif choice == "1":
                self._get_node_input_and_execute(
                    "Kill node", lambda n: self.manager.kill_node(n)
                )
            elif choice == "2":
                self._get_node_input_and_execute(
                    "Revive node", lambda n: self.manager.revive_node(n)
                )
            elif choice == "3":
                self._get_node_input_and_execute(
                    "Query node", lambda n: self.manager.query_node_database(n)
                )
            elif choice == "4":
                from log_utils import print_snapshot
                print_snapshot(self.manager.nodes, "MANUAL SNAPSHOT REQUEST", 
                             "User requested cluster state")
            elif choice == "5":
                print("\nExiting...\n")
                self.running = False
            else:
                print("Invalid choice. Please enter 0-5.")
        except KeyboardInterrupt:
            print("\n\nInterrupted. Exiting...\n")
            self.running = False
        except Exception as e:
            print(f"Error: {str(e)}")
    
    def _get_node_input_and_execute(self, action: str, callback):
        """Get node ID input and execute callback"""
        summary = self.manager.get_cluster_summary()
        if not summary:
            print("No cluster running")
            return
        
        print(f"Cluster has nodes 0-{summary['total_nodes']-1}")
        node_id = get_node_input("Enter node ID: ", 0, summary['total_nodes'] - 1)
        if node_id is not None:
            callback(node_id)
    
# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point"""
    try:
        menu = Menu()
        menu.run()
    finally:
        print("\nCleaning up...")


if __name__ == "__main__":
    main()





