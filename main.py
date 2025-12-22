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


def get_node_ids_input(prompt: str, min_val: int, max_val: int) -> list:
    """Get validated node IDs input from user (supports multiple IDs separated by space/comma)"""
    try:
        raw_input = input(prompt).strip()
        if not raw_input:
            return []
        
        # Split by space or comma
        parts = raw_input.replace(',', ' ').split()
        node_ids = []
        
        for part in parts:
            try:
                value = int(part.strip())
                if min_val <= value <= max_val:
                    if value not in node_ids:  # Avoid duplicates
                        node_ids.append(value)
                else:
                    print(f"Skipping {value}: must be between {min_val} and {max_val}")
            except ValueError:
                print(f"Skipping invalid input: {part}")
        
        return node_ids
    except KeyboardInterrupt:
        print("\nCancelled.\n")
        return []

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
            self._handle_menu_choice(input("Enter choice (0-8): ").strip())
    
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
        print("  5. Create network partition")
        print("  6. Heal network partition")
        print("  7. Show partition status")
        print("  8. Exit")
        print()
    
    def _handle_menu_choice(self, choice: str):
        """Handle menu choice"""
        try:
            if choice == "0":
                clear_screen()
            elif choice == "1":
                self._get_node_ids_and_execute(
                    "Kill nodes", lambda n: self.manager.kill_node(n)
                )
            elif choice == "2":
                self._get_node_ids_and_execute(
                    "Revive nodes", lambda n: self.manager.revive_node(n)
                )
            elif choice == "3":
                self._get_single_node_and_execute(
                    "Query node", lambda n: self.manager.query_node_database(n)
                )
            elif choice == "4":
                from log_utils import print_snapshot
                print_snapshot(self.manager.nodes, "MANUAL SNAPSHOT REQUEST", 
                             "User requested cluster state")
            elif choice == "5":
                self._create_partition()
            elif choice == "6":
                self._heal_partition()
            elif choice == "7":
                self.manager.print_partition_status()
            elif choice == "8":
                print("\nExiting...\n")
                self.running = False
            else:
                print("Invalid choice. Please enter 0-8.")
        except KeyboardInterrupt:
            print("\n\nInterrupted. Exiting...\n")
            self.running = False
        except Exception as e:
            print(f"Error: {str(e)}")
    
    def _get_node_ids_and_execute(self, action: str, callback):
        """Get multiple node IDs input and execute callback for each"""
        summary = self.manager.get_cluster_summary()
        if not summary:
            print("No cluster running")
            return
        
        print(f"Cluster has nodes 0-{summary['total_nodes']-1}")
        print("Enter node ID(s) separated by space or comma (e.g., '1' or '1 2 3' or '1,2,3'):")
        node_ids = get_node_ids_input("Node ID(s): ", 0, summary['total_nodes'] - 1)
        
        if node_ids:
            for node_id in node_ids:
                callback(node_id)
            print(f"{action}: {node_ids}")
    
    def _get_single_node_and_execute(self, action: str, callback):
        """Get single node ID input and execute callback"""
        summary = self.manager.get_cluster_summary()
        if not summary:
            print("No cluster running")
            return
        
        print(f"Cluster has nodes 0-{summary['total_nodes']-1}")
        node_ids = get_node_ids_input("Enter node ID: ", 0, summary['total_nodes'] - 1)
        if node_ids:
            callback(node_ids[0])

    def _create_partition(self):
        """Create a network partition between two groups"""
        summary = self.manager.get_cluster_summary()
        if not summary:
            print("No cluster running")
            return
        
        all_node_ids = set(range(summary['total_nodes']))
        quorum = (summary['total_nodes'] // 2) + 1
        
        print("\n" + "="*60)
        print("CREATE NETWORK PARTITION")
        print("="*60)
        print(f"Cluster has {summary['total_nodes']} nodes (0-{summary['total_nodes']-1})")
        print(f"Quorum required: {quorum} nodes")
        print("\nEnter Group A - remaining nodes will be Group B")
        print("Example: '0,1,2,3' creates Group A={0,1,2,3}, Group B={4,5,6}")
        print()
        
        group_a = get_node_ids_input("Group A node IDs: ", 0, summary['total_nodes'] - 1)
        if not group_a:
            print("Cancelled.")
            return
        
        # Auto-calculate Group B as remaining nodes
        group_b = list(all_node_ids - set(group_a))
        group_b.sort()
        
        if not group_b:
            print("Error: Group B would be empty. Enter fewer nodes for Group A.")
            return
        
        self.manager.create_partition(group_a, group_b)
        
        # Show analysis
        print("Partition Analysis:")
        print(f"  Group A: {group_a} ({len(group_a)} nodes) {'-> CAN elect leader' if len(group_a) >= quorum else '-> NO quorum'}")
        print(f"  Group B: {group_b} ({len(group_b)} nodes) {'-> CAN elect leader' if len(group_b) >= quorum else '-> NO quorum'}")
        print()

    def _heal_partition(self):
        """Heal network partition for specific nodes"""
        summary = self.manager.get_cluster_summary()
        if not summary:
            print("No cluster running")
            return
        
        print(f"\nEnter node IDs to heal (0-{summary['total_nodes']-1}):")
        node_ids = get_node_ids_input("Node IDs: ", 0, summary['total_nodes'] - 1)
        if node_ids:
            self.manager.heal_partition(node_ids)
    
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





