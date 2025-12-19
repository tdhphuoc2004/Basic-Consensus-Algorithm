"""
Raft Consensus Algorithm - Configurable Test Scenarios
Focus: Normal leader election and recovery from leader death
"""

import sys
import time
import threading
from typing import List, Optional
from raft_node import RaftNode, NodeConfig
from log_utils import print_header, Colors, print_colored, log_event


# ============================================================================
# ADJUSTABLE PARAMETERS - MODIFY THESE TO TEST DIFFERENT SCENARIOS
# ============================================================================

class Config:
    """Easy-to-adjust test configuration"""
    
    # CLUSTER SETUP
    NUM_NODES = 7                 # Cluster size (3, 5, 7 recommended)
    
    # ELECTION TIMING IN MILLISECONDS 
    ELECTION_TIMEOUT_MIN = 800             # Min ms before follower becomes candidate
    ELECTION_TIMEOUT_MAX = 1500            # Max ms before follower becomes candidate
    HEARTBEAT_INTERVAL = 200               # Leader sends heartbeat every N ms
    RPC_TIMEOUT = 500                      # RPC timeout in ms
    
    # TEST SCENARIO SELECTION
    SCENARIO = "leader_death"           # Options: "normal_election" or "leader_death"
    
    # For "normal_election" scenario
    NORMAL_ELECTION_DURATION = 10          # How long to observe normal election (seconds)
    
    # For "leader_death" scenario
    LEADER_DEATH_DELAY = 8                 # Wait N seconds before killing leader

    # MONITOR OUTPUT
    MONITOR_INTERVAL = 0.3                 # Print cluster state every N seconds



# ============================================================================
# CLUSTER MONITORING
# ============================================================================

class Monitor:
    """Simple cluster monitor"""
    
    def __init__(self, nodes: List[RaftNode], interval: float = 0.5):
        self.nodes = nodes
        self.interval = interval
        self.running = False
        self.last_states = {}
        self.start_time = time.time()
    
    def start(self):
        self.running = True
        self.start_time = time.time()
        thread = threading.Thread(target=self._loop, daemon=True)
        thread.start()
    
    def stop(self):
        self.running = False
    
    def _loop(self):
        while self.running:
            self._print_state()
            time.sleep(self.interval)
    
    def _print_state(self):
        """Print state changes"""
        for node in self.nodes:
            state = node.get_state()
            node_id = state["node_id"]
            current = {"state": state["state"], "term": state["term"]}
            
            if node_id not in self.last_states or self.last_states[node_id] != current:
                self._print_node(state)
                self.last_states[node_id] = current
    
    def _print_node(self, state: dict):
        """Print node details"""
        node_id = state["node_id"]
        state_str = state["state"].upper()
        term = state["term"]
        is_leader = state["is_leader"]
        
        # Color and symbol
        if state_str == "LEADER":
            symbol, color = "👑", Colors.OKGREEN
        elif state_str == "CANDIDATE":
            symbol, color = "🔔", Colors.WARNING
        elif state_str == "STOPPED":
            symbol, color = "💀", Colors.FAIL
        else:
            symbol, color = "👤", Colors.OKCYAN
        
        elapsed = time.time() - self.start_time
        print(f"[{elapsed:6.1f}s] {color}{symbol} Node {node_id}: {state_str}{Colors.ENDC} | Term={term} | Leader={is_leader}")
    
    def print_snapshot(self, title: str, reason: str = ""):
        """Print clean snapshot of cluster state with timeout info"""
        states = [node.get_state() for node in self.nodes]
        elapsed = time.time() - self.start_time
        
        print("\n" + "=" * 100)
        print(f"[{elapsed:6.1f}s] {title}")
        if reason:
            print(f"→ {reason}")
        print("=" * 100)
        
        current_time = time.time()
        
        for state in states:
            node_id = state["node_id"]
            state_str = state["state"].upper()
            term = state["term"]
            is_leader = state["is_leader"]
            
            # Get node's specific timeout
            node = self.nodes[node_id]
            with node.lock:
                timeout_abs = node.election_timeout
                timeout_remain = (timeout_abs - current_time) * 1000  # Convert to ms
            
            if state_str == "LEADER":
                symbol = "👑"
                timeout_str = "LEADING (no timeout)"
            elif state_str == "CANDIDATE":
                symbol = "🔔"
                timeout_str = f"VOTING (expires in {timeout_remain:7.0f}ms)" if timeout_remain > 0 else "VOTING (EXPIRED)"
            elif state_str == "STOPPED":
                symbol = "💀"
                timeout_str = "DEAD"
            else:
                symbol = "👤"
                timeout_str = f"WAIT (expires in {timeout_remain:7.0f}ms)" if timeout_remain > 0 else "WAIT (EXPIRED)"
            
            role = "LEADER" if is_leader else "FOLLOWER"
            print(f"  Node {node_id}: {symbol} {state_str:<10} Term={term:<3} Role={role:<8} Timeout={timeout_str}")
        
        print("=" * 100 + "\n")


# ============================================================================
# TEST SCENARIO EXECUTORS
# ============================================================================

class TestScenarios:
    """Test scenario implementations"""
    
    @staticmethod
    def normal_election(nodes: List[RaftNode], monitor: Monitor):
        """Scenario 1: Just observe normal leader election"""
        print_colored("\n>>> SCENARIO: Normal Leader Election", Colors.BOLD)
        print("Watching cluster elect a leader naturally...")
        
        duration = Config.NORMAL_ELECTION_DURATION
        log_event(f"Running for {duration} seconds", "INFO")
        time.sleep(duration)
        
        # Find leader
        leader = None
        for node in nodes:
            if node.get_state()["is_leader"]:
                leader = node.node_id
                break
        
        if leader is not None:
            monitor.print_snapshot("✅ ELECTION COMPLETE", f"Leader elected: Node {leader}")
            log_event(f"✓ Normal election succeeded - Node {leader} is leader", "OKGREEN")
        else:
            log_event("✗ No leader elected", "FAIL")
    
    @staticmethod
    def leader_death(nodes: List[RaftNode], monitor: Monitor):
        """Scenario 2: Kill leader and observe recovery"""
        print_colored("\n>>> SCENARIO: Leader Death & Recovery", Colors.BOLD)
        
        # Wait for initial leader election
        delay = Config.LEADER_DEATH_DELAY
        print(f"Waiting {delay}s for leader election...")
        time.sleep(delay)
        
        # Find current leader
        leader_id = None
        for node in nodes:
            if node.get_state()["is_leader"]:
                leader_id = node.node_id
                break
        
        if leader_id is None:
            log_event("ERROR: No leader found to kill", "FAIL")
            return
        
        # Kill the leader
        log_event(f"Killing leader: Node {leader_id}", "WARNING")
        node = nodes[leader_id]
        with node.lock:
            from raft_state import NodeState
            node.state = NodeState.STOPPED
        
        monitor.print_snapshot("⚠️  LEADER DEATH", f"Node {leader_id} crashed - new election will start")
        time.sleep(1)
        
        # Wait for re-election (timeout will reset to new random value between 800-1500ms)
        election_time = Config.ELECTION_TIMEOUT_MAX / 1000 + 1  # Convert ms to seconds
        print(f"Waiting {election_time:.1f}s for re-election...")
        time.sleep(election_time)
        
        # Find new leader
        new_leader = None
        for node in nodes:
            if node.get_state()["is_leader"]:
                new_leader = node.node_id
                break
        
        if new_leader is not None:
            monitor.print_snapshot("✅ NEW LEADER ELECTED", f"Node {new_leader} elected as new leader")
            log_event(f"✓ Re-election succeeded - Node {new_leader} is new leader", "OKGREEN")
        else:
            log_event("✗ Failed to elect new leader", "FAIL")
            return
     

# ============================================================================
# MAIN
# ============================================================================

def main():
    print_colored("\n" + "=" * 100, Colors.HEADER)
    print_colored("RAFT CONSENSUS ALGORITHM - ELECTION & FAULT RECOVERY TEST", Colors.BOLD)
    print_colored("=" * 100 + "\n", Colors.HEADER)
    
    # Display configuration
    print_colored("CONFIGURATION:", Colors.BOLD)
    print(f"""
  Cluster Size:             {Config.NUM_NODES} nodes
  Quorum Required:          {(Config.NUM_NODES + 1) // 2} nodes
  Election Timeout:         {Config.ELECTION_TIMEOUT_MIN}ms - {Config.ELECTION_TIMEOUT_MAX}ms (Raft paper: 150-300ms)
  Heartbeat Interval:       {Config.HEARTBEAT_INTERVAL}ms
  RPC Timeout:              {Config.RPC_TIMEOUT}ms
  Test Scenario:            {Config.SCENARIO}
""")
    
    # Create Raft configuration (convert ms to seconds for internal use)
    config = NodeConfig(
        election_timeout_min=Config.ELECTION_TIMEOUT_MIN / 1000,
        election_timeout_max=Config.ELECTION_TIMEOUT_MAX / 1000,
        heartbeat_interval=Config.HEARTBEAT_INTERVAL / 1000,
        rpc_timeout=Config.RPC_TIMEOUT / 1000,
    )
    
    # Create cluster
    print_colored("Creating cluster...", Colors.OKCYAN)
    all_node_ids = list(range(Config.NUM_NODES))
    nodes = []
    for node_id in all_node_ids:
        node = RaftNode(node_id, all_node_ids, config)
        nodes.append(node)
    print_colored("[✓] Cluster created\n", Colors.OKGREEN)
    
    # Start cluster
    print_colored("Starting nodes...", Colors.OKCYAN)
    for node in nodes:
        threading.Thread(target=node.start, daemon=True).start()
        time.sleep(0.05)  # Reduced delay - start nodes quickly
    print_colored("[✓] All nodes started\n", Colors.OKGREEN)
    
    # Start monitor
    monitor = Monitor(nodes, interval=Config.MONITOR_INTERVAL)
    monitor.start()
    
    # Wait for nodes to initialize BEFORE any election timeouts trigger
    # Need to wait less than MIN election timeout (800ms)
    time.sleep(0.3)
    
    # Print initial state snapshot (all nodes should still be FOLLOWER with Term=0)
    monitor.print_snapshot("🚀 CLUSTER INITIALIZED", "All nodes started in FOLLOWER state, waiting for election...")
    
    # Wait longer to let nodes settle and timeouts initialize properly
    # This ensures snapshot shows accurate remaining timeout values
    time.sleep(0.5)
    
    # Run selected scenario
    try:
        if Config.SCENARIO == "normal_election":
            TestScenarios.normal_election(nodes, monitor)
        elif Config.SCENARIO == "leader_death":
            TestScenarios.leader_death(nodes, monitor)
        else:
            log_event(f"Unknown scenario: {Config.SCENARIO}", "FAIL")
        
        # Final state
        print_colored("\nTest complete!\n", Colors.OKGREEN)
        monitor.print_snapshot("🏁 FINAL CLUSTER STATE", "Test execution complete")
        
    except KeyboardInterrupt:
        print_colored("\n[Interrupted by user]", Colors.WARNING)
    
    monitor.stop()
    log_event("Test finished", "INFO")


if __name__ == "__main__":
    main()




