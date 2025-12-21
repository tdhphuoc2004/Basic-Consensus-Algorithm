"""Utilities for Raft logging and monitoring."""

import logging
from typing import Dict
from datetime import datetime

logging.basicConfig(
    level=logging.CRITICAL,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class Colors:
    """ANSI color codes for terminal output."""
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def print_colored(text: str, color: str) -> None:
    """Print colored text to terminal"""
    print(f"{color}{text}{Colors.ENDC}")


def print_header(text: str) -> None:
    """Print a formatted header"""
    print_colored(f"\n{'='*60}", Colors.BOLD)
    print_colored(f"  {text}", Colors.OKBLUE)
    print_colored(f"{'='*60}\n", Colors.BOLD)


def log_event(event: str, level: str = "INFO") -> None:
    """Log an event with timestamp"""
    if level == "INFO":
        logger.info(event)
    elif level == "WARNING":
        logger.warning(event)
    elif level == "ERROR":
        logger.error(event)


def format_node_state(node_state: Dict) -> str:
    """Format a node's state for display"""
    node_id = node_state["node_id"]
    state = node_state["state"].upper()
    term = node_state["term"]
    voted_for = node_state["voted_for"]
    is_leader = node_state["is_leader"]

    # Color code based on state
    if state == "LEADER":
        color = Colors.OKGREEN
        symbol = "👑"
    elif state == "CANDIDATE":
        color = Colors.WARNING
        symbol = "🔔"
    else:
        color = Colors.OKCYAN
        symbol = "👤"

    state_str = f"{color}{symbol} {state}{Colors.ENDC}"
    voted_str = f"voted_for={voted_for}" if voted_for is not None else "voted_for=None"

    return (
        f"Node {node_id}: {state_str} | "
        f"Term={term} | {voted_str} | Leader={is_leader}"
    )


def print_snapshot(nodes, title: str = "CLUSTER STATE", reason: str = ""):
    """Print current state of all nodes (optimized for minimal lock time)."""
    import time
    
    if not nodes:
        print_colored("[ERROR] No cluster running", Colors.FAIL)
        return
    
    # Collect all data first with minimal lock time
    current_time = time.time()
    node_data = []
    
    for node in nodes:
        try:
            # Use timeout to avoid blocking on stopped nodes
            acquired = node.lock.acquire(timeout=0.1)
            if acquired:
                try:
                    node_data.append({
                        "node_id": node.node_id,
                        "state": node.state.value.upper(),
                        "term": node.current_term,
                        "log_len": len(node.log),
                        "commit_idx": node.commit_index,
                        "data_size": node.state_machine.get_size() if hasattr(node, 'state_machine') else 0,
                        "timeout": node.election_timeout,
                    })
                finally:
                    node.lock.release()
            else:
                # Could not acquire lock in time, read without lock (best effort)
                node_data.append({
                    "node_id": getattr(node, 'node_id', '?'),
                    "state": getattr(node.state, 'value', 'BUSY').upper(),
                    "term": getattr(node, 'current_term', 0),
                    "log_len": len(getattr(node, 'log', [])),
                    "commit_idx": getattr(node, 'commit_index', 0),
                    "data_size": 0,
                    "timeout": getattr(node, 'election_timeout', 0),
                })
        except Exception:
            # Node may be in inconsistent state during kill/revive
            node_data.append({
                "node_id": getattr(node, 'node_id', '?'),
                "state": "UNKNOWN",
                "term": 0,
                "log_len": 0,
                "commit_idx": 0,
                "data_size": 0,
                "timeout": 0,
            })
    
    # Now print without holding any locks
    print("\n" + "="*120)
    print(f"{title}")
    if reason:
        print(f"→ {reason}")
    print("="*120)
    
    for data in node_data:
        state_str = data["state"]
        term = data["term"]
        
        if state_str == "LEADER":
            symbol = "👑"
            status = f"LEADER (term={term})"
            timeout_str = "LEADING (no timeout)"
        elif state_str == "CANDIDATE":
            symbol = "🔔"
            status = f"CANDIDATE (term={term})"
            timeout_remain = (data["timeout"] - current_time) * 1000
            timeout_str = f"expires in {timeout_remain:.0f}ms" if timeout_remain > 0 else "EXPIRED"
        elif state_str == "STOPPED":
            symbol = "💀"
            status = "STOPPED"
            timeout_str = "DEAD"
        elif state_str == "UNKNOWN":
            symbol = "❓"
            status = "UNKNOWN"
            timeout_str = "N/A"
        else:
            symbol = "👤"
            status = f"FOLLOWER (term={term})"
            timeout_remain = (data["timeout"] - current_time) * 1000
            timeout_str = f"expires in {timeout_remain:.0f}ms" if timeout_remain > 0 else "EXPIRED"
        
        print(f"  {symbol} Node {data['node_id']}: {status:<18} | Log={data['log_len']:2d} Commit={data['commit_idx']:2d} Data={data['data_size']:2d} | Timeout: {timeout_str}")
    
    print("="*120 + "\n")


def print_pre_election_timeouts(nodes):
    """Print election timeout analysis (ignoring STOPPED nodes and negative timeouts)"""
    if not nodes:
        return
    
    print("\n" + "="*100)
    print("PRE-ELECTION TIMEOUT ANALYSIS")
    print("="*100)
    print("These are the election timeouts set for ACTIVE nodes only.")
    print("The node with the LOWEST (earliest) timeout will win the next election.\n")
    
    import time
    current_time = time.time()
    timeout_list = []
    
    # Collect timeout info for all ACTIVE nodes (skip STOPPED and negative)
    for node in nodes:
        try:
            # Use timeout to avoid blocking
            acquired = node.lock.acquire(timeout=0.05)
            if not acquired:
                continue
            try:
                # Skip stopped nodes
                if node.state.value == "stopped":
                    continue
                timeout_abs = node.election_timeout
                timeout_remain = (timeout_abs - current_time) * 1000  # Convert to ms
            finally:
                node.lock.release()
            
            # Skip nodes with negative timeout (already expired/dead)
            if timeout_remain < 0:
                continue
        except Exception:
            continue
        
        timeout_list.append({
            "node_id": node.node_id,
            "timeout_ms": timeout_remain,
            "timeout_abs": timeout_abs,
        })
    
    # Sort by timeout (ascending)
    timeout_list.sort(key=lambda x: x["timeout_ms"])
    
    if not timeout_list:
        print("  (No active nodes with valid timeouts)")
        print("\n" + "="*100 + "\n")
        return
    
    # Print sorted list
    for i, item in enumerate(timeout_list):
        node_id = item["node_id"]
        timeout_ms = item["timeout_ms"]
        
        # Mark the first one
        if i == 0:
            marker = "⏱️  WILL TIMEOUT FIRST (Expected to win election)"
            color = Colors.OKGREEN
        else:
            marker = f"     (Timeout in {timeout_ms - timeout_list[0]['timeout_ms']:.0f}ms later)"
            color = Colors.OKCYAN
        
        print(f"  {color}Node {node_id}: {timeout_ms:7.0f}ms  {marker}{Colors.ENDC}")
    
    print("\n" + "="*100 + "\n")
