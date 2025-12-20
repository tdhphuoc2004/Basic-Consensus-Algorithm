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
    """Print current state of all nodes."""
    import time
    
    if not nodes:
        print_colored("[ERROR] No cluster running", Colors.FAIL)
        return
    
    print("\n" + "="*120)
    print(f"{title}")
    if reason:
        print(f"→ {reason}")
    print("="*120)
    
    current_time = time.time()
    
    for node in nodes:
        state = node.get_state()
        node_id = state["node_id"]
        state_str = state["state"].upper()
        term = state["term"]
        log_len = state["log_length"]
        commit_idx = state["commit_index"]
        data_size = state.get("state_machine_size", 0)
        
        if state_str == "LEADER":
            symbol = "👑"
            status = f"LEADER (term={term})"
        elif state_str == "CANDIDATE":
            symbol = "🔔"
            status = f"CANDIDATE (term={term})"
        elif state_str == "STOPPED":
            symbol = "💀"
            status = "STOPPED"
        else:
            symbol = "👤"
            status = f"FOLLOWER (term={term})"
        
        with node.lock:
            timeout_abs = node.election_timeout
            timeout_remain = (timeout_abs - current_time) * 1000
        
        timeout_str = f"expires in {timeout_remain:.0f}ms" if timeout_remain > 0 else "EXPIRED"
        if state_str == "LEADER":
            timeout_str = "LEADING (no timeout)"
        elif state_str == "STOPPED":
            timeout_str = "DEAD"
        
        print(f"  {symbol} Node {node_id}: {status:<18} | Log={log_len:2d} Commit={commit_idx:2d} Data={data_size:2d} | Timeout: {timeout_str}")
    
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
        # Skip stopped nodes
        node_state = node.get_state()
        if node_state["state"] == "STOPPED":
            continue
        
        with node.lock:
            timeout_abs = node.election_timeout
            timeout_remain = (timeout_abs - current_time) * 1000  # Convert to ms
        
        # Skip nodes with negative timeout (already expired/dead)
        if timeout_remain < 0:
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
