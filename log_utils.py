"""
Utilities for Raft logging and monitoring.
"""

import logging
from typing import Dict
from datetime import datetime


# Configure logging with timestamp
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class Colors:
    """ANSI color codes for terminal output"""
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
