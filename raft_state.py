"""Raft State Management - Core data structures and state for Raft consensus nodes."""

from dataclasses import dataclass
from enum import Enum
from typing import List, Set, Dict, Optional


class NodeState(Enum):
    """Enum for node states in Raft."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"
    STOPPED = "stopped"


@dataclass
class LogEntry:
    """Represents a log entry in Raft."""
    term: int
    command: str
    
    def __eq__(self, other):
        if not isinstance(other, LogEntry):
            return False
        return self.term == other.term and self.command == other.command


@dataclass
class NodeConfig:
    """Configuration for Raft nodes (all times in seconds)."""
    election_timeout_min: float = 1.5   # seconds
    election_timeout_max: float = 3.0   # seconds
    heartbeat_interval: float = 0.5     # seconds - must be << election timeout
    rpc_timeout: float = 0.4            # seconds             
