"""
Raft State Management
Defines core data structures and state for Raft consensus nodes.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Set, Dict, Optional


class NodeState(Enum):
    """Enum for node states in Raft"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"
    STOPPED = "stopped"


@dataclass
class LogEntry:
    """Represents a log entry in Raft"""
    term: int
    command: str
    
    def __eq__(self, other):
        if not isinstance(other, LogEntry):
            return False
        return self.term == other.term and self.command == other.command


@dataclass
class NodeConfig:
    """Configuration for Raft nodes"""
    election_timeout_min: float = 1.5
    election_timeout_max: float = 3.0
    heartbeat_interval: float = 0.5
    rpc_timeout: float = 1.0
