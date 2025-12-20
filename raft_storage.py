"""
Raft Persistent Storage Manager
Handles persistence of term, voted_for, log entries, and state machine.
Single Responsibility: Storage operations only.
"""

import json
import os
import threading
from typing import Dict, List, Optional, Any
from raft_state import LogEntry


class RaftStorage:
    """Manages persistent storage for Raft nodes"""

    def __init__(self, node_id: int, storage_dir: str = "."):
        """Initialize storage manager."""
        self.node_id = node_id
        self.storage_dir = storage_dir
        self.lock = threading.RLock()
        self.persistent_state_file = os.path.join(storage_dir, f"node_{node_id}_persistent.json")
        self.log_file = os.path.join(storage_dir, f"node_{node_id}_log.json")
        self.state_file = os.path.join(storage_dir, f"node_{node_id}_state.json")
        os.makedirs(storage_dir, exist_ok=True)

    def load_persistent_state(self) -> tuple[int, Optional[int], List[LogEntry]]:
        """Load persistent state from disk."""
        with self.lock:
            current_term = 0
            voted_for = None
            log = []

            if os.path.exists(self.persistent_state_file):
                try:
                    with open(self.persistent_state_file, "r") as f:
                        data = json.load(f)

                    current_term = data.get("current_term", 0)
                    voted_for = data.get("voted_for", None)

                    for entry_data in data.get("log", []):
                        entry = LogEntry(
                            term=entry_data["term"], command=entry_data["command"]
                        )
                        log.append(entry)

                except Exception:
                    current_term = 0
                    voted_for = None
                    log = []

            return current_term, voted_for, log

    def save_persistent_state(
        self, current_term: int, voted_for: Optional[int], log: List[LogEntry]
    ):
        """Save persistent state to disk."""
        with self.lock:
            try:
                log_data = [{"term": entry.term, "command": entry.command} for entry in log]
                state = {"current_term": current_term, "voted_for": voted_for, "log": log_data}
                temp_file = self.persistent_state_file + ".tmp"
                with open(temp_file, "w") as f:
                    json.dump(state, f, indent=2)
                os.replace(temp_file, self.persistent_state_file)
            except Exception:
                pass

    def save_current_term(self, current_term: int, voted_for: Optional[int], log: List[LogEntry]):
        """Save current term immediately."""
        self.save_persistent_state(current_term, voted_for, log)

    def save_voted_for(self, current_term: int, voted_for: Optional[int], log: List[LogEntry]):
        """Save voted_for immediately."""
        self.save_persistent_state(current_term, voted_for, log)

    def append_log_entry(self, entry: LogEntry, current_term: int, voted_for: Optional[int], log: List[LogEntry]):
        """Append a single entry to log and save."""
        self.save_persistent_state(current_term, voted_for, log)

    def append_log_entries(
        self, entries: List[LogEntry], current_term: int, voted_for: Optional[int], log: List[LogEntry]
    ):
        """Append multiple entries to log and save."""
        self.save_persistent_state(current_term, voted_for, log)

    def delete_log_from(self, index: int, current_term: int, voted_for: Optional[int], log: List[LogEntry]):
        """Delete log entries from index onwards."""
        self.save_persistent_state(current_term, voted_for, log)

    def clear_persistent_state(self):
        """Clear all persistent state from disk."""
        with self.lock:
            try:
                if os.path.exists(self.persistent_state_file):
                    os.remove(self.persistent_state_file)
                if os.path.exists(self.log_file):
                    os.remove(self.log_file)
            except Exception:
                pass

    def load_state_machine(self) -> Dict[str, str]:
        """Load state machine from disk."""
        with self.lock:
            if os.path.exists(self.state_file):
                try:
                    with open(self.state_file, "r") as f:
                        return json.load(f)
                except Exception:
                    return {}
            return {}

    def save_state_machine(self, state: Dict[str, str]):
        """Save state machine to disk."""
        with self.lock:
            try:
                temp_file = self.state_file + ".tmp"
                with open(temp_file, "w") as f:
                    json.dump(state, f, indent=2)
                os.replace(temp_file, self.state_file)
            except Exception:
                pass

    def update_state_machine_key(self, key: str, value: str, state: Dict[str, str]):
        """Update a single key-value pair and save."""
        state[key] = value
        self.save_state_machine(state)

    def delete_state_machine_key(self, key: str, state: Dict[str, str]):
        """Delete a key from state machine and save."""
        if key in state:
            del state[key]
            self.save_state_machine(state)

    def clear_state_machine(self):
        """Clear all state machine data."""
        with self.lock:
            try:
                if os.path.exists(self.state_file):
                    os.remove(self.state_file)
            except Exception:
                pass

    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about stored data."""
        with self.lock:
            info = {
                "node_id": self.node_id,
                "persistent_state_file": self.persistent_state_file,
                "log_file": self.log_file,
                "state_file": self.state_file,
                "files": {},
            }

            for file_path in [self.persistent_state_file, self.log_file, self.state_file]:
                if os.path.exists(file_path):
                    info["files"][os.path.basename(file_path)] = {
                        "exists": True,
                        "size_bytes": os.path.getsize(file_path),
                    }
                else:
                    info["files"][os.path.basename(file_path)] = {"exists": False}

            return info
