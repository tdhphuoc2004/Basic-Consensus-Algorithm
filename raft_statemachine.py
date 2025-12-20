"""
Raft State Machine Module
Simple key-value store that persists to JSON.
Single Responsibility: State machine operations only.
"""

import json
import os
import threading
from typing import Dict


class KeyValueStore:
    """Simple key-value store that persists to JSON"""
    
    def __init__(self, node_id: int, storage_dir: str = "."):
        """Initialize key-value store."""
        self.node_id = node_id
        self.storage_dir = storage_dir
        self.filename = os.path.join(storage_dir, f"node_{node_id}_state.json")
        self.data: Dict[str, str] = {}
        self.lock = threading.RLock()
        os.makedirs(storage_dir, exist_ok=True)
        self._load_from_disk()
    
    def _load_from_disk(self):
        """Load state from JSON file."""
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    self.data = json.load(f)
            except Exception:
                self.data = {}
    
    def _save_to_disk(self):
        """Save state to JSON file."""
        try:
            with open(self.filename, 'w') as f:
                json.dump(self.data, f, indent=2)
        except Exception:
            pass
    
    def apply_command(self, command: str) -> tuple[bool, str]:
        """Apply a command to the state machine (SET/GET operations)."""
        with self.lock:
            if command.upper() == "NO-OP":
                return True, "NO-OP"
            
            parts = command.strip().split(None, 2)
            
            if not parts:
                return False, "Empty command"
            
            operation = parts[0].upper()
            
            if operation == "SET":
                if len(parts) < 3:
                    return False, "SET requires: SET <key> <value>"
                key, value = parts[1], parts[2]
                self.data[key] = value
                return True, f"SET {key}={value}"
            
            elif operation == "GET":
                if len(parts) < 2:
                    return False, "GET requires: GET <key>"
                key = parts[1]
                value = self.data.get(key, "NOT_FOUND")
                return True, f"GET {key}={value}"
            
            else:
                return False, f"Unknown command: {operation}"
    
    def get_data(self) -> Dict[str, str]:
        """Get current state machine data."""
        with self.lock:
            return self.data.copy()
    
    def get_size(self) -> int:
        """Get number of key-value pairs."""
        with self.lock:
            return len(self.data)
      
    def display(self) -> str:
        """Format current state for display."""
        with self.lock:
            if not self.data:
                return "  (empty)"
            
            lines = []
            for key, value in sorted(self.data.items()):
                lines.append(f"    {key}: {value}")
            return "\n".join(lines)
