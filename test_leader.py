#!/usr/bin/env python
"""Enhanced leader discovery test"""

import time
from client import RaftClient

node_addresses = {
    0: ("localhost", 5000),
    1: ("localhost", 5001),
    2: ("localhost", 5002),
    3: ("localhost", 5003),
    4: ("localhost", 5004),
    5: ("localhost", 5005),
    6: ("localhost", 5006),
}

print("Connecting to cluster...")
client = RaftClient(node_addresses)

print("\n" + "="*60)
print("Checking cluster status...")
print("="*60)
client.print_cluster_status()

print("Searching for leader...")
leader = client.get_leader()

if leader is not None:
    print(f"✅ Found leader: Node {leader}\n")
    
    # Try sending a test command
    print(f"Sending test command to Node {leader}...")
    success, msg = client.send_command("GET test")
    print(f"Result: {msg}\n")
else:
    print("❌ Could not find leader\n")

client.close()
