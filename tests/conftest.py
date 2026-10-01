import os
import sys

# The logger reads its config at import time; tests use a dummy key id and never touch the network.
os.environ.setdefault("KALSHI_KEY_ID", "test-key-id")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "logger"))
