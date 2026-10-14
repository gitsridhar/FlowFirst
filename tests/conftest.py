"""Shared test setup. Unit tests must run with no network, no .env and no cluster."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# config.py checks secrets when it is imported; tests use sample values, so opt out explicitly (dev-only switch).
os.environ.setdefault("ALLOW_DEFAULT_SECRETS", "true")
# Keep a developer's real .env from leaking into the tests.
os.environ["LOG_LEVEL"] = "WARNING"
