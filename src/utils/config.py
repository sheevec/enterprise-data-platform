"""
config.py
---------
Shared environment-variable helpers for all platform components.

Convention: fail loudly on missing required config (a misconfigured job must
never start half-configured), coerce types defensively, and centralize the
env-var names in one place.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional


def require_env(name: str) -> str:
    """Return an env var or raise with a clear message."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable {name} is not set.")
    return value


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Env var {name}={raw!r} is not a valid integer.") from exc


def get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y")


def get_json_list(name: str, default: str = "[]") -> List[Dict[str, Any]]:
    """Parse a JSON array-of-objects env var (used for topic configs)."""
    raw = os.getenv(name, default)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Env var {name} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, list):
        raise ValueError(f"Env var {name} must be a JSON array.")
    return parsed
