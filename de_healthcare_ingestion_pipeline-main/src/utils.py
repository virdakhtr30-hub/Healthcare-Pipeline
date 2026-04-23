"""
General helper utilities used across the ingestion project.
"""

from __future__ import annotations

import json
import uuid
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Iterable
from src.io_utils import write_json as io_write_json

import pandas as pd
import yaml


def now_utc_iso() -> str:
    """
    Return the current UTC timestamp in ISO 8601 string format.
    """
    return datetime.now(timezone.utc).isoformat()


def generate_uuid() -> str:
    """
    Generate a random UUID4 string.
    """
    return str(uuid.uuid4())


def safe_mkdir(path: str | Path) -> None:
    """
    Create a directory if it does not already exist.

    Args:
        path: Directory path to create.
    """
    Path(path).mkdir(parents=True, exist_ok=True)


def read_yaml(path: str | Path) -> dict:
    """
    Read a YAML file and return it as a Python dictionary.

    Args:
        path: Path to the YAML file.

    Returns:
        Parsed YAML content.

    Raises:
        FileNotFoundError: If file does not exist.
        ValueError: If YAML is empty or not a dictionary.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"YAML file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if data is None:
        raise ValueError(f"YAML file is empty: {path}")

    if not isinstance(data, dict):
        raise ValueError(f"YAML top-level structure must be a dictionary: {path}")

    return data


def write_json(obj: dict | list, path: str | Path) -> None:
    """
    Backward-compatible JSON writer wrapper.

    Canonical project-wide signature:
        write_json(obj, path)

    Delegates to src.io_utils.write_json().
    """
    io_write_json(obj, path)


def read_json(path: str | Path) -> dict:
    """
    Read a JSON file and return its contents.

    Args:
        path: Path to JSON file.

    Returns:
        Parsed JSON dictionary.
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_ts(value: Any) -> pd.Timestamp:
    """
    Convert any value into a pandas UTC Timestamp.

    Returns:
        pd.Timestamp or NaT if parsing fails.
    """
    return pd.to_datetime(value, errors="coerce", utc=True)


def coerce_bool(value: Any) -> bool | None:
    """
    Attempt to coerce common string/int/float representations into bool.

    Accepted truthy values:
        True, 1, "true", "t", "yes", "y", "1"

    Accepted falsy values:
        False, 0, "false", "f", "no", "n", "0"

    Returns:
        True / False / None
    """
    if pd.isna(value):
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "yes", "y", "1"}:
            return True
        if normalized in {"false", "f", "no", "n", "0"}:
            return False

    return None


def hash_trace_id(source_id: str, record_id: str, ts: str) -> str:
    """
    Create a deterministic trace ID from source, record key, and timestamp.

    Args:
        source_id: Source name/id.
        record_id: Record identifier.
        ts: Timestamp string.

    Returns:
        SHA256 hash string.
    """
    raw = f"{source_id}|{record_id}|{ts}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def chunk_iterable(iterable: Iterable, size: int):
    """
    Yield successive fixed-size chunks from an iterable.

    Args:
        iterable: Any iterable object.
        size: Chunk size.

    Yields:
        Lists of length <= size.
    """
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def normalize_whitespace(value: Any) -> Any:
    """
    Strip leading and trailing whitespace from strings.
    Leave non-string values unchanged.
    """
    if isinstance(value, str):
        return value.strip()
    return value


def normalize_upper(value: Any) -> Any:
    """
    Strip whitespace and uppercase string values.
    Leave non-string values unchanged.
    """
    if isinstance(value, str):
        return value.strip().upper()
    return value


def ensure_list(value: Any) -> list:
    """
    Ensure the returned object is always a list.

    Rules:
    - None -> []
    - list -> unchanged
    - tuple/set -> converted to list
    - scalar -> [scalar]
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, (tuple, set)):
        return list(value)
    return [value]


def to_serializable_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a dictionary into a JSON-serializable record.

    Useful before writing event envelopes or telemetry payloads.
    """
    out = {}
    for key, value in record.items():
        if isinstance(value, pd.Timestamp):
            out[key] = value.isoformat()
        elif pd.isna(value) if not isinstance(value, (list, dict, tuple, set)) else False:
            out[key] = None
        else:
            out[key] = value
    return out