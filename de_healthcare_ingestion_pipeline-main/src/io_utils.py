"""
File I/O utilities for CSV, JSONL, and related project outputs.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Sequence, Any

import pandas as pd


def read_csv_source(path: str | Path) -> pd.DataFrame:
    """
    Read a CSV source file into a pandas DataFrame.

    Args:
        path: Path to the CSV file.

    Returns:
        Loaded pandas DataFrame.
    """
    return pd.read_csv(path)


def write_jsonl(records: Sequence[dict], path: str | Path) -> None:
    """
    Write a sequence of dictionaries to a JSONL file.

    Args:
        records: Sequence of dictionary records.
        path: Output JSONL file path.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")


def append_jsonl(records: Iterable[dict], path: str | Path) -> None:
    """
    Append a sequence of dictionaries to a JSONL file.

    Args:
        records: Iterable of dictionary records.
        path: Output JSONL file path.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")


def read_jsonl(path: str | Path) -> list[dict]:
    """
    Read a JSONL file into a list of dictionaries.

    Args:
        path: Path to JSONL file.

    Returns:
        List of parsed JSON objects.
    """
    path = Path(path)

    if not path.exists():
        return []

    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    return records


def write_dataframe_csv(df: pd.DataFrame, path: str | Path) -> None:
    """
    Write a pandas DataFrame to CSV.

    Args:
        df: DataFrame to save.
        path: Output CSV file path.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def read_dataframe_csv(path: str | Path) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame.

    Args:
        path: Input CSV path.

    Returns:
        Loaded DataFrame.
    """
    return pd.read_csv(path)


def write_text(text: str, path: str | Path) -> None:
    """
    Write plain text to a file.

    Args:
        text: Text content to write.
        path: Output file path.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def read_text(path: str | Path) -> str:
    """
    Read plain text from a file.

    Args:
        path: Input file path.

    Returns:
        File contents as string.
    """
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_json(obj: dict | list, path: str | Path) -> None:
    """
    Write a dictionary or list to a JSON file.

    Args:
        obj: Python object to serialize.
        path: Output JSON path.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)


def read_json(path: str | Path) -> dict | list:
    """
    Read a JSON file.

    Args:
        path: Input JSON path.

    Returns:
        Parsed Python object.
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def file_exists(path: str | Path) -> bool:
    """
    Check whether a file exists.

    Args:
        path: File path.

    Returns:
        True if file exists, else False.
    """
    return Path(path).exists()


def ensure_parent_dir(path: str | Path) -> None:
    """
    Ensure the parent directory for a file path exists.

    Args:
        path: File path whose parent should be created.
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def save_plot(fig: Any, path: str | Path) -> None:
    """
    Save a matplotlib figure and close it safely.

    Args:
        fig: Matplotlib figure object.
        path: Output image path.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, bbox_inches="tight")
    fig.clf()
