from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils import generate_uuid, now_utc_iso, to_serializable_record
from src.io_utils import write_json, read_json


class CheckpointManager:
    """
    Manages checkpoint persistence for CDC pipelines.

    Important convention:
    - last_offset = -1 means "nothing processed yet"
    - this ensures offset 0 is included on the first run
    """

    def __init__(self, checkpoint_path: str | Path) -> None:
        self.checkpoint_path = Path(checkpoint_path)
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, offset: int, watermark: str | None = None, extra: dict | None = None) -> None:
        data: dict[str, Any] = {
            "last_offset": offset,
            "last_watermark": watermark,
            "saved_at": now_utc_iso(),
        }
        if extra:
            data.update(extra)
        write_json(data, self.checkpoint_path)

    def load(self) -> dict:
        """
        Load checkpoint state.

        If checkpoint does not exist, return initial "nothing processed" state.
        """
        if not self.checkpoint_path.exists():
            return {"last_offset": -1, "last_watermark": None}

        try:
            data = read_json(self.checkpoint_path)
            if not isinstance(data, dict):
                return {"last_offset": -1, "last_watermark": None}
            return data
        except Exception:
            return {"last_offset": -1, "last_watermark": None}

    def get_last_offset(self) -> int:
        return int(self.load().get("last_offset", -1))

    def get_last_watermark(self) -> str | None:
        return self.load().get("last_watermark", None)

    def reset(self) -> None:
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()


class OffsetManager:
    """
    Tracks committed and pending offsets.
    """

    def __init__(self, start_offset: int = 0) -> None:
        self._current_offset: int = start_offset
        self._committed_offset: int = start_offset - 1
        self._pending: list[int] = []

    def next_offset(self) -> int:
        offset = self._current_offset
        self._current_offset += 1
        self._pending.append(offset)
        return offset

    def commit(self, offset: int) -> None:
        self._committed_offset = offset
        self._pending = [o for o in self._pending if o > offset]

    def get_committed_offset(self) -> int:
        return self._committed_offset

    def get_pending_offsets(self) -> list[int]:
        return list(self._pending)

    def reset_to(self, offset: int) -> None:
        self._current_offset = offset + 1
        self._committed_offset = offset
        self._pending = []


class TransactionalSink:
    """
    Atomic append-style JSONL sink:
    either the batch is fully committed or not committed.
    """

    def __init__(self, output_path: str | Path) -> None:
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._temp_path = self.output_path.with_suffix(self.output_path.suffix + ".tmp")
        self._buffer: list[dict] = []

    def write(self, record: dict) -> None:
        self._buffer.append(record)

    def write_batch(self, records: list[dict]) -> None:
        self._buffer.extend(records)

    def commit(self) -> int:
        if not self._buffer:
            return 0

        try:
            existing: list[dict] = []
            if self.output_path.exists():
                with open(self.output_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            existing.append(json.loads(line))

            all_records = existing + self._buffer

            with open(self._temp_path, "w", encoding="utf-8") as f:
                for rec in all_records:
                    f.write(json.dumps(rec, default=str) + "\n")

            os.replace(self._temp_path, self.output_path)

            count = len(self._buffer)
            self._buffer.clear()
            return count

        except Exception as e:
            self.rollback()
            raise RuntimeError(f"TransactionalSink commit failed: {e}") from e

    def rollback(self) -> None:
        self._buffer.clear()
        if self._temp_path.exists():
            self._temp_path.unlink()

    def buffer_size(self) -> int:
        return len(self._buffer)


def build_cdc_event(
    operation: str,
    after: dict | None,
    source_id: str,
    dataset_id: str,
    offset: int,
    before: dict | None = None,
    schema_version: str = "v1",
) -> dict[str, Any]:
    allowed_ops = {"INSERT", "UPDATE", "DELETE", "SNAPSHOT"}
    if operation not in allowed_ops:
        raise ValueError(f"Invalid operation '{operation}'. Allowed: {allowed_ops}")

    return {
        "cdc_event_id": generate_uuid(),
        "operation": operation,
        "offset": offset,
        "before": to_serializable_record(before) if before else None,
        "after": to_serializable_record(after) if after else None,
        "source_id": source_id,
        "dataset_id": dataset_id,
        "schema_version": schema_version,
        "captured_at": now_utc_iso(),
    }


def build_cdc_events_from_df(
    df: pd.DataFrame,
    operation: str,
    source_id: str,
    dataset_id: str,
    start_offset: int = 0,
    schema_version: str = "v1",
) -> list[dict]:
    events = []
    for i, row in enumerate(df.to_dict(orient="records")):
        event = build_cdc_event(
            operation=operation,
            after=row if operation != "DELETE" else None,
            before=row if operation == "DELETE" else None,
            source_id=source_id,
            dataset_id=dataset_id,
            offset=start_offset + i,
            schema_version=schema_version,
        )
        events.append(event)
    return events