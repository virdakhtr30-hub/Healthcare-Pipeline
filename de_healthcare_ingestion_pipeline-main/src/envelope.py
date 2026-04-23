"""
Event envelope construction for ingestion outputs.

Wraps each record with metadata so downstream layers (storage, CDC, serving)
can track lineage, ordering, and provenance.
"""

from __future__ import annotations

from typing import Any, Iterable, List
from datetime import datetime, timezone

import pandas as pd

from config.constants import DEFAULT_SCHEMA_VERSION
from src.utils import generate_uuid, now_utc_iso, hash_trace_id, to_serializable_record


def _ensure_ts_str(value: Any) -> str | None:
    """
    Convert a value to an ISO-8601 UTC string if possible.
    Returns None if conversion fails or value is null.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    # pandas-friendly parsing
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts.isoformat()


def _pick_source_ts(record: dict[str, Any]) -> str | None:
    """
    Heuristic to pick a source timestamp from common fields.
    Priority order:
      source_timestamp > event_timestamp > last_updated_at > result_timestamp > admit_datetime
    """
    for key in ("source_timestamp", "event_timestamp", "last_updated_at", "result_timestamp", "admit_datetime"):
        if key in record:
            ts = _ensure_ts_str(record.get(key))
            if ts:
                return ts
    return None


def _pick_record_id(record: dict[str, Any]) -> str:
    """
    Heuristic to pick a stable record identifier from common keys.
    """
    for key in ("event_id", "lab_result_id", "diagnosis_id", "death_record_id", "encounter_id"):
        if key in record and record.get(key) is not None:
            return str(record.get(key))
    # fallback to random id if nothing present
    return generate_uuid()


def build_envelope(
    record: dict[str, Any],
    *,
    source_id: str,
    dataset_id: str,
    operation_type: str = "SNAPSHOT",
    schema_version: str = DEFAULT_SCHEMA_VERSION,
    ingestion_ts: str | None = None,
) -> dict[str, Any]:
    """
    Build a single event envelope for a record.

    Args:
        record: Input record (dict).
        source_id: Source name (e.g., 'lab_results').
        dataset_id: Logical dataset name (e.g., 'lab_results_dataset').
        operation_type: INSERT/UPDATE/DELETE/SNAPSHOT.
        schema_version: Version string.
        ingestion_ts: Optional ingestion timestamp (ISO string). If None, uses now.

    Returns:
        Envelope dictionary.
    """
    ingestion_timestamp = ingestion_ts or now_utc_iso()

    source_ts = _pick_source_ts(record)
    record_id = _pick_record_id(record)

    trace_id = hash_trace_id(
        source_id=source_id,
        record_id=record_id,
        ts=source_ts or ingestion_timestamp,
    )

    payload = to_serializable_record(record)

    envelope = {
        "event_id": generate_uuid(),
        "event_timestamp": ingestion_timestamp,
        "source_timestamp": source_ts,
        "schema_version": schema_version,
        "ingestion_timestamp": ingestion_timestamp,
        "operation_type": operation_type,
        "trace_id": trace_id,
        "source_id": source_id,
        "dataset_id": dataset_id,
        "payload": payload,
    }

    return envelope


def build_envelopes_from_df(
    df: pd.DataFrame,
    *,
    source_id: str,
    dataset_id: str,
    operation_type: str = "SNAPSHOT",
    schema_version: str = DEFAULT_SCHEMA_VERSION,
) -> List[dict[str, Any]]:
    """
    Build envelopes for all rows in a DataFrame.

    Args:
        df: Input DataFrame.
        source_id: Source name.
        dataset_id: Dataset name.
        operation_type: Operation type.
        schema_version: Schema version.

    Returns:
        List of envelope dictionaries.
    """
    records = df.to_dict(orient="records")
    envelopes: List[dict[str, Any]] = []

    for rec in records:
        envelopes.append(
            build_envelope(
                rec,
                source_id=source_id,
                dataset_id=dataset_id,
                operation_type=operation_type,
                schema_version=schema_version,
            )
        )

    return envelopes


def stream_envelopes(
    records: Iterable[dict[str, Any]],
    *,
    source_id: str,
    dataset_id: str,
    operation_type: str = "INSERT",
    schema_version: str = DEFAULT_SCHEMA_VERSION,
):
    """
    Generator that yields envelopes one-by-one (useful for streaming).

    Args:
        records: Iterable of input records.
        source_id: Source name.
        dataset_id: Dataset name.
        operation_type: Operation type.
        schema_version: Schema version.

    Yields:
        Envelope dict per record.
    """
    for rec in records:
        yield build_envelope(
            rec,
            source_id=source_id,
            dataset_id=dataset_id,
            operation_type=operation_type,
            schema_version=schema_version,
        )
