"""
Telemetry collection and summary utilities for ingestion jobs.

This module tracks:
- records ingested
- records failed / quarantined
- latency
- lag
- throughput
- basic job timing
"""

from __future__ import annotations

from dataclasses import asdict
import time
from typing import Any

import pandas as pd

from src.models import TelemetryRecord
from src.utils import now_utc_iso, parse_ts
from src.io_utils import write_json


class TelemetryCollector:
    """
    Collects telemetry metrics during an ingestion job run.
    """

    def __init__(self) -> None:
        self.job_id = ""
        self.source_id = ""
        self.dataset_id = ""
        self.execution_mode = ""

        self.records_ingested = 0
        self.records_failed = 0

        self.source_timestamps: list[pd.Timestamp] = []
        self.ingestion_timestamps: list[pd.Timestamp] = []

        self.started_at: str | None = None
        self.ended_at: str | None = None
        self._start_perf: float | None = None

    def start_job(
        self,
        job_id: str,
        source_id: str,
        dataset_id: str,
        execution_mode: str,
    ) -> None:
        """
        Start telemetry collection for a job.
        """
        self.job_id = job_id
        self.source_id = source_id
        self.dataset_id = dataset_id
        self.execution_mode = execution_mode
        self.started_at = now_utc_iso()
        self._start_perf = time.perf_counter()

    def increment_ingested(self, n: int = 1) -> None:
        """
        Increase count of successfully ingested records.
        """
        self.records_ingested += n

    def increment_failed(self, n: int = 1) -> None:
        """
        Increase count of failed or quarantined records.
        """
        self.records_failed += n

    def record_source_and_ingestion_times(self, source_ts: Any, ingestion_ts: Any) -> None:
        """
        Store source and ingestion timestamps for latency calculations.
        
        Only append when both timestamps are valid and parsed successfully.
        """
        parsed_source = parse_ts(source_ts)
        parsed_ingestion = parse_ts(ingestion_ts)

        if pd.notna(parsed_source) and pd.notna(parsed_ingestion):
            self.source_timestamps.append(parsed_source)
            self.ingestion_timestamps.append(parsed_ingestion)

    def finish_job(self) -> TelemetryRecord:
        """
        Finish telemetry collection and return a TelemetryRecord object.
        """
        self.ended_at = now_utc_iso()

        elapsed = 0.0
        if self._start_perf is not None:
            elapsed = max(time.perf_counter() - self._start_perf, 1e-9)

        ingestion_latency = compute_ingestion_latency_seconds(
            self.source_timestamps,
            self.ingestion_timestamps,
        )

        processing_lag = compute_processing_lag_seconds(
            self.source_timestamps,
            self.ingestion_timestamps,
        )

        throughput = compute_throughput(self.records_ingested, elapsed)

        return TelemetryRecord(
            job_id=self.job_id,
            source_id=self.source_id,
            dataset_id=self.dataset_id,
            execution_mode=self.execution_mode,
            records_ingested=self.records_ingested,
            records_failed=self.records_failed,
            ingestion_latency_seconds=ingestion_latency,
            processing_lag_seconds=processing_lag,
            throughput_rps=throughput,
            started_at=self.started_at,
            ended_at=self.ended_at,
        )

    def to_dict(self, record: TelemetryRecord) -> dict:
        """
        Convert a TelemetryRecord dataclass into a plain dictionary.
        """
        return asdict(record)


def compute_ingestion_latency_seconds(
    source_timestamps: list[pd.Timestamp],
    ingestion_timestamps: list[pd.Timestamp],
) -> float:
    """
    Compute average source-to-ingestion latency in seconds.

    If lengths differ, only compare up to the shortest available list length.
    """
    if not source_timestamps or not ingestion_timestamps:
        return 0.0

    n = min(len(source_timestamps), len(ingestion_timestamps))
    deltas = []

    for i in range(n):
        src = source_timestamps[i]
        ing = ingestion_timestamps[i]

        if pd.notna(src) and pd.notna(ing):
            deltas.append((ing - src).total_seconds())

    return float(sum(deltas) / len(deltas)) if deltas else 0.0


def compute_processing_lag_seconds(
    latest_available_ts: list[pd.Timestamp],
    latest_processed_ts: list[pd.Timestamp],
) -> float:
    """
    Compute lag in seconds between the latest available source timestamp
    and the latest processed/ingested timestamp.
    """
    if not latest_available_ts or not latest_processed_ts:
        return 0.0

    max_source = max(latest_available_ts)
    max_processed = max(latest_processed_ts)

    return float((max_processed - max_source).total_seconds())


def compute_throughput(records: int, elapsed_seconds: float) -> float:
    """
    Compute records processed per second.
    """
    if elapsed_seconds <= 0:
        return 0.0
    return float(records / elapsed_seconds)


def write_telemetry(record: dict, output_path: str) -> None:
    """
    Write telemetry dictionary to JSON output.
    """
    write_json(record, output_path)


def write_job_summary(summary: dict, output_path: str) -> None:
    """
    Write a job summary dictionary to JSON output.
    """
    write_json(summary, output_path)
