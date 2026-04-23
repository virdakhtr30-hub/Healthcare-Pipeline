"""
Streaming ingestion implementation for simulated vital signs.

This module simulates real-time ingestion using chunked/micro-batch processing
over the vitals CSV source.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd

from src.ingestion_base import BaseIngestionPipeline
from src.contracts import ContractValidator, ContractValidationResult
from src.envelope import build_envelope
from src.io_utils import read_csv_source, append_jsonl, write_dataframe_csv
from src.telemetry import TelemetryCollector, write_telemetry, write_job_summary
from src.utils import safe_mkdir, now_utc_iso


class StreamingIngestionPipeline(BaseIngestionPipeline):
    """
    Implements simulated streaming ingestion for the vitals source.

    Modes supported:
    - steady stream: fixed records/sec
    - burst stream: large chunk in a short time window
    """

    def load_source(self, input_path: str | Path) -> pd.DataFrame:
        """
        Load the streaming seed CSV into a DataFrame.
        """
        return read_csv_source(input_path)

    def validate(self, df: pd.DataFrame) -> ContractValidationResult:
        """
        Validate a DataFrame chunk against the configured contract.
        """
        validator = ContractValidator()
        return validator.validate_dataframe(
            df=df,
            contract=self.contract,
            source_name=self.source.source_id,
        )

    def wrap(self, accepted_df: pd.DataFrame, operation_type: str = "INSERT") -> list[dict[str, Any]]:
        """
        Wrap accepted rows into per-record event envelopes.
        """
        envelopes: list[dict[str, Any]] = []

        for record in accepted_df.to_dict(orient="records"):
            envelopes.append(
                build_envelope(
                    record=record,
                    source_id=self.source.source_id,
                    dataset_id=self.dataset.dataset_id,
                    operation_type=operation_type,
                    schema_version=self.dataset.schema_version,
                )
            )

        return envelopes

    def write_outputs(
        self,
        accepted_records: list[dict[str, Any]],
        quarantined_df: pd.DataFrame,
        accepted_path: str | Path,
        quarantine_path: str | Path,
    ) -> None:
        """
        Append accepted JSONL records and write/update quarantine CSV.
        """
        if accepted_records:
            append_jsonl(accepted_records, accepted_path)

        if not quarantined_df.empty:
            # overwrite-safe simple approach for coursework:
            # if file exists, append by reading old + new and re-writing
            quarantine_path = Path(quarantine_path)
            if quarantine_path.exists():
                existing = pd.read_csv(quarantine_path)
                combined = pd.concat([existing, quarantined_df], ignore_index=True)
                write_dataframe_csv(combined, quarantine_path)
            else:
                write_dataframe_csv(quarantined_df, quarantine_path)

    def run(self, *args, **kwargs) -> dict[str, Any]:
        """
        Generic run entry point required by BaseIngestionPipeline.

        Use:
        - scenario="steady" to call run_steady_stream()
        - scenario="burst" to call run_burst_stream()

        Raises:
            ValueError if scenario is missing or unsupported.
        """
        scenario = kwargs.pop("scenario", None)

        if scenario == "steady":
            return self.run_steady_stream(*args, **kwargs)
        elif scenario == "burst":
            return self.run_burst_stream(*args, **kwargs)
        else:
            raise ValueError(
                "StreamingIngestionPipeline.run() requires scenario='steady' or scenario='burst'."
            )
    
    def run_steady_stream(
        self,
        input_path: str | Path,
        accepted_path: str | Path,
        quarantine_path: str | Path,
        telemetry_path: str | Path,
        summary_path: str | Path,
        rate_per_sec: int = 10,
        duration_seconds: int = 30,
    ) -> dict[str, Any]:
        """
        Simulate a steady stream by ingesting records one by one at a fixed rate.

        Args:
            input_path: Source CSV path.
            accepted_path: Accepted JSONL output path.
            quarantine_path: Quarantine CSV output path.
            telemetry_path: Telemetry JSON path.
            summary_path: Summary JSON path.
            rate_per_sec: Number of records per second.
            duration_seconds: Total run duration.

        Returns:
            Summary dictionary.
        """
        for parent in [
            Path(accepted_path).parent,
            Path(quarantine_path).parent,
            Path(telemetry_path).parent,
            Path(summary_path).parent,
        ]:
            safe_mkdir(parent)

        df = self.load_source(input_path)

        # Use only as many rows as needed for the scenario
        target_rows = min(len(df), rate_per_sec * duration_seconds)
        stream_df = df.head(target_rows).copy()

        telemetry = TelemetryCollector()
        telemetry.start_job(
            job_id=self.job.job_id,
            source_id=self.source.source_id,
            dataset_id=self.dataset.dataset_id,
            execution_mode=self.job.execution_mode,
        )

        total_quarantined = 0

        for _, row in stream_df.iterrows():
            chunk_df = pd.DataFrame([row.to_dict()])
            validation = self.validate(chunk_df)

            accepted_df = validation.accepted_df
            quarantined_df = validation.quarantined_df
            rejected_df = validation.rejected_df

            accepted_records = self.wrap(accepted_df, operation_type="INSERT") if not accepted_df.empty else []

            self.write_outputs(
                accepted_records=accepted_records,
                quarantined_df=quarantined_df,
                accepted_path=accepted_path,
                quarantine_path=quarantine_path,
            )

            telemetry.increment_ingested(len(accepted_df))
            telemetry.increment_failed(len(quarantined_df) + len(rejected_df))
            total_quarantined += len(quarantined_df)

            watermark_col = self.source.watermark_column
            if watermark_col and watermark_col in accepted_df.columns:
                for source_ts in accepted_df[watermark_col]:
                    telemetry.record_source_and_ingestion_times(source_ts, now_utc_iso())

            # simulate real-time pace
            time.sleep(1 / rate_per_sec)

        telemetry_record = telemetry.finish_job()
        telemetry_dict = telemetry.to_dict(telemetry_record)
        write_telemetry(telemetry_dict, telemetry_path)

        summary = {
            "job_id": self.job.job_id,
            "scenario": "steady_stream",
            "source_id": self.source.source_id,
            "dataset_id": self.dataset.dataset_id,
            "execution_mode": self.job.execution_mode,
            "rate_per_sec": rate_per_sec,
            "duration_seconds": duration_seconds,
            "records_total_input": int(target_rows),
            "records_ingested": int(telemetry.records_ingested),
            "records_quarantined": int(total_quarantined),
            "records_failed": int(telemetry.records_failed),
            "finished_at": now_utc_iso(),
        }
        write_job_summary(summary, summary_path)

        return summary

    def run_burst_stream(
        self,
        input_path: str | Path,
        accepted_path: str | Path,
        quarantine_path: str | Path,
        telemetry_path: str | Path,
        summary_path: str | Path,
        burst_size: int = 5000,
        burst_window_seconds: int = 1,
        pause_seconds: int = 3,
    ) -> dict[str, Any]:
        """
        Simulate a burst stream by ingesting a large chunk in a very short interval.

        Args:
            input_path: Source CSV path.
            accepted_path: Accepted JSONL output path.
            quarantine_path: Quarantine CSV output path.
            telemetry_path: Telemetry JSON path.
            summary_path: Summary JSON path.
            burst_size: Number of records in the burst.
            burst_window_seconds: Intended burst duration.
            pause_seconds: Post-burst pause to simulate backlog recovery window.

        Returns:
            Summary dictionary.
        """
        for parent in [
            Path(accepted_path).parent,
            Path(quarantine_path).parent,
            Path(telemetry_path).parent,
            Path(summary_path).parent,
        ]:
            safe_mkdir(parent)

        df = self.load_source(input_path)

        # sample with replacement to simulate a large burst if needed
        if len(df) >= burst_size:
            burst_df = df.head(burst_size).copy()
        else:
            burst_df = df.sample(n=burst_size, replace=True, random_state=42).reset_index(drop=True)

        telemetry = TelemetryCollector()
        telemetry.start_job(
            job_id=self.job.job_id,
            source_id=self.source.source_id,
            dataset_id=self.dataset.dataset_id,
            execution_mode=self.job.execution_mode,
        )

        start_time = time.perf_counter()

        validation = self.validate(burst_df)
        accepted_df = validation.accepted_df
        quarantined_df = validation.quarantined_df
        rejected_df = validation.rejected_df

        accepted_records = self.wrap(accepted_df, operation_type="INSERT") if not accepted_df.empty else []

        self.write_outputs(
            accepted_records=accepted_records,
            quarantined_df=quarantined_df,
            accepted_path=accepted_path,
            quarantine_path=quarantine_path,
        )

        telemetry.increment_ingested(len(accepted_df))
        telemetry.increment_failed(len(quarantined_df) + len(rejected_df))

        watermark_col = self.source.watermark_column
        if watermark_col and watermark_col in accepted_df.columns:
            for source_ts in accepted_df[watermark_col]:
                telemetry.record_source_and_ingestion_times(source_ts, now_utc_iso())

        elapsed = time.perf_counter() - start_time

        # If processing was faster than the nominal burst window, sleep the remainder.
        if elapsed < burst_window_seconds:
            time.sleep(burst_window_seconds - elapsed)

        # Simulate recovery pause
        time.sleep(pause_seconds)

        telemetry_record = telemetry.finish_job()
        telemetry_dict = telemetry.to_dict(telemetry_record)
        write_telemetry(telemetry_dict, telemetry_path)

        summary = {
            "job_id": self.job.job_id,
            "scenario": "burst_stream",
            "source_id": self.source.source_id,
            "dataset_id": self.dataset.dataset_id,
            "execution_mode": self.job.execution_mode,
            "burst_size": burst_size,
            "burst_window_seconds": burst_window_seconds,
            "pause_seconds": pause_seconds,
            "records_total_input": int(len(burst_df)),
            "records_ingested": int(len(accepted_df)),
            "records_quarantined": int(len(quarantined_df)),
            "records_failed": int(len(quarantined_df) + len(rejected_df)),
            "finished_at": now_utc_iso(),
        }
        write_job_summary(summary, summary_path)

        return summary
