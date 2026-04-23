"""
Timestamp-based CDC pipeline.

This strategy detects new or changed rows using a watermark column such as
last_updated_at and checkpointed watermark progression.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.ingestion_base import BaseIngestionPipeline
from src.contracts import ContractValidator, ContractValidationResult
from src.cdc_common import CheckpointManager, TransactionalSink, build_cdc_event
from src.io_utils import read_csv_source, write_json, write_dataframe_csv
from src.telemetry import TelemetryCollector, write_telemetry, write_job_summary
from src.utils import now_utc_iso, safe_mkdir


class TimestampCDCPipeline(BaseIngestionPipeline):
    """
    Timestamp-based CDC:
    - reads source snapshot/file
    - filters rows newer than checkpoint watermark
    - validates rows
    - emits CDC events
    - stores checkpoint for exactly-once-ish resumability
    """

    def load_source(self, input_path: str | Path) -> pd.DataFrame:
        return read_csv_source(input_path)

    def validate(self, df: pd.DataFrame) -> ContractValidationResult:
        validator = ContractValidator()
        return validator.validate_dataframe(
            df=df,
            contract=self.contract,
            source_name=self.source.source_id,
        )

    def wrap(self, df: pd.DataFrame, start_offset: int = 0) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for i, row in enumerate(df.to_dict(orient="records")):
            event = build_cdc_event(
                operation="UPDATE",
                after=row,
                before=None,
                source_id=self.source.source_id,
                dataset_id=self.dataset.dataset_id,
                offset=start_offset + i,
                schema_version=self.dataset.schema_version,
            )
            events.append(event)
        return events

    def write_outputs(self, events: list[dict[str, Any]], sink: TransactionalSink) -> int:
        sink.write_batch(events)
        return sink.commit()

    def run(
        self,
        input_path: str | Path,
        accepted_path: str | Path,
        quarantine_path: str | Path,
        checkpoint_path: str | Path,
        cdc_log_path: str | Path,
        telemetry_path: str | Path,
        summary_path: str | Path,
    ) -> dict[str, Any]:

        for p in [
            accepted_path,
            quarantine_path,
            checkpoint_path,
            cdc_log_path,
            telemetry_path,
            summary_path,
        ]:
            safe_mkdir(Path(p).parent)

        checkpoint = CheckpointManager(checkpoint_path)
        last_watermark = checkpoint.get_last_watermark()

        df = self.load_source(input_path)
        watermark_col = self.source.watermark_column or "last_updated_at"

        if watermark_col not in df.columns:
            raise ValueError(
                f"Timestamp CDC requires watermark column '{watermark_col}' in source data."
            )

        df[watermark_col] = pd.to_datetime(df[watermark_col], errors="coerce", utc=True)

        if last_watermark:
            last_watermark_ts = pd.to_datetime(last_watermark, errors="coerce", utc=True)
            candidate_df = df[df[watermark_col] > last_watermark_ts].copy()
        else:
            candidate_df = df.copy()

        total_candidates = len(candidate_df)

        if total_candidates == 0:
            summary = {
                "job_id": self.job.job_id,
                "strategy": "timestamp_cdc",
                "message": "No new rows found beyond watermark.",
                "last_watermark": last_watermark,
                "finished_at": now_utc_iso(),
            }
            write_job_summary(summary, summary_path)
            return summary

        validation = self.validate(candidate_df)
        accepted_df = validation.accepted_df.copy()
        quarantined_df = validation.quarantined_df.copy()

        if not quarantined_df.empty:
            write_dataframe_csv(quarantined_df, quarantine_path)

        telemetry = TelemetryCollector()
        telemetry.start_job(
            job_id=self.job.job_id,
            source_id=self.source.source_id,
            dataset_id=self.dataset.dataset_id,
            execution_mode=self.job.execution_mode,
        )

        events = self.wrap(accepted_df, start_offset=0)

        sink = TransactionalSink(accepted_path)
        committed = self.write_outputs(events, sink)

        new_watermark = None
        if not accepted_df.empty:
            new_watermark = str(accepted_df[watermark_col].max())

        checkpoint.save(
            offset=committed,
            watermark=new_watermark or last_watermark,
            extra={"strategy": "timestamp_cdc"},
        )

        telemetry.increment_ingested(len(accepted_df))
        telemetry.increment_failed(len(quarantined_df))

        for source_ts in accepted_df[watermark_col]:
            telemetry.record_source_and_ingestion_times(source_ts, now_utc_iso())

        telemetry_record = telemetry.finish_job()
        write_telemetry(telemetry.to_dict(telemetry_record), telemetry_path)

        cdc_log = {
            "job_id": self.job.job_id,
            "strategy": "timestamp_cdc",
            "source_id": self.source.source_id,
            "watermark_column": watermark_col,
            "previous_watermark": last_watermark,
            "new_watermark": new_watermark,
            "candidate_rows": total_candidates,
            "events_emitted": len(events),
            "quarantined": len(quarantined_df),
            "committed": committed,
            "run_at": now_utc_iso(),
        }
        write_json(cdc_log, cdc_log_path)

        summary = {
            "job_id": self.job.job_id,
            "strategy": "timestamp_cdc",
            "source_id": self.source.source_id,
            "dataset_id": self.dataset.dataset_id,
            "candidate_rows": total_candidates,
            "events_emitted": len(events),
            "quarantined": len(quarantined_df),
            "new_watermark": new_watermark,
            "finished_at": now_utc_iso(),
        }
        write_job_summary(summary, summary_path)
        return summary