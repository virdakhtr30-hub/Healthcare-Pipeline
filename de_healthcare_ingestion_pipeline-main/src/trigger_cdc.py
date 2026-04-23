"""
Trigger-based CDC pipeline.

This simulates database triggers by generating row-level trigger events from source
records and storing progress through offsets/checkpoints.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np

from src.ingestion_base import BaseIngestionPipeline
from src.contracts import ContractValidator, ContractValidationResult
from src.cdc_common import (
    CheckpointManager,
    OffsetManager,
    TransactionalSink,
    build_cdc_event,
)
from src.io_utils import read_csv_source, write_json, write_dataframe_csv
from src.telemetry import TelemetryCollector, write_telemetry, write_job_summary
from src.utils import now_utc_iso, generate_uuid, safe_mkdir


def simulate_trigger_log(
    source_df: pd.DataFrame,
    primary_key: str,
    watermark_col: str = "last_updated_at",
    seed: int = 42,
) -> pd.DataFrame:
    """
    Simulate a trigger log stream.

    Operations:
    - INSERT 60%
    - UPDATE 30%
    - DELETE 10%
    """
    rng = np.random.default_rng(seed)
    n = len(source_df)

    choices = ["INSERT"] * 6 + ["UPDATE"] * 3 + ["DELETE"] * 1
    operations = [choices[rng.integers(0, len(choices))] for _ in range(n)]

    rows = []
    for i, (_, row) in enumerate(source_df.iterrows()):
        op = operations[i]
        row_dict = row.to_dict()
        record_id = str(row.get(primary_key, generate_uuid()))

        if op == "INSERT":
            before = None
            after = row_dict
        elif op == "UPDATE":
            before = dict(row_dict)
            before["_simulated_before"] = True
            after = row_dict
        else:
            before = {primary_key: record_id}
            after = None

        rows.append(
            {
                "trigger_offset": i,
                "record_id": record_id,
                "operation": op,
                "trigger_timestamp": str(row.get(watermark_col, now_utc_iso())),
                "_before_row": before,
                "_after_row": after,
            }
        )

    return pd.DataFrame(rows)


class TriggerCDCPipeline(BaseIngestionPipeline):
    def load_source(self, input_path: str | Path) -> pd.DataFrame:
        return read_csv_source(input_path)

    def validate(self, df: pd.DataFrame) -> ContractValidationResult:
        validator = ContractValidator()
        return validator.validate_dataframe(
            df=df,
            contract=self.contract,
            source_name=self.source.source_id,
        )

    def wrap(self, trigger_entries: list[dict], start_offset: int = 0) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for i, entry in enumerate(trigger_entries):
            operation = str(entry.get("operation", "INSERT")).upper()
            before = entry.get("_before_row")
            after = entry.get("_after_row")

            event = build_cdc_event(
                operation=operation,
                after=after,
                before=before,
                source_id=self.source.source_id,
                dataset_id=self.dataset.dataset_id,
                offset=start_offset + i,
                schema_version=self.dataset.schema_version,
            )
            event["trigger_timestamp"] = entry.get("trigger_timestamp")
            event["record_id"] = entry.get("record_id")
            events.append(event)
        return events

    def write_outputs(self, events: list[dict], sink: TransactionalSink) -> int:
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
        last_offset = checkpoint.get_last_offset()

        df = self.load_source(input_path)
        primary_key = self.source.primary_key[0] if self.source.primary_key else "id"
        watermark_col = self.source.watermark_column or "last_updated_at"

        trigger_df = simulate_trigger_log(
            source_df=df,
            primary_key=primary_key,
            watermark_col=watermark_col,
        )

        new_trigger_df = trigger_df[trigger_df["trigger_offset"] > last_offset].copy()
        total_new = len(new_trigger_df)

        if total_new == 0:
            summary = {
                "job_id": self.job.job_id,
                "strategy": "trigger_cdc",
                "message": "No new trigger events found.",
                "last_offset": last_offset,
                "finished_at": now_utc_iso(),
            }
            write_job_summary(summary, summary_path)
            return summary

        trigger_entries = new_trigger_df.to_dict(orient="records")

        insert_update_entries = [
            e for e in trigger_entries if e.get("operation") in ("INSERT", "UPDATE")
        ]
        raw_rows = [e.get("_after_row", {}) for e in insert_update_entries if e.get("_after_row")]
        quarantine_count = 0

        if raw_rows:
            raw_df = pd.DataFrame(raw_rows)
            validation = self.validate(raw_df)
            quarantine_count = len(validation.quarantined_df)
            if not validation.quarantined_df.empty:
                write_dataframe_csv(validation.quarantined_df, quarantine_path)

        telemetry = TelemetryCollector()
        telemetry.start_job(
            job_id=self.job.job_id,
            source_id=self.source.source_id,
            dataset_id=self.dataset.dataset_id,
            execution_mode=self.job.execution_mode,
        )

        events = self.wrap(trigger_entries, start_offset=last_offset + 1)

        offset_mgr = OffsetManager(start_offset=last_offset)
        if events:
            offset_mgr.commit(events[-1]["offset"])

        sink = TransactionalSink(accepted_path)
        committed = self.write_outputs(events, sink)

        new_offset = int(new_trigger_df["trigger_offset"].max())
        checkpoint.save(
            offset=new_offset,
            watermark=now_utc_iso(),
            extra={"strategy": "trigger_cdc"},
        )

        telemetry.increment_ingested(len(insert_update_entries))
        telemetry.increment_failed(quarantine_count)
        telemetry_record = telemetry.finish_job()
        write_telemetry(telemetry.to_dict(telemetry_record), telemetry_path)

        op_counts = new_trigger_df["operation"].value_counts().to_dict()

        cdc_log = {
            "job_id": self.job.job_id,
            "strategy": "trigger_cdc",
            "source_id": self.source.source_id,
            "previous_offset": last_offset,
            "new_offset": new_offset,
            "events_read": total_new,
            "inserts": op_counts.get("INSERT", 0),
            "updates": op_counts.get("UPDATE", 0),
            "deletes": op_counts.get("DELETE", 0),
            "events_emitted": len(events),
            "committed": committed,
            "quarantined": quarantine_count,
            "run_at": now_utc_iso(),
        }
        write_json(cdc_log, cdc_log_path)

        summary = {
            "job_id": self.job.job_id,
            "strategy": "trigger_cdc",
            "source_id": self.source.source_id,
            "dataset_id": self.dataset.dataset_id,
            "previous_offset": last_offset,
            "new_offset": new_offset,
            "events_emitted": len(events),
            "quarantined": quarantine_count,
            "finished_at": now_utc_iso(),
        }
        write_job_summary(summary, summary_path)
        return summary