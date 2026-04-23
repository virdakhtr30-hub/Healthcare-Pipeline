from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

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

import numpy as _np


def simulate_wal_log(
    source_df: pd.DataFrame,
    primary_key: str,
    table_name: str = "simulated_table",
    watermark_col: str = "last_updated_at",
    seed: int = 99,
) -> pd.DataFrame:

    rng = _np.random.default_rng(seed)
    n = len(source_df)

    # Operations: INSERT 60%, UPDATE 30%, DELETE 10%
    choices = ["INSERT"] * 6 + ["UPDATE"] * 3 + ["DELETE"] * 1
    operations = [choices[rng.integers(0, len(choices))] for _ in range(n)]

    wal_rows = []
    for i, (_, row) in enumerate(source_df.iterrows()):
        operation = operations[i]
        record_id = str(row.get(primary_key, generate_uuid()))
        txn_id = f"txn_{generate_uuid()[:8]}"

        row_dict = row.to_dict()

        if operation == "INSERT":
            old_row = None
            new_row = row_dict
        elif operation == "UPDATE":
            # "Pehle" mein kuch fields alag the -- simulate karo
            old_row = dict(row_dict)
            old_row["_simulated_old"] = True  # Mark for clarity
            new_row = row_dict
        else:  # DELETE
            old_row = {primary_key: record_id}
            new_row = None

        wal_rows.append({
            "log_position": 5000 + i,   # WAL positions 5000 se shuru
            "txn_id": txn_id,
            "operation": operation,
            "table_name": table_name,
            "record_id": record_id,
            "committed_at": str(row.get(watermark_col, now_utc_iso())),
            "_old_row": old_row,        # In-memory only (not in real CSV)
            "_new_row": new_row,        # In-memory only
        })

    return pd.DataFrame(wal_rows)


class LogCDCPipeline(BaseIngestionPipeline):


    def load_source(self, input_path: str | Path) -> pd.DataFrame:
        return read_csv_source(input_path)

    def validate(self, df: pd.DataFrame) -> ContractValidationResult:
        validator = ContractValidator()
        return validator.validate_dataframe(
            df=df,
            contract=self.contract,
            source_name=self.source.source_id,
        )

    def wrap(self, wal_entries: list[dict], start_offset: int = 0) -> list[dict[str, Any]]:

        events = []
        for i, entry in enumerate(wal_entries):
            operation = str(entry.get("operation", "INSERT")).upper()
            old_row = entry.get("_old_row")
            new_row = entry.get("_new_row")

            event = build_cdc_event(
                operation=operation,
                after=new_row,
                before=old_row,
                source_id=self.source.source_id,
                dataset_id=self.dataset.dataset_id,
                offset=start_offset + i,
                schema_version=self.dataset.schema_version,
            )

            # Log-specific metadata bhi add karo
            event["log_position"] = entry.get("log_position")
            event["txn_id"] = entry.get("txn_id")
            event["table_name"] = entry.get("table_name")

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

        for p in [accepted_path, quarantine_path, checkpoint_path,
                  cdc_log_path, telemetry_path, summary_path]:
            safe_mkdir(Path(p).parent)

        # --- Checkpoint ---
        checkpoint = CheckpointManager(checkpoint_path)
        last_log_position = checkpoint.get_last_offset()  # 0 agar pehli baar

        # --- WAL simulation ---
        df = self.load_source(input_path)
        primary_key = self.source.primary_key[0] if self.source.primary_key else "id"
        watermark_col = self.source.watermark_column or "last_updated_at"

        wal_df = simulate_wal_log(
            source_df=df,
            primary_key=primary_key,
            table_name=self.source.source_id,
            watermark_col=watermark_col,
        )

        # --- New WAL entries ---
        new_wal = wal_df[wal_df["log_position"] > last_log_position].copy()
        total_new = len(new_wal)

        if total_new == 0:
            summary = {
                "job_id": self.job.job_id,
                "strategy": "log_cdc",
                "message": "Koi naya WAL entry nahi mila",
                "last_log_position": last_log_position,
                "finished_at": now_utc_iso(),
            }
            write_job_summary(summary, summary_path)
            return summary

        # --- Telemetry ---
        telemetry = TelemetryCollector()
        telemetry.start_job(
            job_id=self.job.job_id,
            source_id=self.source.source_id,
            dataset_id=self.dataset.dataset_id,
            execution_mode=self.job.execution_mode,
        )

        wal_entries = new_wal.to_dict(orient="records")


        insert_update_entries = [e for e in wal_entries if e.get("operation") in ("INSERT", "UPDATE")]
        raw_rows = [e.get("_new_row", {}) for e in insert_update_entries if e.get("_new_row")]
        quarantine_count = 0

        if raw_rows:
            raw_df = pd.DataFrame(raw_rows)
            validation = self.validate(raw_df)
            quarantine_count = len(validation.quarantined_df)
            if not validation.quarantined_df.empty:
                write_dataframe_csv(validation.quarantined_df, quarantine_path)

        # --- CDC events ---
        events = self.wrap(wal_entries, start_offset=last_log_position)

        # --- Offset management ---
        offset_mgr = OffsetManager(start_offset=last_log_position)
        if events:
            offset_mgr.commit(events[-1]["offset"])

        # --- Transactionally save ---
        sink = TransactionalSink(accepted_path)
        committed = self.write_outputs(events, sink)

        # --- Checkpoint update ---
        new_log_position = int(new_wal["log_position"].max())
        checkpoint.save(
            offset=new_log_position,
            watermark=now_utc_iso(),
            extra={"last_log_position": new_log_position, "last_txn_id": wal_entries[-1].get("txn_id")},
        )

        # --- Telemetry finish ---
        telemetry.increment_ingested(len(insert_update_entries))
        telemetry.increment_failed(quarantine_count)
        telemetry_record = telemetry.finish_job()
        write_telemetry(telemetry.to_dict(telemetry_record), telemetry_path)

        # Operation counts
        op_counts = new_wal["operation"].value_counts().to_dict()


        cdc_log = {
            "job_id": self.job.job_id,
            "strategy": "log_cdc",
            "source_id": self.source.source_id,
            "table_name": self.source.source_id,
            "previous_log_position": last_log_position,
            "new_log_position": new_log_position,
            "wal_entries_read": total_new,
            "inserts": op_counts.get("INSERT", 0),
            "updates": op_counts.get("UPDATE", 0),
            "deletes": op_counts.get("DELETE", 0),
            "events_emitted": len(events),
            "committed": committed,
            "quarantined": quarantine_count,
            "last_txn_id": wal_entries[-1].get("txn_id") if wal_entries else None,
            "run_at": now_utc_iso(),
        }
        write_json(cdc_log, cdc_log_path)

        summary = {
            "job_id": self.job.job_id,
            "strategy": "log_cdc",
            "source_id": self.source.source_id,
            "dataset_id": self.dataset.dataset_id,
            "previous_log_position": last_log_position,
            "new_log_position": new_log_position,
            "inserts": cdc_log["inserts"],
            "updates": cdc_log["updates"],
            "deletes": cdc_log["deletes"],
            "events_emitted": len(events),
            "quarantined": quarantine_count,
            "finished_at": now_utc_iso(),
        }
        write_job_summary(summary, summary_path)
        return summary
