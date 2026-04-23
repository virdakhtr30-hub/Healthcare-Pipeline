"""
Batch ingestion implementation for CSV-based healthcare sources.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.ingestion_base import BaseIngestionPipeline
from src.io_utils import read_csv_source, write_jsonl, write_dataframe_csv
from src.contracts import ContractValidator, ContractValidationResult
from src.envelope import build_envelopes_from_df
from src.telemetry import TelemetryCollector, write_telemetry, write_job_summary
from src.utils import safe_mkdir, now_utc_iso


class BatchIngestionPipeline(BaseIngestionPipeline):
    """
    Implements initial full-snapshot ingestion for batch CSV sources.

    Main flow:
    1. Read source CSV
    2. Validate against contract
    3. Wrap accepted rows in event envelopes
    4. Write accepted/quarantine/error outputs
    5. Emit telemetry and summary
    """

    def load_source(self, input_path: str | Path) -> pd.DataFrame:
        """
        Load the input CSV source into a DataFrame.
        """
        return read_csv_source(input_path)

    def validate(self, df: pd.DataFrame) -> ContractValidationResult:
        """
        Validate the source DataFrame against the configured contract.
        """
        validator = ContractValidator()
        return validator.validate_dataframe(
            df=df,
            contract=self.contract,
            source_name=self.source.source_id,
        )

    def wrap(self, accepted_df: pd.DataFrame) -> list[dict[str, Any]]:
        """
        Wrap accepted records in SNAPSHOT event envelopes.
        """
        return build_envelopes_from_df(
            df=accepted_df,
            source_id=self.source.source_id,
            dataset_id=self.dataset.dataset_id,
            operation_type="SNAPSHOT",
            schema_version=self.dataset.schema_version,
        )

    def write_outputs(
        self,
        enveloped_records: list[dict[str, Any]],
        quarantined_df: pd.DataFrame,
        error_report: pd.DataFrame,
        accepted_path: str | Path,
        quarantine_path: str | Path,
        error_report_path: str | Path,
    ) -> None:
        """
        Write accepted JSONL, quarantine CSV, and error report CSV.
        """
        write_jsonl(enveloped_records, accepted_path)

        if not quarantined_df.empty:
            write_dataframe_csv(quarantined_df, quarantine_path)

        if not error_report.empty:
            write_dataframe_csv(error_report, error_report_path)

    def run(
        self,
        input_path: str | Path,
        accepted_path: str | Path,
        quarantine_path: str | Path,
        error_report_path: str | Path,
        telemetry_path: str | Path,
        summary_path: str | Path,
    ) -> dict[str, Any]:
        """
        Execute the end-to-end batch ingestion flow.

        Args:
            input_path: Source CSV path.
            accepted_path: Output JSONL path for accepted enveloped records.
            quarantine_path: Output CSV path for quarantined records.
            error_report_path: Output CSV path for validation errors.
            telemetry_path: Output JSON path for telemetry.
            summary_path: Output JSON path for run summary.

        Returns:
            Summary dictionary for this ingestion job.
        """
        # Ensure output directories exist
        for parent in [
            Path(accepted_path).parent,
            Path(quarantine_path).parent,
            Path(error_report_path).parent,
            Path(telemetry_path).parent,
            Path(summary_path).parent,
        ]:
            safe_mkdir(parent)

        # Start telemetry
        telemetry = TelemetryCollector()
        telemetry.start_job(
            job_id=self.job.job_id,
            source_id=self.source.source_id,
            dataset_id=self.dataset.dataset_id,
            execution_mode=self.job.execution_mode,
        )

        # Load and validate
        df = self.load_source(input_path)
        validation = self.validate(df)

        accepted_df = validation.accepted_df
        quarantined_df = validation.quarantined_df
        rejected_df = validation.rejected_df
        error_report = validation.error_report

        # Wrap accepted rows
        enveloped_records = self.wrap(accepted_df)

        # Write outputs
        self.write_outputs(
            enveloped_records=enveloped_records,
            quarantined_df=quarantined_df,
            error_report=error_report,
            accepted_path=accepted_path,
            quarantine_path=quarantine_path,
            error_report_path=error_report_path,
        )

        # Telemetry counts
        telemetry.increment_ingested(len(accepted_df))
        telemetry.increment_failed(len(quarantined_df) + len(rejected_df))

        # Timestamp telemetry
        watermark_col = self.source.watermark_column
        if watermark_col and watermark_col in accepted_df.columns:
            for source_ts in accepted_df[watermark_col]:
                telemetry.record_source_and_ingestion_times(source_ts, now_utc_iso())

        telemetry_record = telemetry.finish_job()
        telemetry_dict = telemetry.to_dict(telemetry_record)
        write_telemetry(telemetry_dict, telemetry_path)

        # Summary
        summary = {
            "job_id": self.job.job_id,
            "source_id": self.source.source_id,
            "dataset_id": self.dataset.dataset_id,
            "execution_mode": self.job.execution_mode,
            "source_type": self.source.source_type,
            "change_capture_mode": self.source.change_capture_mode,
            "input_path": str(input_path),
            "accepted_output_path": str(accepted_path),
            "quarantine_output_path": str(quarantine_path),
            "error_report_path": str(error_report_path),
            "records_total_input": int(len(df)),
            "records_ingested": int(len(accepted_df)),
            "records_quarantined": int(len(quarantined_df)),
            "records_rejected": int(len(rejected_df)),
            "finished_at": now_utc_iso(),
        }
        write_job_summary(summary, summary_path)

        return summary


def run_initial_load_for_source(
    pipeline: BatchIngestionPipeline,
    input_path: str | Path,
    accepted_path: str | Path,
    quarantine_path: str | Path,
    error_report_path: str | Path,
    telemetry_path: str | Path,
    summary_path: str | Path,
) -> dict[str, Any]:
    """
    Convenience wrapper to run initial batch load for one source.
    """
    return pipeline.run(
        input_path=input_path,
        accepted_path=accepted_path,
        quarantine_path=quarantine_path,
        error_report_path=error_report_path,
        telemetry_path=telemetry_path,
        summary_path=summary_path,
    )
