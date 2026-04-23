"""
Run Phase 7: Consolidate telemetry outputs into dashboard-ready summary artifacts.

This script:
1. Reads all job-level telemetry JSON files from data/telemetry/
2. Builds a consolidated telemetry table
3. Computes high-level summary metrics
4. Writes dashboard-ready CSV and JSON outputs to data/snapshots/

This is the observability-plane aggregation layer for the ingestion project.
"""

from __future__ import annotations

from pathlib import Path
import json
from typing import Any

import pandas as pd

from config.constants import TELEMETRY_DIR, SNAPSHOTS_DIR
from src.utils import now_utc_iso, safe_mkdir


def load_telemetry_files(telemetry_dir: Path) -> list[dict[str, Any]]:
    """
    Read all telemetry JSON files from the telemetry directory.
    """
    records: list[dict[str, Any]] = []

    if not telemetry_dir.exists():
        return records

    for path in sorted(telemetry_dir.glob("*.json")):
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
            payload["telemetry_file"] = str(path)
            records.append(payload)

    return records


def build_summary(df: pd.DataFrame) -> dict[str, Any]:
    """
    Compute high-level telemetry summary metrics for dashboard display.
    """
    if df.empty:
        return {
            "generated_at": now_utc_iso(),
            "job_count": 0,
            "total_records_ingested": 0,
            "total_records_failed": 0,
            "avg_ingestion_latency_seconds": 0.0,
            "avg_processing_lag_seconds": 0.0,
            "avg_throughput_rps": 0.0,
            "max_ingestion_latency_job": None,
            "max_failure_job": None,
        }

    numeric_cols = [
        "records_ingested",
        "records_failed",
        "ingestion_latency_seconds",
        "processing_lag_seconds",
        "throughput_rps",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    max_latency_job = None
    if "ingestion_latency_seconds" in df.columns and df["ingestion_latency_seconds"].notna().any():
        max_latency_row = df.loc[df["ingestion_latency_seconds"].idxmax()]
        max_latency_job = {
            "job_id": max_latency_row.get("job_id"),
            "ingestion_latency_seconds": float(max_latency_row.get("ingestion_latency_seconds", 0.0)),
        }

    max_failure_job = None
    if "records_failed" in df.columns and df["records_failed"].notna().any():
        max_failure_row = df.loc[df["records_failed"].idxmax()]
        max_failure_job = {
            "job_id": max_failure_row.get("job_id"),
            "records_failed": int(max_failure_row.get("records_failed", 0)),
        }

    return {
        "generated_at": now_utc_iso(),
        "job_count": int(len(df)),
        "total_records_ingested": int(df["records_ingested"].fillna(0).sum()),
        "total_records_failed": int(df["records_failed"].fillna(0).sum()),
        "avg_ingestion_latency_seconds": float(df["ingestion_latency_seconds"].fillna(0).mean()),
        "avg_processing_lag_seconds": float(df["processing_lag_seconds"].fillna(0).mean()),
        "avg_throughput_rps": float(df["throughput_rps"].fillna(0).mean()),
        "max_ingestion_latency_job": max_latency_job,
        "max_failure_job": max_failure_job,
    }


def main() -> None:
    """
    Execute Phase 7 telemetry summary generation.
    """
    safe_mkdir(SNAPSHOTS_DIR)

    telemetry_records = load_telemetry_files(TELEMETRY_DIR)
    telemetry_df = pd.DataFrame(telemetry_records)

    detail_csv_path = SNAPSHOTS_DIR / "telemetry_dashboard_table.csv"
    detail_json_path = SNAPSHOTS_DIR / "telemetry_dashboard_table.json"
    summary_json_path = SNAPSHOTS_DIR / "telemetry_dashboard_summary.json"

    if telemetry_df.empty:
        telemetry_df = pd.DataFrame(
            columns=[
                "job_id",
                "source_id",
                "dataset_id",
                "execution_mode",
                "records_ingested",
                "records_failed",
                "ingestion_latency_seconds",
                "processing_lag_seconds",
                "throughput_rps",
                "started_at",
                "ended_at",
                "telemetry_file",
            ]
        )

    telemetry_df.to_csv(detail_csv_path, index=False)
    telemetry_df.to_json(detail_json_path, orient="records", indent=2)

    summary = build_summary(telemetry_df)
    with open(summary_json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Telemetry summary artifacts written successfully.")
    print(f"Detail CSV: {detail_csv_path}")
    print(f"Detail JSON: {detail_json_path}")
    print(f"Summary JSON: {summary_json_path}")


if __name__ == "__main__":
    main()