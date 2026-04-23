from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOTS_DIR = PROJECT_ROOT / "data" / "snapshots"

SUMMARY_JSON = SNAPSHOTS_DIR / "telemetry_dashboard_summary.json"
DETAIL_CSV = SNAPSHOTS_DIR / "telemetry_dashboard_table.csv"


st.set_page_config(
    page_title="Healthcare Ingestion Telemetry Dashboard",
    layout="wide",
)

st.title("Healthcare Ingestion Telemetry Dashboard")
st.caption("Observability view for Phase 4, Phase 5, and Phase 6 pipeline runs.")


def load_summary(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


summary = load_summary(SUMMARY_JSON)
table_df = load_table(DETAIL_CSV)

if not summary:
    st.error("Summary file not found. Run: python run_phase7_telemetry_summary.py")
    st.stop()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Job Count", summary.get("job_count", 0))
col2.metric("Total Records Ingested", summary.get("total_records_ingested", 0))
col3.metric("Total Records Failed", summary.get("total_records_failed", 0))
col4.metric("Avg Throughput (rps)", round(summary.get("avg_throughput_rps", 0.0), 2))

col5, col6, col7 = st.columns(3)
col5.metric("Avg Ingestion Latency (s)", round(summary.get("avg_ingestion_latency_seconds", 0.0), 2))
col6.metric("Avg Processing Lag (s)", round(summary.get("avg_processing_lag_seconds", 0.0), 2))

max_latency_job = summary.get("max_ingestion_latency_job") or {}
max_failure_job = summary.get("max_failure_job") or {}

col7.metric(
    "Max Failure Job",
    max_failure_job.get("job_id", "N/A"),
    delta=max_failure_job.get("records_failed", 0),
)

st.subheader("Run-Level Summary")
st.json(summary)

st.subheader("Telemetry Table")
if table_df.empty:
    st.warning("Telemetry detail table not found or empty.")
else:
    show_cols = [
        c for c in [
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
        ]
        if c in table_df.columns
    ]
    st.dataframe(table_df[show_cols], width="stretch")

    st.subheader("Records Ingested by Job")
    ingested_chart = table_df[["job_id", "records_ingested"]].copy()
    ingested_chart = ingested_chart.set_index("job_id")
    st.bar_chart(ingested_chart)

    st.subheader("Records Failed by Job")
    failed_chart = table_df[["job_id", "records_failed"]].copy()
    failed_chart = failed_chart.set_index("job_id")
    st.bar_chart(failed_chart)

    st.subheader("Throughput by Job")
    throughput_chart = table_df[["job_id", "throughput_rps"]].copy()
    throughput_chart = throughput_chart.set_index("job_id")
    st.bar_chart(throughput_chart)