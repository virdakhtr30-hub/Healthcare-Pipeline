from pathlib import Path
import json

import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Healthcare Workload Monitoring Dashboard",
    layout="wide",
)

st.title("Healthcare Workload Monitoring Dashboard")
st.caption("Query performance, workload category, scan estimates, and bottleneck monitoring")


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKLOAD_DIR = PROJECT_ROOT / "workload_monitoring"

QUERY_METRICS = WORKLOAD_DIR / "query_execution_metrics.json"
SUMMARY_METRICS = WORKLOAD_DIR / "workload_summary_metrics.json"


def load_json(path: Path):
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


query_metrics = load_json(QUERY_METRICS)
summary_metrics = load_json(SUMMARY_METRICS)

if query_metrics is None:
    st.error("query_execution_metrics.json not found. Run: python run_part3_sql_workloads.py")
    st.stop()

if summary_metrics is None:
    st.error("workload_summary_metrics.json not found. Run: python workload_monitoring/workload_metrics.py")
    st.stop()

query_df = pd.DataFrame(query_metrics)

st.header("1. Workload KPI Summary")

perf = summary_metrics.get("query_performance_kpis", {})
sql_kpis = summary_metrics.get("sql_workload_kpis", {})

c1, c2, c3, c4 = st.columns(4)

c1.metric("Avg Query Latency (s)", perf.get("avg_query_latency_seconds", 0))
c2.metric("P95 Query Latency (s)", perf.get("p95_query_latency_seconds", 0))
c3.metric("Failure Rate", perf.get("query_failure_rate", 0))
c4.metric("Avg Bytes Scanned (MB)", sql_kpis.get("avg_bytes_scanned_per_query_mb", 0))

c5, c6, c7 = st.columns(3)
c5.metric("Concurrent Queries", perf.get("concurrent_queries", 0))
c6.metric("Queue Time (s)", perf.get("query_queue_time_seconds", 0))
c7.metric("Snapshot Scan Count", sql_kpis.get("snapshot_scan_count", 0))

st.header("2. Query Execution Metrics")

if query_df.empty:
    st.warning("No query metrics available.")
else:
    st.dataframe(query_df, use_container_width=True)

    st.subheader("Query Latency by Query")

    latency_df = query_df[["query_id", "latency_seconds"]].copy()
    latency_df = latency_df.sort_values("latency_seconds", ascending=False)

    st.bar_chart(
        latency_df.set_index("query_id")["latency_seconds"]
    )

    st.subheader("Queries by Workload Category")

    if "category" in query_df.columns:
        category_counts = query_df.groupby("category").size().reset_index(name="query_count")
        st.bar_chart(category_counts.set_index("category")["query_count"])

    st.subheader("Bytes Scanned Estimate by Query")

    if "bytes_scanned_estimate_mb" in query_df.columns:
        bytes_df = query_df[["query_id", "bytes_scanned_estimate_mb"]].copy()
        bytes_df = bytes_df.sort_values("bytes_scanned_estimate_mb", ascending=False)
        st.bar_chart(bytes_df.set_index("query_id")["bytes_scanned_estimate_mb"])

st.header("3. Category-Level Summary")

category_summary = summary_metrics.get("category_summary", {})
if category_summary:
    category_df = pd.DataFrame(category_summary).T.reset_index()
    category_df = category_df.rename(columns={"index": "category"})
    st.dataframe(category_df, use_container_width=True)
else:
    st.warning("No category summary available.")

st.header("4. Top Expensive Queries")

top_expensive = sql_kpis.get("top_expensive_queries", [])
if top_expensive:
    top_df = pd.DataFrame(top_expensive)
    st.dataframe(top_df, use_container_width=True)
else:
    st.info("No expensive query list available.")