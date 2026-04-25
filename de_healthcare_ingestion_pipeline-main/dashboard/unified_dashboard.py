from pathlib import Path
import json
import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Healthcare Part 2 Unified Dashboard",
    layout="wide",
)

st.title("Healthcare Data Engineering Part 2 Dashboard")
st.caption("Ingestion, Storage, and Transformation KPIs")


def load_json_files(folder: str):
    path = Path(folder)
    rows = []

    if not path.exists():
        return pd.DataFrame()

    for file in path.rglob("*.json"):
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
            data["source_file"] = str(file)
            rows.append(data)
        except Exception:
            pass

    return pd.DataFrame(rows)


st.header("1. Transformation KPIs")
transformation_df = load_json_files("data/transformation_metrics")

if transformation_df.empty:
    st.warning("No transformation metrics found.")
else:
    st.dataframe(transformation_df, use_container_width=True)

    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric(
            "Total Records Cleaned",
            int(transformation_df.get("records_cleaned", pd.Series([0])).fillna(0).sum())
        )

    with c2:
        st.metric(
            "Total Records Rejected",
            int(transformation_df.get("records_rejected", pd.Series([0])).fillna(0).sum())
        )

    with c3:
        st.metric(
            "Avg Transformation Latency",
            round(transformation_df.get("transformation_latency_seconds", pd.Series([0])).fillna(0).mean(), 4)
        )


st.header("2. Storage Layer View")

warehouse = Path("data/iceberg_warehouse")
table_rows = []

if warehouse.exists():
    for layer in ["bronze", "silver", "gold"]:
        layer_path = warehouse / layer
        if layer_path.exists():
            for table_path in layer_path.iterdir():
                if table_path.is_dir():
                    parquet_files = list(table_path.rglob("*.parquet"))
                    json_files = list(table_path.rglob("*.json"))
                    size_mb = sum(f.stat().st_size for f in parquet_files + json_files) / (1024 * 1024)

                    table_rows.append({
                        "layer": layer.upper(),
                        "table_name": table_path.name,
                        "parquet_file_count": len(parquet_files),
                        "metadata_file_count": len(json_files),
                        "total_size_mb": round(size_mb, 4),
                    })

storage_df = pd.DataFrame(table_rows)

if storage_df.empty:
    st.warning("No storage tables found.")
else:
    st.dataframe(storage_df, use_container_width=True)

    c1, c2, c3 = st.columns(3)

    with c1:
        st.metric("Tables", len(storage_df))

    with c2:
        st.metric("Parquet Files", int(storage_df["parquet_file_count"].sum()))

    with c3:
        st.metric("Total Size MB", round(storage_df["total_size_mb"].sum(), 4))


st.header("3. Bronze Snapshot Metadata")

snapshot_rows = []

for snapshot_file in Path("data/iceberg_warehouse").rglob("metadata/snapshots/*.json"):
    try:
        with open(snapshot_file, "r", encoding="utf-8") as f:
            snapshot = json.load(f)
        snapshot_rows.append({
            "table_id": snapshot.get("table_id"),
            "layer": snapshot.get("table_layer"),
            "snapshot_id": snapshot.get("snapshot_id"),
            "operation": snapshot.get("operation"),
            "record_count": snapshot.get("record_count"),
            "created_at": snapshot.get("created_at"),
        })
    except Exception:
        pass

snapshot_df = pd.DataFrame(snapshot_rows)

if snapshot_df.empty:
    st.warning("No snapshot metadata found.")
else:
    st.dataframe(snapshot_df, use_container_width=True)
    st.metric("Snapshot Count", len(snapshot_df))


st.header("4. Gold Business Outputs")

gold_root = Path("data/iceberg_warehouse/gold")

if gold_root.exists():
    for table_path in gold_root.iterdir():
        if table_path.is_dir():
            st.subheader(table_path.name)
            files = list(table_path.rglob("*.parquet"))

            if files:
                df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
                st.dataframe(df.head(50), use_container_width=True)
            else:
                st.info("No Parquet files found.")
else:
    st.warning("Gold layer not found.")


st.header("5. Engineering Insights")

st.markdown(
    """
    - Bronze stores raw governed data using partitioned Parquet files and snapshot metadata.
    - Silver improves quality through type casting, null handling, deduplication, and timestamp standardization.
    - Gold provides business-ready outputs for patient risk and operational monitoring.
    - Storage KPIs help detect small files, partition imbalance, and snapshot growth.
    - Transformation KPIs show whether data quality improves from Bronze to Silver to Gold.
    """
)