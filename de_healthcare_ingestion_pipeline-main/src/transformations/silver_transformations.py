from pathlib import Path
import pandas as pd

from src.transformations.transformation_metrics import TransformationMetrics


class SilverTransformer:
    def __init__(self):
        self.silver_root = Path("data/iceberg_warehouse/silver")
        self.metrics_root = Path("data/transformation_metrics")

    def _read_bronze_table(self, table_id: str) -> pd.DataFrame:
        table_path = Path("data/iceberg_warehouse/bronze") / table_id / "data"
        files = list(table_path.rglob("*.parquet"))

        if not files:
            return pd.DataFrame()

        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    def _write_silver_table(self, table_id: str, df: pd.DataFrame):
        output_path = self.silver_root / table_id
        output_path.mkdir(parents=True, exist_ok=True)

        if "event_date" not in df.columns:
            df["event_date"] = pd.Timestamp.now().date().isoformat()

        for event_date, group in df.groupby("event_date"):
            partition_path = output_path / "data" / f"event_date={event_date}"
            partition_path.mkdir(parents=True, exist_ok=True)
            group.to_parquet(partition_path / f"{table_id}.parquet", index=False)

    def _basic_clean(self, df: pd.DataFrame, key_columns: list[str], timestamp_columns: list[str]):
        metrics = {}

        input_count = len(df)

        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).str.strip()

        null_pct = df.isna().mean().round(4).to_dict()

        before_drop = len(df)
        for key in key_columns:
            if key in df.columns:
                df = df[df[key].notna()]
                df = df[df[key].astype(str).str.lower() != "nan"]

        rejected = before_drop - len(df)

        before_dedup = len(df)
        existing_keys = [k for k in key_columns if k in df.columns]
        if existing_keys:
            df = df.drop_duplicates(subset=existing_keys, keep="last")

        duplicates = before_dedup - len(df)

        for col in timestamp_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                df["event_date"] = df[col].dt.date.astype(str)

        metrics["input_count"] = input_count
        metrics["output_count"] = len(df)
        metrics["rejected"] = rejected
        metrics["duplicates"] = duplicates
        metrics["null_pct"] = null_pct

        return df, metrics

    def transform_encounters(self):
        metrics = TransformationMetrics("bronze_to_silver_encounters")
        df = self._read_bronze_table("bronze_encounter_master")
        metrics.records_input = len(df)

        df, m = self._basic_clean(
            df,
            key_columns=["encounter_id"],
            timestamp_columns=["admit_datetime", "event_timestamp", "ingestion_timestamp"]
        )

        metrics.records_output = m["output_count"]
        metrics.records_rejected = m["rejected"]
        metrics.duplicate_records_detected = m["duplicates"]
        metrics.null_percentage_per_column = m["null_pct"]

        self._write_silver_table("silver_encounters", df)
        metrics.write(self.metrics_root / "silver_encounters_metrics.json")

    def run_all(self):
        self.transform_encounters()