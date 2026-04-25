import json
import shutil
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

from src.storage.iceberg_models import IcebergTable, IcebergSnapshot


WAREHOUSE_ROOT = Path("data/iceberg_warehouse")


class BronzeStorageManager:
    def __init__(self):
        self.warehouse_root = WAREHOUSE_ROOT

    def _ensure_dirs(self, table: IcebergTable):
        table.data_path(self.warehouse_root).mkdir(parents=True, exist_ok=True)
        table.metadata_path(self.warehouse_root).mkdir(parents=True, exist_ok=True)
        table.snapshots_path(self.warehouse_root).mkdir(parents=True, exist_ok=True)

    def _clean_table_before_write(self, table: IcebergTable):
        table_path = table.table_path(self.warehouse_root)
        if table_path.exists():
            shutil.rmtree(table_path)

    def _safe_partition_value(self, value) -> str:
        if pd.isna(value):
            return "unknown"

        safe_value = str(value)
        safe_value = safe_value.replace(":", "-")
        safe_value = safe_value.replace("/", "-")
        safe_value = safe_value.replace("\\", "-")
        safe_value = safe_value.replace(" ", "_")
        return safe_value

    def write_batch(self, table: IcebergTable, df: pd.DataFrame):
        """
        Safer Bronze writer:
        - clears previous generated table output before writing
        - writes one valid Parquet file per partition
        - avoids hidden .tmp.parquet files because pyarrow may try to read them
        - creates Iceberg-style snapshot metadata
        """

        self._clean_table_before_write(table)
        self._ensure_dirs(table)

        df = df.copy()

        data_files = []
        partition_summary = {}

        partition_cols = [col for col in table.partition_spec if col in df.columns]

        if not partition_cols:
            df["_partition_unknown"] = "unknown"
            partition_cols = ["_partition_unknown"]

        grouped = df.groupby(partition_cols, dropna=False)

        for partition_values, group in grouped:
            if not isinstance(partition_values, tuple):
                partition_values = (partition_values,)

            partition_parts = []
            for col, value in zip(partition_cols, partition_values):
                partition_parts.append(f"{col}={self._safe_partition_value(value)}")

            partition_path = Path(*partition_parts)
            full_partition_path = table.data_path(self.warehouse_root) / partition_path
            full_partition_path.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
            final_file = full_partition_path / f"{table.table_id}_{timestamp}.parquet"

            group.to_parquet(final_file, index=False)

            data_files.append(str(final_file))
            partition_summary[str(partition_path)] = int(len(group))

        snapshot = IcebergSnapshot.new(
            table=table,
            operation="APPEND",
            record_count=len(df),
            data_files=data_files,
            partition_summary=partition_summary,
        )

        snapshot_file = table.snapshots_path(self.warehouse_root) / f"{snapshot.snapshot_id}.json"

        with open(snapshot_file, "w", encoding="utf-8") as f:
            json.dump(snapshot.__dict__, f, indent=2)

        return snapshot

    def read_source_csv(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path)