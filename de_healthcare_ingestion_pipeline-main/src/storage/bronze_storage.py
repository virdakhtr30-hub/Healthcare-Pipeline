import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
from typing import List

from src.storage.iceberg_models import IcebergTable, IcebergSnapshot


WAREHOUSE_ROOT = Path("data/iceberg_warehouse")


class BronzeStorageManager:

    def __init__(self):
        self.warehouse_root = WAREHOUSE_ROOT

    def _ensure_dirs(self, table: IcebergTable):
        table_path = table.table_path(self.warehouse_root)
        data_path = table.data_path(self.warehouse_root)
        metadata_path = table.metadata_path(self.warehouse_root)
        snapshots_path = table.snapshots_path(self.warehouse_root)

        data_path.mkdir(parents=True, exist_ok=True)
        metadata_path.mkdir(parents=True, exist_ok=True)
        snapshots_path.mkdir(parents=True, exist_ok=True)

    def _get_partition_path(self, table: IcebergTable, row: dict):
        partition_values = []

        for col in table.partition_spec:
            if col in row:
                partition_values.append(f"{col}={row[col]}")
            else:
                partition_values.append(f"{col}=unknown")

        return Path("/".join(partition_values))

    def write_batch(self, table: IcebergTable, df: pd.DataFrame):
        self._ensure_dirs(table)

        data_files = []
        partition_summary = {}

        for _, row in df.iterrows():
            row_dict = row.to_dict()

            partition_path = self._get_partition_path(table, row_dict)

            full_partition_path = table.data_path(self.warehouse_root) / partition_path
            full_partition_path.mkdir(parents=True, exist_ok=True)

            file_name = f"{datetime.utcnow().timestamp()}_{os.getpid()}.parquet"
            file_path = full_partition_path / file_name

            pd.DataFrame([row_dict]).to_parquet(file_path, index=False)

            data_files.append(str(file_path))

            partition_key = str(partition_path)
            partition_summary[partition_key] = partition_summary.get(partition_key, 0) + 1

        snapshot = IcebergSnapshot.new(
            table=table,
            operation="APPEND",
            record_count=len(df),
            data_files=data_files,
            partition_summary=partition_summary
        )

        snapshot_file = table.snapshots_path(self.warehouse_root) / f"{snapshot.snapshot_id}.json"

        with open(snapshot_file, "w") as f:
            json.dump(snapshot.__dict__, f, indent=2)

        return snapshot

    def read_source_csv(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path)