import os
from pathlib import Path
import json
import pandas as pd


class StorageMetrics:

    def __init__(self, table_path: Path):
        self.table_path = table_path
        self.data_path = table_path / "data"
        self.snapshot_path = table_path / "metadata" / "snapshots"

    def file_metrics(self):
        file_sizes = []
        partition_counts = {}

        for root, dirs, files in os.walk(self.data_path):
            for file in files:
                if file.endswith(".parquet"):
                    full_path = Path(root) / file
                    size_mb = full_path.stat().st_size / (1024 * 1024)

                    file_sizes.append(size_mb)

                    partition = str(Path(root).relative_to(self.data_path))
                    partition_counts[partition] = partition_counts.get(partition, 0) + 1

        avg_file_size = sum(file_sizes) / len(file_sizes) if file_sizes else 0

        small_files = [f for f in file_sizes if f < 1]  # threshold 1MB
        small_file_ratio = len(small_files) / len(file_sizes) if file_sizes else 0

        return {
            "file_count_per_partition": partition_counts,
            "avg_file_size_mb": round(avg_file_size, 4),
            "small_file_ratio": round(small_file_ratio, 4)
        }

    def snapshot_metrics(self):
        snapshot_files = list(self.snapshot_path.glob("*.json"))
        return {
            "snapshot_count": len(snapshot_files)
        }

    def storage_metrics(self):
        total_size = 0

        for root, dirs, files in os.walk(self.table_path):
            for file in files:
                full_path = Path(root) / file
                total_size += full_path.stat().st_size

        total_size_mb = total_size / (1024 * 1024)

        return {
            "total_storage_size_mb": round(total_size_mb, 4)
        }

    def partition_health(self):
        partition_records = {}

        for root, dirs, files in os.walk(self.data_path):
            for file in files:
                if file.endswith(".parquet"):
                    partition = str(Path(root).relative_to(self.data_path))
                    partition_records[partition] = partition_records.get(partition, 0) + 1

        values = list(partition_records.values())

        skew = max(values) - min(values) if values else 0

        return {
            "records_per_partition": partition_records,
            "partition_skew": skew
        }

    def full_report(self):
        report = {}
        report.update(self.file_metrics())
        report.update(self.snapshot_metrics())
        report.update(self.storage_metrics())
        report.update(self.partition_health())
        return report