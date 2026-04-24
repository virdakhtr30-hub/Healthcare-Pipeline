from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid


@dataclass
class CompactionPolicy:
    small_file_threshold_mb: float = 1.0
    target_file_size_mb: float = 16.0
    frequency: str = "every_pipeline_run"


@dataclass
class SnapshotRetentionPolicy:
    max_snapshots: int = 10
    retention_days: int = 7


@dataclass
class IcebergTable:
    """
    Course-project Iceberg table entity.

    This models the important Iceberg governance concepts required in Part 2:
    table identity, layer, partitioning, file format, compaction policy,
    snapshot retention, and metadata tracking.
    """

    table_id: str
    dataset_id: Optional[str]
    table_layer: str
    partition_spec: List[str]
    file_format: str = "parquet"
    source_id: Optional[str] = None
    source_tables: List[str] = field(default_factory=list)
    primary_key: List[str] = field(default_factory=list)
    description: str = ""
    compaction_policy: CompactionPolicy = field(default_factory=CompactionPolicy)
    snapshot_retention: SnapshotRetentionPolicy = field(default_factory=SnapshotRetentionPolicy)

    def table_path(self, warehouse_root: str | Path) -> Path:
        return Path(warehouse_root) / self.table_layer.lower() / self.table_id

    def data_path(self, warehouse_root: str | Path) -> Path:
        return self.table_path(warehouse_root) / "data"

    def metadata_path(self, warehouse_root: str | Path) -> Path:
        return self.table_path(warehouse_root) / "metadata"

    def snapshots_path(self, warehouse_root: str | Path) -> Path:
        return self.metadata_path(warehouse_root) / "snapshots"


@dataclass
class IcebergSnapshot:
    snapshot_id: str
    table_id: str
    table_layer: str
    created_at: str
    operation: str
    record_count: int
    data_files: List[str]
    partition_summary: Dict[str, Any]
    schema_version: str = "v1"
    parent_snapshot_id: Optional[str] = None

    @staticmethod
    def new(
        table: IcebergTable,
        operation: str,
        record_count: int,
        data_files: List[str],
        partition_summary: Dict[str, Any],
        parent_snapshot_id: Optional[str] = None,
        schema_version: str = "v1",
    ) -> "IcebergSnapshot":
        return IcebergSnapshot(
            snapshot_id=str(uuid.uuid4()),
            table_id=table.table_id,
            table_layer=table.table_layer,
            created_at=datetime.now(timezone.utc).isoformat(),
            operation=operation,
            record_count=int(record_count),
            data_files=data_files,
            partition_summary=partition_summary,
            schema_version=schema_version,
            parent_snapshot_id=parent_snapshot_id,
        )