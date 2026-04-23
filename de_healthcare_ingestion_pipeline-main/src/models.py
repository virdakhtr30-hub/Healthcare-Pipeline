"""
Core project models and YAML config loaders.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from config.constants import (
    SOURCES_YAML,
    DATASETS_YAML,
    JOBS_YAML,
    CONTRACTS_YAML,
)


@dataclass
class DataSource:
    """
    Represents a source system or source file.

    Attributes:
        source_id: Unique identifier of the source.
        source_type: One of db, api, stream, file.
        extraction_mode: pull, push, or query_based.
        change_capture_mode: FULL_SNAPSHOT, INCREMENTAL, CDC_LOG_BASED,
                             CDC_TRIGGER_BASED, STREAM_EVENT, etc.
        expected_schema: Optional source schema metadata.
        ingestion_frequency: daily, hourly, real-time, etc.
        primary_key: List of fields that uniquely identify a row.
        watermark_column: Timestamp field used for incremental/timestamp CDC.
    """
    source_id: str
    source_type: str
    extraction_mode: str
    change_capture_mode: str
    expected_schema: dict[str, Any]
    ingestion_frequency: str
    primary_key: list[str]
    watermark_column: str | None = None


@dataclass
class Dataset:
    """
    Represents a logical dataset produced by ingestion.

    Attributes:
        dataset_id: Unique identifier of the logical dataset.
        domain: Business domain, e.g. healthcare.
        classification_level: public/internal/confidential/restricted.
        schema_version: Version of the dataset schema.
        retention_policy: How long the dataset should be retained.
        description: Short explanation of the dataset.
    """
    dataset_id: str
    domain: str
    classification_level: str
    schema_version: str
    retention_policy: str
    description: str | None = None


@dataclass
class IngestionJob:
    """
    Represents a concrete ingestion job definition.

    Attributes:
        job_id: Unique identifier of the job.
        dataset_id: Logical dataset produced by the job.
        source_id: Source consumed by the job.
        execution_mode: BATCH, MICRO_BATCH, STREAMING, CDC_CONTINUOUS.
        schedule: Human-readable schedule or scenario label.
        owner: Optional owner/person responsible for the job.
    """
    job_id: str
    dataset_id: str
    source_id: str
    execution_mode: str
    schedule: str | None = None
    owner: str | None = None


@dataclass
class DataContract:
    """
    Represents a data contract for validation.

    Attributes:
        contract_id: Unique contract identifier.
        required_fields: Fields that must exist and be non-null.
        nullable_fields: Fields that are allowed to be null.
        type_constraints: Logical type expectations per field.
        unit_constraints: Allowed units for selected lab test fields.
        enumerations: Allowed categorical values for selected fields.
        violation_policy: REJECT, QUARANTINE, or AUTO_COERCE.
    """
    contract_id: str
    required_fields: list[str]
    nullable_fields: list[str]
    type_constraints: dict[str, str]
    unit_constraints: dict[str, list[str]]
    enumerations: dict[str, list[Any]]
    violation_policy: str


@dataclass
class EventEnvelope:
    """
    Represents a metadata wrapper around a single ingested record.
    """
    event_id: str
    event_timestamp: str
    source_timestamp: str | None
    schema_version: str
    ingestion_timestamp: str
    operation_type: str
    trace_id: str
    source_id: str
    dataset_id: str
    payload: dict[str, Any]


@dataclass
class TelemetryRecord:
    """
    Represents telemetry emitted by an ingestion job.
    """
    job_id: str
    source_id: str
    dataset_id: str
    execution_mode: str
    records_ingested: int
    records_failed: int
    ingestion_latency_seconds: float
    processing_lag_seconds: float
    throughput_rps: float
    file_count_per_partition: int | None = None
    snapshot_count: int | None = None
    compaction_lag_seconds: float | None = None
    started_at: str | None = None
    ended_at: str | None = None


def _read_yaml(path: str | Path) -> dict:
    """
    Read a YAML file and return its contents as a Python dictionary.

    Args:
        path: Path to YAML file.

    Returns:
        Parsed YAML content.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the YAML is empty or not a dictionary.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"YAML config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if data is None:
        raise ValueError(f"YAML config file is empty: {path}")

    if not isinstance(data, dict):
        raise ValueError(f"YAML config must contain a top-level dictionary: {path}")

    return data


def load_sources_config(path: str | Path = SOURCES_YAML) -> dict[str, DataSource]:
    """
    Load source definitions from sources.yaml.

    Returns:
        Dictionary mapping source key to DataSource object.
    """
    raw = _read_yaml(path)
    return {key: DataSource(**value) for key, value in raw.items()}


def load_datasets_config(path: str | Path = DATASETS_YAML) -> dict[str, Dataset]:
    """
    Load dataset definitions from datasets.yaml.

    Returns:
        Dictionary mapping dataset key to Dataset object.
    """
    raw = _read_yaml(path)
    return {key: Dataset(**value) for key, value in raw.items()}


def load_jobs_config(path: str | Path = JOBS_YAML) -> dict[str, IngestionJob]:
    """
    Load job definitions from jobs.yaml.

    Returns:
        Dictionary mapping job key to IngestionJob object.
    """
    raw = _read_yaml(path)
    return {key: IngestionJob(**value) for key, value in raw.items()}


def load_contracts_config(path: str | Path = CONTRACTS_YAML) -> dict[str, DataContract]:
    """
    Load contract definitions from contracts.yaml.

    Returns:
        Dictionary mapping contract key to DataContract object.
    """
    raw = _read_yaml(path)
    return {key: DataContract(**value) for key, value in raw.items()}


def smoke_test_config_loading() -> None:
    """
    Simple local smoke test to confirm that all YAML configs load correctly.
    Prints loaded keys for quick verification.
    """
    sources = load_sources_config()
    datasets = load_datasets_config()
    jobs = load_jobs_config()
    contracts = load_contracts_config()

    print("Loaded sources:", list(sources.keys()))
    print("Loaded datasets:", list(datasets.keys()))
    print("Loaded jobs:", list(jobs.keys()))
    print("Loaded contracts:", list(contracts.keys()))


if __name__ == "__main__":
    smoke_test_config_loading()
