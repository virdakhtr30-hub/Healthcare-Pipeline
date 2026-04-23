"""
Project-wide constants for paths, runtime defaults, and shared enums.
"""

from pathlib import Path

# Project root = parent of config/
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Main folders
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
SRC_DIR = PROJECT_ROOT / "src"
REPORT_DIR = PROJECT_ROOT / "report"

# Data subfolders
INPUT_DIR = DATA_DIR / "input"
ACCEPTED_DIR = DATA_DIR / "accepted"
QUARANTINE_DIR = DATA_DIR / "quarantine"
PROFILED_DIR = DATA_DIR / "profiled"
SYNTHETIC_DIR = DATA_DIR / "synthetic"
CDC_LOG_DIR = DATA_DIR / "cdc_logs"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
TELEMETRY_DIR = DATA_DIR / "telemetry"
PLOTS_DIR = DATA_DIR / "plots"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"

# Report subfolders
REPORT_FIGURES_DIR = REPORT_DIR / "figures"
REPORT_TABLES_DIR = REPORT_DIR / "tables"

# Config file paths
SOURCES_YAML = CONFIG_DIR / "sources.yaml"
DATASETS_YAML = CONFIG_DIR / "datasets.yaml"
JOBS_YAML = CONFIG_DIR / "jobs.yaml"
CONTRACTS_YAML = CONFIG_DIR / "contracts.yaml"

# Schema / ingestion defaults
DEFAULT_SCHEMA_VERSION = "v1"

# Supported values
ALLOWED_SOURCE_TYPES = {"db", "api", "stream", "file"}
ALLOWED_EXTRACTION_MODES = {"pull", "push", "query_based"}
ALLOWED_EXECUTION_MODES = {"BATCH", "MICRO_BATCH", "STREAMING", "CDC_CONTINUOUS"}
ALLOWED_VIOLATION_POLICIES = {"REJECT", "QUARANTINE", "AUTO_COERCE"}
ALLOWED_OPERATION_TYPES = {"INSERT", "UPDATE", "DELETE", "SNAPSHOT"}

# Scenario defaults
STEADY_RATE_PER_SEC = 10
BURST_SIZE = 5000
BURST_WINDOW_SECONDS = 1
BURST_PAUSE_SECONDS = 3

# Dataset/source names used across the project
SOURCE_NAMES = {
    "encounter_master",
    "diagnosis_events",
    "lab_results",
    "mortality_registry",
    "simulated_vitals_stream",
}

DATASET_NAMES = {
    "encounter_dataset",
    "diagnosis_dataset",
    "lab_results_dataset",
    "mortality_dataset",
    "vitals_stream_dataset",
}
