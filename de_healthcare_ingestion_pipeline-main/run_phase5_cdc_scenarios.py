"""
Run Phase 5 streaming scenarios for the vitals stream.

This version uses the synthetic vitals producer output from Phase 3
instead of directly reusing the original seed CSV.
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd

from config.constants import (
    SYNTHETIC_DIR,
    ACCEPTED_DIR,
    QUARANTINE_DIR,
    TELEMETRY_DIR,
    SNAPSHOTS_DIR,
    STEADY_RATE_PER_SEC,
    BURST_SIZE,
    BURST_WINDOW_SECONDS,
    BURST_PAUSE_SECONDS,
)
from src.models import (
    load_sources_config,
    load_datasets_config,
    load_jobs_config,
    load_contracts_config,
)
from src.stream_ingestion import StreamingIngestionPipeline
from src.scenario_runner import assign_cdc_operations


def ensure_synthetic_vitals_exists() -> Path:
    path = SYNTHETIC_DIR / "simulated_vitals_stream_synthetic.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Synthetic vitals file not found: {path}. Run Phase 3 first."
        )
    return path


def main() -> None:
    sources = load_sources_config()
    datasets = load_datasets_config()
    jobs = load_jobs_config()
    contracts = load_contracts_config()

    synthetic_input_path = ensure_synthetic_vitals_exists()

    # ensure Phase 5 source is truly generator-derived
    df = pd.read_csv(synthetic_input_path)
    df = assign_cdc_operations(df, seed=42)
    phase5_input_path = SYNTHETIC_DIR / "simulated_vitals_stream_phase5_input.csv"
    df.to_csv(phase5_input_path, index=False)

    steady_pipeline = StreamingIngestionPipeline(
        source=sources["simulated_vitals_stream"],
        dataset=datasets["vitals_stream_dataset"],
        job=jobs["job_stream_vitals_steady"],
        contract=contracts["vitals_stream_contract"],
    )

    steady_summary = steady_pipeline.run_steady_stream(
        input_path=phase5_input_path,
        accepted_path=ACCEPTED_DIR / "simulated_vitals_stream_steady_accepted.jsonl",
        quarantine_path=QUARANTINE_DIR / "simulated_vitals_stream_steady_quarantine.csv",
        telemetry_path=TELEMETRY_DIR / "job_stream_vitals_steady_telemetry.json",
        summary_path=SNAPSHOTS_DIR / "job_stream_vitals_steady_summary.json",
        rate_per_sec=STEADY_RATE_PER_SEC,
        duration_seconds=30,
    )

    print("Completed steady stream ingestion")
    print(steady_summary)
    print("-" * 80)

    burst_pipeline = StreamingIngestionPipeline(
        source=sources["simulated_vitals_stream"],
        dataset=datasets["vitals_stream_dataset"],
        job=jobs["job_stream_vitals_burst"],
        contract=contracts["vitals_stream_contract"],
    )

    burst_summary = burst_pipeline.run_burst_stream(
        input_path=phase5_input_path,
        accepted_path=ACCEPTED_DIR / "simulated_vitals_stream_burst_accepted.jsonl",
        quarantine_path=QUARANTINE_DIR / "simulated_vitals_stream_burst_quarantine.csv",
        telemetry_path=TELEMETRY_DIR / "job_stream_vitals_burst_telemetry.json",
        summary_path=SNAPSHOTS_DIR / "job_stream_vitals_burst_summary.json",
        burst_size=BURST_SIZE,
        burst_window_seconds=BURST_WINDOW_SECONDS,
        pause_seconds=BURST_PAUSE_SECONDS,
    )

    print("Completed burst stream ingestion")
    print(burst_summary)
    print("-" * 80)

    print("\nAll streaming scenarios completed successfully.")
    print("Total jobs run: 2")


if __name__ == "__main__":
    main()