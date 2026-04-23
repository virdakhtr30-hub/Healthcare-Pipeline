"""
Run Phase 6: Execute timestamp-based, trigger-based, and log-based CDC
for steady and burst scenarios using the synthetic producer output.

This enforces the stricter interpretation of the project:
synthetic generator -> steady/burst producer scenarios -> CDC capture.
"""

from __future__ import annotations

from pathlib import Path
import json
import pandas as pd

from config.constants import (
    SYNTHETIC_DIR,
    CDC_LOG_DIR,
    CHECKPOINT_DIR,
    TELEMETRY_DIR,
    SNAPSHOTS_DIR,
    BURST_SIZE,
    STEADY_RATE_PER_SEC,
)
from src.models import (
    load_sources_config,
    load_datasets_config,
    load_jobs_config,
    load_contracts_config,
)
from src.timestamp_cdc import TimestampCDCPipeline
from src.trigger_cdc import TriggerCDCPipeline
from src.log_cdc import LogCDCPipeline
from src.scenario_runner import build_steady_batches, build_burst_batches, assign_cdc_operations
from src.utils import safe_mkdir, now_utc_iso


SOURCE_KEY = "simulated_vitals_stream"
DATASET_KEY = "vitals_stream_dataset"
CONTRACT_KEY = "vitals_stream_contract"


def ensure_synthetic_vitals_exists() -> Path:
    path = SYNTHETIC_DIR / "simulated_vitals_stream_synthetic.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Synthetic vitals file not found: {path}. Run Phase 3 first."
        )
    return path


def write_batches_to_csv(batches: list[pd.DataFrame], output_path: Path) -> Path:
    df = pd.concat(batches, ignore_index=True)
    df.to_csv(output_path, index=False)
    return output_path


def main() -> None:
    for path in [CDC_LOG_DIR, CHECKPOINT_DIR, TELEMETRY_DIR, SNAPSHOTS_DIR, SYNTHETIC_DIR]:
        safe_mkdir(path)

    sources = load_sources_config()
    datasets = load_datasets_config()
    jobs = load_jobs_config()
    contracts = load_contracts_config()

    source = sources[SOURCE_KEY]
    dataset = datasets[DATASET_KEY]
    contract = contracts[CONTRACT_KEY]

    synthetic_input_path = ensure_synthetic_vitals_exists()
    producer_df = pd.read_csv(synthetic_input_path)
    producer_df = assign_cdc_operations(producer_df, seed=42)

    steady_input_path = SYNTHETIC_DIR / "simulated_vitals_stream_phase6_steady.csv"
    burst_input_path = SYNTHETIC_DIR / "simulated_vitals_stream_phase6_burst.csv"

    steady_batches = build_steady_batches(producer_df, rate_per_sec=STEADY_RATE_PER_SEC, duration_seconds=30)
    burst_batches = build_burst_batches(producer_df, burst_size=BURST_SIZE)

    write_batches_to_csv(steady_batches, steady_input_path)
    write_batches_to_csv(burst_batches, burst_input_path)

    all_summaries = []

    scenarios = {
        "steady": steady_input_path,
        "burst": burst_input_path,
    }

    for scenario_name, input_path in scenarios.items():
        timestamp_pipeline = TimestampCDCPipeline(
            source=source,
            dataset=dataset,
            job=jobs["job_timestamp_cdc_vitals"],
            contract=contract,
        )
        trigger_pipeline = TriggerCDCPipeline(
            source=source,
            dataset=dataset,
            job=jobs["job_trigger_cdc"],
            contract=contract,
        )
        log_pipeline = LogCDCPipeline(
            source=source,
            dataset=dataset,
            job=jobs["job_log_cdc"],
            contract=contract,
        )

        all_summaries.append(
            timestamp_pipeline.run(
                input_path=input_path,
                accepted_path=CDC_LOG_DIR / f"timestamp_cdc_{scenario_name}_accepted.jsonl",
                quarantine_path=CDC_LOG_DIR / f"timestamp_cdc_{scenario_name}_quarantine.csv",
                checkpoint_path=CHECKPOINT_DIR / f"timestamp_cdc_{scenario_name}_checkpoint.json",
                cdc_log_path=CDC_LOG_DIR / f"timestamp_cdc_{scenario_name}_log.json",
                telemetry_path=TELEMETRY_DIR / f"timestamp_cdc_{scenario_name}_telemetry.json",
                summary_path=SNAPSHOTS_DIR / f"timestamp_cdc_{scenario_name}_summary.json",
            )
        )

        all_summaries.append(
            trigger_pipeline.run(
                input_path=input_path,
                accepted_path=CDC_LOG_DIR / f"trigger_cdc_{scenario_name}_accepted.jsonl",
                quarantine_path=CDC_LOG_DIR / f"trigger_cdc_{scenario_name}_quarantine.csv",
                checkpoint_path=CHECKPOINT_DIR / f"trigger_cdc_{scenario_name}_checkpoint.json",
                cdc_log_path=CDC_LOG_DIR / f"trigger_cdc_{scenario_name}_log.json",
                telemetry_path=TELEMETRY_DIR / f"trigger_cdc_{scenario_name}_telemetry.json",
                summary_path=SNAPSHOTS_DIR / f"trigger_cdc_{scenario_name}_summary.json",
            )
        )

        all_summaries.append(
            log_pipeline.run(
                input_path=input_path,
                accepted_path=CDC_LOG_DIR / f"log_cdc_{scenario_name}_accepted.jsonl",
                quarantine_path=CDC_LOG_DIR / f"log_cdc_{scenario_name}_quarantine.csv",
                checkpoint_path=CHECKPOINT_DIR / f"log_cdc_{scenario_name}_checkpoint.json",
                cdc_log_path=CDC_LOG_DIR / f"log_cdc_{scenario_name}_log.json",
                telemetry_path=TELEMETRY_DIR / f"log_cdc_{scenario_name}_telemetry.json",
                summary_path=SNAPSHOTS_DIR / f"log_cdc_{scenario_name}_summary.json",
            )
        )

    overall_summary_path = SNAPSHOTS_DIR / "phase6_all_cdc_summary.json"
    with open(overall_summary_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": now_utc_iso(),
                "summaries": all_summaries,
                "steady_input_path": str(steady_input_path),
                "burst_input_path": str(burst_input_path),
            },
            f,
            indent=2,
            default=str,
        )

    print("Phase 6 CDC completed successfully.")
    print(f"Overall summary: {overall_summary_path}")


if __name__ == "__main__":
    main()