"""
Run Phase 4: Initial batch load ingestion for all batch datasets.

This script:
1. Loads YAML configs
2. Builds batch ingestion pipelines
3. Runs initial full-snapshot ingestion for each batch source
4. Writes accepted/quarantine/error/telemetry/summary outputs
"""

from __future__ import annotations

from pathlib import Path

from config.constants import (
    INPUT_DIR,
    ACCEPTED_DIR,
    QUARANTINE_DIR,
    TELEMETRY_DIR,
    SNAPSHOTS_DIR,
)
from src.models import (
    load_sources_config,
    load_datasets_config,
    load_jobs_config,
    load_contracts_config,
)
from src.batch_ingestion import BatchIngestionPipeline


def main() -> None:
    """
    Execute initial batch ingestion for all batch-style healthcare sources.
    """
    sources = load_sources_config()
    datasets = load_datasets_config()
    jobs = load_jobs_config()
    contracts = load_contracts_config()

    # Mapping:
    # (source_key, dataset_key, job_key, contract_key, input_filename)
    source_to_dataset_job_contract = [
        ("encounter_master", "encounter_dataset", "job_initial_load_encounter", "encounter_contract", "encounter_master.csv"),
        ("diagnosis_events", "diagnosis_dataset", "job_initial_load_diagnosis", "diagnosis_contract", "diagnosis_events.csv"),
        ("lab_results", "lab_results_dataset", "job_initial_load_labs", "lab_results_contract", "lab_results.csv"),
        ("mortality_registry", "mortality_dataset", "job_initial_load_mortality", "mortality_contract", "mortality_registry.csv"),
    ]

    summaries = []

    for source_key, dataset_key, job_key, contract_key, filename in source_to_dataset_job_contract:
        pipeline = BatchIngestionPipeline(
            source=sources[source_key],
            dataset=datasets[dataset_key],
            job=jobs[job_key],
            contract=contracts[contract_key],
        )

        input_path = INPUT_DIR / filename
        accepted_path = ACCEPTED_DIR / f"{source_key}_accepted.jsonl"
        quarantine_path = QUARANTINE_DIR / f"{source_key}_quarantine.csv"
        error_report_path = SNAPSHOTS_DIR / f"{source_key}_error_report.csv"
        telemetry_path = TELEMETRY_DIR / f"{job_key}_telemetry.json"
        summary_path = SNAPSHOTS_DIR / f"{job_key}_summary.json"

        summary = pipeline.run(
            input_path=input_path,
            accepted_path=accepted_path,
            quarantine_path=quarantine_path,
            error_report_path=error_report_path,
            telemetry_path=telemetry_path,
            summary_path=summary_path,
        )

        summaries.append(summary)
        print(f"Completed initial load for: {source_key}")
        print(summary)
        print("-" * 80)

    print("\nAll batch initial loads completed successfully.")
    print(f"Total jobs run: {len(summaries)}")


if __name__ == "__main__":
    main()
