from pathlib import Path

from src.log_cdc import LogCDCPipeline
from src.models import (
    load_sources_config,
    load_datasets_config,
    load_jobs_config,
    load_contracts_config,
)

from config.constants import (
    INPUT_DIR,
    ACCEPTED_DIR,
    QUARANTINE_DIR,
    TELEMETRY_DIR,
    SNAPSHOTS_DIR,
)

def main():
    sources = load_sources_config()
    datasets = load_datasets_config()
    jobs = load_jobs_config()
    contracts = load_contracts_config()

    # Example: encounter dataset
    pipeline = LogCDCPipeline(
        source=sources["encounter_master"],
        dataset=datasets["encounter_dataset"],
        job=jobs["job_initial_load_encounter"],
        contract=contracts["encounter_contract"],
    )

    summary = pipeline.run(
        input_path=INPUT_DIR / "encounter_master.csv",
        accepted_path=ACCEPTED_DIR / "encounter_cdc.jsonl",
        quarantine_path=QUARANTINE_DIR / "encounter_cdc_quarantine.csv",
        checkpoint_path=SNAPSHOTS_DIR / "encounter_checkpoint.json",
        cdc_log_path=SNAPSHOTS_DIR / "encounter_cdc_log.json",
        telemetry_path=TELEMETRY_DIR / "encounter_cdc_telemetry.json",
        summary_path=SNAPSHOTS_DIR / "encounter_cdc_summary.json",
    )

    print("CDC Run Completed")
    print(summary)


if __name__ == "__main__":
    main()
