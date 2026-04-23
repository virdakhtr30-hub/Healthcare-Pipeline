"""
Run the full implemented project flow in sequence.

Current sequence:
- Phase 3: profiling + synthetic generation + similarity report
- Phase 4: initial batch ingestion
- Phase 5: steady and burst stream scenarios
- Phase 6: CDC strategies
- Phase 7: telemetry aggregation

This script intentionally orchestrates only implemented phases.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent

RUNNERS = [
    "run_phase3_similarity.py",
    "run_phase4_initial_load.py",
    "run_phase5_cdc_scenarios.py",
    "run_phase6_all_cdc.py",
    "run_phase7_telemetry_summary.py",
]


def run_script(script_name: str) -> None:
    script_path = PROJECT_ROOT / script_name
    print(f"\n{'=' * 100}")
    print(f"Running: {script_name}")
    print(f"{'=' * 100}\n")

    result = subprocess.run([sys.executable, str(script_path)], cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        raise SystemExit(f"Pipeline stopped because {script_name} failed with code {result.returncode}.")


def main() -> None:
    for runner in RUNNERS:
        run_script(runner)

    print("\nAll implemented phases completed successfully.")


if __name__ == "__main__":
    main()