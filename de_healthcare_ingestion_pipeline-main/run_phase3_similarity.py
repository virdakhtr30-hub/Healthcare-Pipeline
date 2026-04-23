"""
Run Phase 3: Profile source datasets, generate synthetic data, and evaluate similarity.

Implements:
1. Distribution profiling
2. Synthetic data generation
3. Similarity validation using KL divergence on numeric columns

Outputs:
- data/profiled/*_profile.json
- data/synthetic/*_synthetic.csv
- data/snapshots/phase3_similarity_report.json
"""

from __future__ import annotations

from pathlib import Path
import json
from typing import Any

import pandas as pd

from config.constants import INPUT_DIR, PROFILED_DIR, SYNTHETIC_DIR, SNAPSHOTS_DIR
from src.models import load_sources_config
from src.profiling import profile_dataframe
from src.synthetic_generators import generate_synthetic_dataframe
from src.similarity_analysis import compare_dataframes
from src.utils import safe_mkdir, now_utc_iso


SOURCE_FILE_MAP = {
    "encounter_master": "encounter_master.csv",
    "diagnosis_events": "diagnosis_events.csv",
    "lab_results": "lab_results.csv",
    "mortality_registry": "mortality_registry.csv",
    "simulated_vitals_stream": "simulated_vitals_stream.csv",
}


def main() -> None:
    safe_mkdir(PROFILED_DIR)
    safe_mkdir(SYNTHETIC_DIR)
    safe_mkdir(SNAPSHOTS_DIR)

    sources = load_sources_config()

    overall_report: dict[str, Any] = {
        "generated_at": now_utc_iso(),
        "datasets": {},
    }

    for source_id, filename in SOURCE_FILE_MAP.items():
        input_path = INPUT_DIR / filename
        df = pd.read_csv(input_path)

        profile = profile_dataframe(df)
        profile_path = PROFILED_DIR / f"{source_id}_profile.json"
        with open(profile_path, "w", encoding="utf-8") as f:
            json.dump(profile, f, indent=2, default=str)

        primary_keys = sources[source_id].primary_key if source_id in sources else []
        synthetic_df = generate_synthetic_dataframe(df, primary_keys=primary_keys, multiplier=2)
        synthetic_path = SYNTHETIC_DIR / f"{source_id}_synthetic.csv"
        synthetic_df.to_csv(synthetic_path, index=False)

        similarity = compare_dataframes(df, synthetic_df)

        overall_report["datasets"][source_id] = {
            "input_rows": int(len(df)),
            "synthetic_rows": int(len(synthetic_df)),
            "profile_path": str(profile_path),
            "synthetic_path": str(synthetic_path),
            "similarity_metrics": similarity,
        }

        print(f"Completed profiling + synthetic generation for: {source_id}")

    report_path = SNAPSHOTS_DIR / "phase3_similarity_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(overall_report, f, indent=2, default=str)

    print(f"\nPhase 3 similarity report written to: {report_path}")


if __name__ == "__main__":
    main()