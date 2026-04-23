"""
Reporting utilities for writing structured project summaries.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import pandas as pd

from src.utils import now_utc_iso
from src.io_utils import write_json, write_dataframe_csv


def write_similarity_report(report: dict[str, Any], output_path: str | Path) -> None:
    payload = {
        "generated_at": now_utc_iso(),
        **report,
    }
    write_json(payload, output_path)


def write_telemetry_table(df: pd.DataFrame, csv_path: str | Path, json_path: str | Path) -> None:
    write_dataframe_csv(df, csv_path)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, indent=2, default=str)