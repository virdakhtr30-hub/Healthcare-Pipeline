"""
Profiling utilities for source datasets.
"""

from __future__ import annotations

from typing import Any
import pandas as pd


def profile_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "columns": {},
    }

    for col in df.columns:
        series = df[col]
        col_info: dict[str, Any] = {
            "dtype": str(series.dtype),
            "null_fraction": float(series.isna().mean()),
            "unique_count": int(series.nunique(dropna=True)),
        }

        numeric_series = pd.to_numeric(series, errors="coerce")
        non_null_numeric = numeric_series.dropna()

        if len(non_null_numeric) >= max(5, int(0.8 * max(len(series.dropna()), 1))):
            col_info["kind"] = "numeric"
            col_info["mean"] = float(non_null_numeric.mean())
            col_info["std"] = float(non_null_numeric.std(ddof=0)) if len(non_null_numeric) > 1 else 0.0
            col_info["min"] = float(non_null_numeric.min())
            col_info["max"] = float(non_null_numeric.max())
        else:
            col_info["kind"] = "categorical"
            value_counts = series.astype("string").fillna("<NULL>").value_counts(normalize=True).head(50)
            col_info["top_values"] = {str(k): float(v) for k, v in value_counts.items()}

        profile["columns"][col] = col_info

    return profile