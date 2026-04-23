"""
Synthetic data generation utilities based on source distributions.
"""

from __future__ import annotations

import re
import numpy as np
import pandas as pd


DATETIME_NAME_HINTS = (
    "time",
    "timestamp",
    "datetime",
    "date",
    "_at",
)


def looks_like_datetime_column(column_name: str) -> bool:
    name = column_name.strip().lower()
    return any(token in name for token in DATETIME_NAME_HINTS)


def infer_id_prefix(values: pd.Series) -> str:
    sample = next((str(v) for v in values.dropna().astype(str).tolist() if v), "ID000001")
    match = re.match(r"([A-Za-z_]+)(\d+)$", sample)
    if match:
        return match.group(1)
    return "ID"


def synthesize_datetime_column(
    series: pd.Series,
    row_count: int,
    rng: np.random.Generator,
) -> pd.Series:
    dt_series = pd.to_datetime(series, errors="coerce", utc=True)
    non_null_dt = dt_series.dropna()

    if non_null_dt.empty:
        return pd.Series([pd.NA] * row_count, dtype="string")

    sampled = rng.choice(non_null_dt.astype("int64").to_numpy(), size=row_count, replace=True)
    jitter_seconds = rng.integers(0, 3600, size=row_count)
    adjusted = sampled + (jitter_seconds * 1_000_000_000)
    return pd.to_datetime(adjusted, utc=True).astype("string")


def synthesize_column(
    series: pd.Series,
    column_name: str,
    row_count: int,
    rng: np.random.Generator,
    primary_key: bool = False,
) -> pd.Series:
    clean = series.dropna()

    if primary_key:
        prefix = infer_id_prefix(series)
        width = 7
        sample = next((str(v) for v in clean.astype(str).tolist() if v), f"{prefix}0000001")
        match = re.match(r"([A-Za-z_]+)(\d+)$", sample)
        if match:
            width = len(match.group(2))
        return pd.Series([f"{prefix}{i:0{width}d}" for i in range(1, row_count + 1)], dtype="string")

    if looks_like_datetime_column(column_name):
        return synthesize_datetime_column(series, row_count=row_count, rng=rng)

    numeric_series = pd.to_numeric(series, errors="coerce")
    non_null_numeric = numeric_series.dropna()

    if len(non_null_numeric) >= max(5, int(0.8 * max(len(clean), 1))):
        sampled = rng.choice(non_null_numeric.to_numpy(), size=row_count, replace=True)
        std = float(non_null_numeric.std(ddof=0)) if len(non_null_numeric) > 1 else 0.0
        jitter = rng.normal(0, std * 0.05 if std > 0 else 0.0, size=row_count)
        out = sampled + jitter

        if pd.api.types.is_integer_dtype(non_null_numeric):
            out = np.round(out).astype(int)

        return pd.Series(out)

    if clean.empty:
        return pd.Series([pd.NA] * row_count, dtype="string")

    probs = clean.astype("string").value_counts(normalize=True)
    values = probs.index.to_list()
    weights = probs.to_numpy()
    sampled = rng.choice(values, size=row_count, replace=True, p=weights)
    return pd.Series(sampled, dtype="string")


def generate_synthetic_dataframe(
    df: pd.DataFrame,
    primary_keys: list[str],
    multiplier: int = 2,
    seed: int = 42,
) -> pd.DataFrame:
    row_count = max(len(df) * multiplier, len(df))
    rng = np.random.default_rng(seed)

    synthetic_cols = {}
    for col in df.columns:
        synthetic_cols[col] = synthesize_column(
            df[col],
            column_name=col,
            row_count=row_count,
            rng=rng,
            primary_key=col in primary_keys,
        )

    return pd.DataFrame(synthetic_cols)