"""
Scenario helpers for generator-driven steady and burst simulation.
"""

from __future__ import annotations

from typing import Iterator
import pandas as pd
import numpy as np


def assign_cdc_operations(df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """
    Assign INSERT / UPDATE / DELETE operations to a synthetic producer output.

    Distribution:
    - INSERT 60%
    - UPDATE 30%
    - DELETE 10%
    """
    rng = np.random.default_rng(seed)
    operations = rng.choice(
        ["INSERT", "UPDATE", "DELETE"],
        size=len(df),
        p=[0.6, 0.3, 0.1],
    )

    out = df.copy().reset_index(drop=True)
    out["operation_type"] = operations

    # Simulate updates by perturbing mutable vitals columns
    update_mask = out["operation_type"] == "UPDATE"
    if "heart_rate" in out.columns:
        out.loc[update_mask, "heart_rate"] = pd.to_numeric(
            out.loc[update_mask, "heart_rate"], errors="coerce"
        ) + 3.0
    if "respiratory_rate" in out.columns:
        out.loc[update_mask, "respiratory_rate"] = pd.to_numeric(
            out.loc[update_mask, "respiratory_rate"], errors="coerce"
        ) + 1.0

    return out


def generator_loop(df: pd.DataFrame) -> Iterator[dict]:
    """
    Yield records one by one from a synthetic producer dataframe.
    """
    for record in df.to_dict(orient="records"):
        yield record


def build_steady_batches(df: pd.DataFrame, rate_per_sec: int = 10, duration_seconds: int = 30) -> list[pd.DataFrame]:
    """
    Simulate a steady producer loop as 1-second micro-batches.
    """
    total_records = min(len(df), rate_per_sec * duration_seconds)
    steady_df = df.head(total_records).copy().reset_index(drop=True)

    batches: list[pd.DataFrame] = []
    for start in range(0, len(steady_df), rate_per_sec):
        batches.append(steady_df.iloc[start:start + rate_per_sec].copy())

    return batches


def build_burst_batches(df: pd.DataFrame, burst_size: int = 5000) -> list[pd.DataFrame]:
    """
    Simulate a 1-second burst from the synthetic producer.
    """
    if len(df) >= burst_size:
        burst_df = df.head(burst_size).copy().reset_index(drop=True)
    else:
        burst_df = df.sample(n=burst_size, replace=True, random_state=42).reset_index(drop=True)

    return [burst_df]