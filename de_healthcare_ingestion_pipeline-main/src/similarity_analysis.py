"""
Similarity analysis utilities for comparing original and synthetic datasets.
"""

from __future__ import annotations

from typing import Any
import numpy as np
import pandas as pd


def kl_divergence_numeric(real_series: pd.Series, synth_series: pd.Series, bins: int = 20) -> float | None:
    real = pd.to_numeric(real_series, errors="coerce").dropna()
    synth = pd.to_numeric(synth_series, errors="coerce").dropna()

    if len(real) < 10 or len(synth) < 10:
        return None

    min_edge = min(real.min(), synth.min())
    max_edge = max(real.max(), synth.max())
    if min_edge == max_edge:
        return 0.0

    hist_real, edges = np.histogram(real, bins=bins, range=(min_edge, max_edge), density=True)
    hist_synth, _ = np.histogram(synth, bins=edges, density=True)

    eps = 1e-9
    p = hist_real + eps
    q = hist_synth + eps

    p = p / p.sum()
    q = q / q.sum()

    return float(np.sum(p * np.log(p / q)))


def compare_dataframes(real_df: pd.DataFrame, synth_df: pd.DataFrame) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "real_rows": int(len(real_df)),
        "synthetic_rows": int(len(synth_df)),
        "numeric_kl_divergence": {},
    }

    shared_columns = [c for c in real_df.columns if c in synth_df.columns]
    for col in shared_columns:
        score = kl_divergence_numeric(real_df[col], synth_df[col])
        if score is not None:
            metrics["numeric_kl_divergence"][col] = score

    return metrics