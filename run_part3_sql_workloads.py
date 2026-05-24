import duckdb
from pathlib import Path


GOLD_PATH = "data/iceberg_warehouse/gold"


def run_query(title, query):
    print(f"\n=== {title} ===")
    result = duckdb.sql(query).df()
    print(result.head())
    return result


if __name__ == "__main__":

    patient_risk_files = str(
        Path(GOLD_PATH, "gold_patient_risk_daily").resolve()
    ) + "/**/*.parquet"

    operational_files = str(
        Path(GOLD_PATH, "gold_operational_monitoring").resolve()
    ) + "/**/*.parquet"

    print("DuckDB workload engine ready.")