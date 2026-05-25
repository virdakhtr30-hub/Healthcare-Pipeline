from pathlib import Path
import duckdb


DB_PATH = Path("data/healthcare_semantic.duckdb")
OUTPUT_DIR = Path("data/semantic_exports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def export_table(con, table_name: str):
    output_file = OUTPUT_DIR / f"{table_name}.csv"

    con.execute(f"""
        COPY (
            SELECT *
            FROM {table_name}
        )
        TO '{output_file.as_posix()}'
        WITH (HEADER, DELIMITER ',');
    """)

    print(f"Exported {table_name} -> {output_file}")


def main():
    con = duckdb.connect(str(DB_PATH))

    export_table(con, "mart_patient_risk_summary")
    export_table(con, "mart_operational_summary")
    export_table(con, "mart_healthcare_bi_summary")

    con.close()
    print("dbt semantic marts exported successfully.")


if __name__ == "__main__":
    main()