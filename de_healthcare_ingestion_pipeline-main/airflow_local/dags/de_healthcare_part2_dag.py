from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"


with DAG(
    dag_id="de_healthcare_part2_pipeline",
    description="Part 2 healthcare Lambda architecture pipeline: Bronze, Silver, Gold, KPIs",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "healthcare", "part2"],
) as dag:

    bronze_load = BashOperator(
        task_id="bronze_load",
        bash_command=f"cd {PROJECT_DIR} && python run_part2_bronze.py",
    )

    storage_metrics = BashOperator(
        task_id="storage_metrics",
        bash_command=f"cd {PROJECT_DIR} && python run_part2_storage_metrics.py",
    )

    silver_transformations = BashOperator(
        task_id="silver_transformations",
        bash_command=f"cd {PROJECT_DIR} && python run_part2_silver.py",
    )

    gold_transformations = BashOperator(
        task_id="gold_transformations",
        bash_command=f"cd {PROJECT_DIR} && python run_part2_gold.py",
    )

    full_pipeline = BashOperator(
        task_id="full_pipeline_check",
        bash_command=f"cd {PROJECT_DIR} && python run_part2_pipeline.py",
    )

    bronze_load >> storage_metrics >> silver_transformations >> gold_transformations >> full_pipeline