from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "person_a",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id="de_healthcare_pipeline",
    description="Healthcare ingestion pipeline orchestration from Phase 3 to Phase 7",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["healthcare", "ingestion", "cdc", "telemetry", "coursework"],
) as dag:

    phase3_similarity = BashOperator(
        task_id="phase3_similarity_and_synthetic_generation",
        bash_command="cd /opt/project && python run_phase3_similarity.py",
    )

    phase4_initial_load = BashOperator(
        task_id="phase4_initial_batch_load",
        bash_command="cd /opt/project && python run_phase4_initial_load.py",
    )

    phase5_streaming = BashOperator(
        task_id="phase5_streaming_scenarios",
        bash_command="cd /opt/project && python run_phase5_cdc_scenarios.py",
    )

    phase6_cdc = BashOperator(
        task_id="phase6_all_cdc",
        bash_command="cd /opt/project && python run_phase6_all_cdc.py",
    )

    phase7_telemetry = BashOperator(
        task_id="phase7_telemetry_summary",
        bash_command="cd /opt/project && python run_phase7_telemetry_summary.py",
    )

    phase3_similarity >> phase4_initial_load >> phase5_streaming >> phase6_cdc >> phase7_telemetry