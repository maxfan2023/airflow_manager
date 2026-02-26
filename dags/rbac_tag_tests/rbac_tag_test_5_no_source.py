from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="rbac_tag_test_5_no_source",
    description="Generated DAG for tag-based RBAC testing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="start")
