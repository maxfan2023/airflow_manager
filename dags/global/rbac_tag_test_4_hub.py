from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

source = "hub"

def _print_python_message(dag_id: str) -> None:
    print(f"python task executed in {dag_id}")

with DAG(
    dag_id="rbac_tag_test_4_hub",
    description="Generated DAG for tag-based RBAC testing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["GDTET_GLOBAL_DAG"],
) as dag:
    bash_echo = BashOperator(
        task_id="bash_echo",
        bash_command="echo 'bash task executed for rbac_tag_test_4_hub'",
    )

    python_echo = PythonOperator(
        task_id="python_echo",
        python_callable=_print_python_message,
        op_kwargs={"dag_id": "rbac_tag_test_4_hub"},
    )

    sleep_task = BashOperator(
        task_id="sleep_task",
        bash_command="sleep 5",
    )

    bash_echo >> python_echo >> sleep_task
