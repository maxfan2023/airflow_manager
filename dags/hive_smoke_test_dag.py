from __future__ import annotations

import getpass
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator


DAG_ID = "hive_smoke_test_dag"
TABLE_FQN = "default.max_hive_testing_students"

RUN_USER = os.getenv("HIVE_TEST_RUN_USER", getpass.getuser())
TASK_QUEUE = "queue_worker_fap41-abibatch-01" if RUN_USER == "fap41-abibatch-01" else "default"

KERBEROS_PRINCIPAL = os.getenv("HIVE_TEST_KRB_PRINCIPAL", RUN_USER)
AIRFLOW_ENV = os.getenv("AIRFLOW_ENV", "dev").lower()
DEV_MAX_KEYTAB_PATH = "/home/d373411/43373411.keytab"
DEFAULT_KEYTAB_PATH = DEV_MAX_KEYTAB_PATH if AIRFLOW_ENV == "dev" else f"/home/{RUN_USER}/{RUN_USER}.keytab"
KEYTAB_PATH = os.getenv("HIVE_TEST_KEYTAB_PATH", DEFAULT_KEYTAB_PATH)
HIVE_CLI_CONN_ID = os.getenv("HIVE_TEST_HIVE_CLI_CONN_ID", "hive_cli_default")

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_FQN} (
    student_id INT,
    student_name STRING,
    age INT
)
STORED AS PARQUET
"""

INSERT_SQL = f"""
TRUNCATE TABLE {TABLE_FQN};
INSERT INTO TABLE {TABLE_FQN}
VALUES
    (1, 'Alice', 18),
    (2, 'Bob', 19),
    (3, 'Cindy', 20)
"""

SELECT_SQL = f"""
SELECT student_id, student_name, age
FROM {TABLE_FQN}
ORDER BY student_id
"""


with DAG(
    dag_id=DAG_ID,
    description="Hive smoke test: kinit -> create table -> insert -> query and print",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": RUN_USER, "retries": 0},
    tags=["hive", "smoke-test"],
) as dag:
    kinit = BashOperator(
        task_id="kinit_with_keytab",
        queue=TASK_QUEUE,
        env={
            "RUN_USER": RUN_USER,
            "KERBEROS_PRINCIPAL": KERBEROS_PRINCIPAL,
            "KEYTAB_PATH": KEYTAB_PATH,
        },
        bash_command="""
        set -euo pipefail
        echo "Run user: ${RUN_USER}"
        echo "Airflow env: {{ params.airflow_env }}"
        echo "Airflow queue: {{ params.task_queue }}"
        echo "Hive conn id: {{ params.hive_cli_conn_id }}"
        echo "Kerberos principal: ${KERBEROS_PRINCIPAL}"
        echo "Keytab: ${KEYTAB_PATH}"

        if [ ! -f "${KEYTAB_PATH}" ]; then
          echo "Keytab not found: ${KEYTAB_PATH}" >&2
          exit 1
        fi

        kinit -kt "${KEYTAB_PATH}" "${KERBEROS_PRINCIPAL}"
        klist
        """,
        params={"task_queue": TASK_QUEUE, "airflow_env": AIRFLOW_ENV, "hive_cli_conn_id": HIVE_CLI_CONN_ID},
    )

    create_table = HiveOperator(
        task_id="create_hive_test_table",
        queue=TASK_QUEUE,
        hive_cli_conn_id=HIVE_CLI_CONN_ID,
        schema="default",
        hql=CREATE_TABLE_SQL,
    )

    insert_rows = HiveOperator(
        task_id="insert_hive_test_data",
        queue=TASK_QUEUE,
        hive_cli_conn_id=HIVE_CLI_CONN_ID,
        schema="default",
        hql=INSERT_SQL,
    )

    query_and_print = HiveOperator(
        task_id="query_and_print_hive_data",
        queue=TASK_QUEUE,
        hive_cli_conn_id=HIVE_CLI_CONN_ID,
        schema="default",
        hql=SELECT_SQL,
    )

    kinit >> create_table >> insert_rows >> query_and_print
