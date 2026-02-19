from __future__ import annotations

import getpass
import os
import re
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


DAG_ID = "hive_smoke_test_dag"
TABLE_FQN = "default.max_hive_testing_students"

RUN_USER = os.getenv("HIVE_TEST_RUN_USER", getpass.getuser())
TASK_QUEUE = "queue_worker_fap41-abibatch-01" if RUN_USER == "fap41-abibatch-01" else "default"

KERBEROS_PRINCIPAL = os.getenv("HIVE_TEST_KRB_PRINCIPAL", RUN_USER)
AIRFLOW_ENV = os.getenv("AIRFLOW_ENV", "dev").lower()
DEV_MAX_KEYTAB_PATH = "/home/d373411/43373411.keytab"
DEFAULT_KEYTAB_PATH = DEV_MAX_KEYTAB_PATH if AIRFLOW_ENV == "dev" else f"/home/{RUN_USER}/{RUN_USER}.keytab"
KEYTAB_PATH = os.getenv("HIVE_TEST_KEYTAB_PATH", DEFAULT_KEYTAB_PATH)

DEV_HIVE_JDBC_URL = (
    "jdbc:hive2://"
    "hkl25182035.hk.hsbc:2181,"
    "hkl25182036.hk.hsbc:2181,"
    "hkl25182161.hk.hsbc:2181/"
    "default;"
    "password=d3373411;"
    "principal=hive/_HOST@HRES.ADROOT.HSBC;"
    "serviceDiscoveryMode=zooKeeper;"
    "ssl=true;"
    "sslTrustStore=/opt/cloudera/security/pki/truststore.jks;"
    "trustStorePassword=changeme;"
    "truststoreType=jks;"
    "user=d3373411;"
    "zooKeeperNamespace=hiveserver2"
)
DEFAULT_HIVE_JDBC_URL = DEV_HIVE_JDBC_URL if AIRFLOW_ENV == "dev" else ""
HIVE_JDBC_URL = re.sub(
    r"\s+",
    "",
    os.getenv("HIVE_TEST_JDBC_URL", os.getenv("AB_HIVE_JDBC_URL", DEFAULT_HIVE_JDBC_URL)),
)

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
            "HIVE_JDBC_URL": HIVE_JDBC_URL,
        },
        bash_command="""
        set -euo pipefail
        echo "Run user: ${RUN_USER}"
        echo "Airflow env: {{ params.airflow_env }}"
        echo "Airflow queue: {{ params.task_queue }}"
        echo "Kerberos principal: ${KERBEROS_PRINCIPAL}"
        echo "Keytab: ${KEYTAB_PATH}"

        if [ -z "${HIVE_JDBC_URL}" ]; then
          echo "HIVE_JDBC_URL is empty. Please set HIVE_TEST_JDBC_URL or AB_HIVE_JDBC_URL." >&2
          exit 1
        fi

        if [ ! -f "${KEYTAB_PATH}" ]; then
          echo "Keytab not found: ${KEYTAB_PATH}" >&2
          exit 1
        fi

        kinit -kt "${KEYTAB_PATH}" "${KERBEROS_PRINCIPAL}"
        klist
        """,
        params={"task_queue": TASK_QUEUE, "airflow_env": AIRFLOW_ENV},
    )

    create_table = BashOperator(
        task_id="create_hive_test_table_with_beeline",
        queue=TASK_QUEUE,
        env={"HIVE_JDBC_URL": HIVE_JDBC_URL},
        bash_command=f"""
        set -euo pipefail
        beeline -u "${{HIVE_JDBC_URL}}" --showHeader=false --outputformat=tsv2 -e "{CREATE_TABLE_SQL.strip()}"
        """,
    )

    insert_rows = BashOperator(
        task_id="insert_hive_test_data_with_beeline",
        queue=TASK_QUEUE,
        env={"HIVE_JDBC_URL": HIVE_JDBC_URL},
        bash_command=f"""
        set -euo pipefail
        beeline -u "${{HIVE_JDBC_URL}}" --showHeader=false --outputformat=tsv2 -e "{INSERT_SQL.strip()}"
        """,
    )

    query_and_print = BashOperator(
        task_id="query_and_print_hive_data_with_beeline",
        queue=TASK_QUEUE,
        env={"HIVE_JDBC_URL": HIVE_JDBC_URL},
        bash_command=f"""
        set -euo pipefail
        beeline -u "${{HIVE_JDBC_URL}}" --showHeader=true --outputformat=table -e "{SELECT_SQL.strip()}"
        """,
    )

    done = EmptyOperator(task_id="done", queue=TASK_QUEUE)

    kinit >> create_table >> insert_rows >> query_and_print >> done
