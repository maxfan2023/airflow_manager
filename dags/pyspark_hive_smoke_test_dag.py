from __future__ import annotations

import getpass
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


DAG_ID = "pyspark_hive_smoke_test_dag"
TABLE_FQN = os.getenv("PYSPARK_TEST_TABLE_FQN", "default.max_hive_testing_courses")

RUN_USER = os.getenv("PYSPARK_TEST_RUN_USER", getpass.getuser())
TASK_QUEUE = "queue_worker_fap41-abibatch-01" if RUN_USER == "fap41-abibatch-01" else "default"

AIRFLOW_ENV = os.getenv("AIRFLOW_ENV", "dev").lower()
DEV_MAX_KEYTAB_PATH = os.getenv("PYSPARK_TEST_DEV_MAX_KEYTAB_PATH", "/home/d373411/43373411.keytab")
DEFAULT_KEYTAB_PATH = DEV_MAX_KEYTAB_PATH if AIRFLOW_ENV == "dev" else f"/home/{RUN_USER}/{RUN_USER}.keytab"
KEYTAB_PATH = os.getenv("PYSPARK_TEST_KEYTAB_PATH", DEFAULT_KEYTAB_PATH)
KERBEROS_PRINCIPAL = os.getenv("PYSPARK_TEST_KRB_PRINCIPAL", RUN_USER)

SPARK_CONN_ID = os.getenv("PYSPARK_TEST_SPARK_CONN_ID", "spark_default")
SPARK_YARN_QUEUE = os.getenv("PYSPARK_TEST_YARN_QUEUE")
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PYSPARK_APP = os.path.join(DAG_DIR, "scripts", "pyspark_hive_table_test_job.py")

SPARK_SUBMIT_CONF = {
    "spark.sql.catalogImplementation": "hive",
}

COMMON_SPARK_SUBMIT_ARGS = {
    "conn_id": SPARK_CONN_ID,
    "application": PYSPARK_APP,
    "queue": TASK_QUEUE,
    "conf": SPARK_SUBMIT_CONF,
    "principal": KERBEROS_PRINCIPAL,
    "keytab": KEYTAB_PATH,
}

if SPARK_YARN_QUEUE:
    COMMON_SPARK_SUBMIT_ARGS["yarn_queue"] = SPARK_YARN_QUEUE


with DAG(
    dag_id=DAG_ID,
    description="PySpark smoke test DAG: create table, insert records, query and print",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": RUN_USER, "retries": 0},
    tags=["spark", "pyspark", "smoke-test", "GDTET_GLOBAL_DAG"],
) as dag:
    create_hive_test_table = SparkSubmitOperator(
        task_id="create_hive_test_table",
        application_args=["--action", "create", "--table", TABLE_FQN],
        name="pyspark_create_hive_test_table",
        **COMMON_SPARK_SUBMIT_ARGS,
    )

    insert_hive_test_data = SparkSubmitOperator(
        task_id="insert_hive_test_data",
        application_args=["--action", "insert", "--table", TABLE_FQN],
        name="pyspark_insert_hive_test_data",
        **COMMON_SPARK_SUBMIT_ARGS,
    )

    query_and_print_hive_data = SparkSubmitOperator(
        task_id="query_and_print_hive_data",
        application_args=["--action", "query", "--table", TABLE_FQN],
        name="pyspark_query_and_print_hive_data",
        **COMMON_SPARK_SUBMIT_ARGS,
    )

    create_hive_test_table >> insert_hive_test_data >> query_and_print_hive_data
