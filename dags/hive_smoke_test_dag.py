from __future__ import annotations

import getpass
import logging
import os
import tempfile
from datetime import datetime

from airflow import DAG
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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
AIRFLOW_TMP_DIR = os.getenv("HIVE_TEST_TMP_DIR", "/FCR_APP/abinitio/tmp")
HIVE_LOCAL_SCRATCHDIR = AIRFLOW_TMP_DIR
EXPECTED_HIVE_PRINCIPAL = os.getenv("HIVE_TEST_EXPECTED_HIVE_PRINCIPAL", "hive/_HOST@HRES.ADROOT.HSBC")

# HiveCliHook creates temporary files via Python tempfile; force company-approved tmp dir.
os.environ["TMPDIR"] = AIRFLOW_TMP_DIR
os.environ["TMP"] = AIRFLOW_TMP_DIR
os.environ["TEMP"] = AIRFLOW_TMP_DIR
tempfile.tempdir = AIRFLOW_TMP_DIR

HIVE_CLI_PARAMS = os.getenv(
    "HIVE_TEST_HIVE_CLI_PARAMS",
    f"--hiveconf hive.exec.local.scratchdir={HIVE_LOCAL_SCRATCHDIR} "
    "--hiveconf javax.net.ssl.trustStore=/opt/cloudera/security/pki/truststore.jks "
    "--hiveconf javax.net.ssl.trustStorePassword=changeme "
    "--hiveconf javax.net.ssl.trustStoreType=jks",
)
HIVE_OPERATOR_AUTH = os.getenv(
    "HIVE_TEST_HIVE_AUTH",
    "",
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


def _validate_hive_cli_connection(hive_cli_conn_id: str) -> None:
    conn = BaseHook.get_connection(hive_cli_conn_id)
    extra = conn.extra_dejson
    security = conf.get("core", "security", fallback="").lower()
    issues: list[str] = []

    if not extra.get("use_beeline", False):
        issues.append("Connection extra.use_beeline should be true.")

    if "," in (conn.host or "") and not extra.get("high_availability", False):
        issues.append("Connection host has multiple endpoints, but extra.high_availability is not true.")

    if extra.get("high_availability", False) and security != "kerberos" and not HIVE_OPERATOR_AUTH:
        issues.append(
            f"Airflow core.security is '{security or 'empty'}'; set it to 'kerberos' so HiveOperator "
            "builds JDBC URL with serviceDiscoveryMode/principal/zooKeeperNamespace, "
            "or set HIVE_TEST_HIVE_AUTH."
        )

    if security == "kerberos":
        configured_principal = str(extra.get("principal", "")).strip()
        if not configured_principal:
            issues.append(
                "Airflow core.security=kerberos but connection extra.principal is empty. "
                f"Set principal to '{EXPECTED_HIVE_PRINCIPAL}'."
            )
        elif "EXAMPLE.COM" in configured_principal.upper():
            issues.append(
                f"Connection principal looks wrong: '{configured_principal}'. "
                f"Expected realm from '{EXPECTED_HIVE_PRINCIPAL}'."
            )
        elif "@" in EXPECTED_HIVE_PRINCIPAL:
            expected_realm = EXPECTED_HIVE_PRINCIPAL.split("@", 1)[1].upper()
            if f"@{expected_realm}" not in configured_principal.upper():
                issues.append(
                    f"Connection principal realm mismatch: '{configured_principal}'. "
                    f"Expected realm '{expected_realm}'."
                )

    if issues:
        raise AirflowException("Hive connection precheck failed:\n- " + "\n- ".join(issues))

    logging.info(
        "Hive connection precheck passed: conn_id=%s host=%s schema=%s high_availability=%s security=%s principal=%s",
        hive_cli_conn_id,
        conn.host,
        conn.schema,
        extra.get("high_availability", False),
        security,
        extra.get("principal", ""),
    )


with DAG(
    dag_id=DAG_ID,
    description="Hive smoke test: kinit -> create table -> insert -> query and print",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": RUN_USER, "retries": 0},
    tags=["hive", "smoke-test"],
) as dag:
    kinit_env = {
        "RUN_USER": RUN_USER,
        "KERBEROS_PRINCIPAL": KERBEROS_PRINCIPAL,
        "KEYTAB_PATH": KEYTAB_PATH,
        "HIVE_LOCAL_SCRATCHDIR": HIVE_LOCAL_SCRATCHDIR,
        "TMPDIR": AIRFLOW_TMP_DIR,
        "TMP": AIRFLOW_TMP_DIR,
        "TEMP": AIRFLOW_TMP_DIR,
    }

    print_runtime_context = BashOperator(
        task_id="print_runtime_context",
        queue=TASK_QUEUE,
        env=kinit_env,
        bash_command="""
        echo "Run user: ${RUN_USER}"
        echo "Airflow env: {{ params.airflow_env }}"
        echo "Airflow queue: {{ params.task_queue }}"
        echo "Hive conn id: {{ params.hive_cli_conn_id }}"
        echo "Hive auth: {{ params.hive_operator_auth }}"
        echo "hive.exec.local.scratchdir: {{ params.hive_local_scratchdir }}"
        echo "Expected hive principal: {{ params.expected_hive_principal }}"
        echo "Kerberos principal: ${KERBEROS_PRINCIPAL}"
        echo "Keytab: ${KEYTAB_PATH}"
        """,
        params={
            "task_queue": TASK_QUEUE,
            "airflow_env": AIRFLOW_ENV,
            "hive_cli_conn_id": HIVE_CLI_CONN_ID,
            "hive_operator_auth": HIVE_OPERATOR_AUTH,
            "hive_local_scratchdir": HIVE_LOCAL_SCRATCHDIR,
            "expected_hive_principal": EXPECTED_HIVE_PRINCIPAL,
        },
    )

    check_tmpdir = BashOperator(
        task_id="check_tmpdir",
        queue=TASK_QUEUE,
        env=kinit_env,
        bash_command="""
        set -euo pipefail
        mkdir -p "${HIVE_LOCAL_SCRATCHDIR}"
        test -d "${HIVE_LOCAL_SCRATCHDIR}"
        test -w "${HIVE_LOCAL_SCRATCHDIR}"
        tmp_file="$(mktemp "${HIVE_LOCAL_SCRATCHDIR}/airflow_hive_scratchdir_test.XXXXXX")"
        ls -l "${tmp_file}"
        rm -f "${tmp_file}"
        """,
    )

    check_keytab_file = BashOperator(
        task_id="check_keytab_file",
        queue=TASK_QUEUE,
        env=kinit_env,
        bash_command="""
        set -euo pipefail
        test -f "${KEYTAB_PATH}"
        ls -l "${KEYTAB_PATH}"
        """,
    )

    run_kinit = BashOperator(
        task_id="run_kinit",
        queue=TASK_QUEUE,
        env=kinit_env,
        bash_command="""
        set -euo pipefail
        kinit -kt "${KEYTAB_PATH}" "${KERBEROS_PRINCIPAL}"
        """,
    )

    show_klist = BashOperator(
        task_id="show_klist",
        queue=TASK_QUEUE,
        env=kinit_env,
        bash_command="""
        set -euo pipefail
        klist
        """,
    )

    validate_hive_connection = PythonOperator(
        task_id="validate_hive_cli_connection",
        queue=TASK_QUEUE,
        python_callable=_validate_hive_cli_connection,
        op_kwargs={"hive_cli_conn_id": HIVE_CLI_CONN_ID},
    )

    create_table = HiveOperator(
        task_id="create_hive_test_table",
        queue=TASK_QUEUE,
        hive_cli_conn_id=HIVE_CLI_CONN_ID,
        hive_cli_params=HIVE_CLI_PARAMS,
        auth=HIVE_OPERATOR_AUTH,
        schema="default",
        hql=CREATE_TABLE_SQL,
    )

    insert_rows = HiveOperator(
        task_id="insert_hive_test_data",
        queue=TASK_QUEUE,
        hive_cli_conn_id=HIVE_CLI_CONN_ID,
        hive_cli_params=HIVE_CLI_PARAMS,
        auth=HIVE_OPERATOR_AUTH,
        schema="default",
        hql=INSERT_SQL,
    )

    query_and_print = HiveOperator(
        task_id="query_and_print_hive_data",
        queue=TASK_QUEUE,
        hive_cli_conn_id=HIVE_CLI_CONN_ID,
        hive_cli_params=HIVE_CLI_PARAMS,
        auth=HIVE_OPERATOR_AUTH,
        schema="default",
        hql=SELECT_SQL,
    )

    print_runtime_context >> check_tmpdir >> check_keytab_file >> run_kinit >> show_klist
    show_klist >> validate_hive_connection >> create_table >> insert_rows >> query_and_print
