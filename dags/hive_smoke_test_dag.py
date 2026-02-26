from __future__ import annotations

import getpass
import os
import subprocess
import tempfile
from functools import cached_property
from typing import Any
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
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
HIVE_JDBC_URL = os.getenv(
    "HIVE_TEST_HIVE_JDBC_URL",
    "jdbc:hive2://hkl25182035.hk.hsbc:2181,hkl25182036.hk.hsbc:2181,hkl25182161.hk.hsbc:2181/default;"
    "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2",
)
HIVE_JDBC_PRINCIPAL = os.getenv("HIVE_TEST_HIVE_JDBC_PRINCIPAL", "hive/_HOST@HRES.ADROOT.HSBC")
BEELINE_JAVA_HOME = os.getenv("HIVE_TEST_JAVA_HOME", os.getenv("AB_JAVA_HOME", os.getenv("JAVA_HOME", "")))
BEELINE_BIN = os.getenv("HIVE_TEST_BEELINE_BIN", "beeline")
BASH_INIT_FILE = os.path.expanduser(os.getenv("HIVE_TEST_BASH_INIT_FILE", "~/.bashr"))
USER_HOME = os.path.expanduser("~")

# HiveCliHook creates temporary files via Python tempfile; force company-approved tmp dir.
os.environ["TMPDIR"] = AIRFLOW_TMP_DIR
os.environ["TMP"] = AIRFLOW_TMP_DIR
os.environ["TEMP"] = AIRFLOW_TMP_DIR
tempfile.tempdir = AIRFLOW_TMP_DIR
if BEELINE_JAVA_HOME:
    os.environ["JAVA_HOME"] = BEELINE_JAVA_HOME
    os.environ["PATH"] = f"{BEELINE_JAVA_HOME}/bin:{os.environ.get('PATH', '')}"

HIVE_CLI_PARAMS = os.getenv(
    "HIVE_TEST_HIVE_CLI_PARAMS",
    f"--hiveconf hive.exec.local.scratchdir={HIVE_LOCAL_SCRATCHDIR}",
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


class HiveCliHookFixedJdbc(HiveCliHook):
    """Force HiveOperator to use a fixed beeline JDBC URL that is known to work."""

    def _prepare_cli_cmd(self, jdbc_url_value: str) -> list[Any]:
        hive_params_list = self.hive_cli_params.split()
        jdbc_url = f'"{jdbc_url_value}"'
        return [BEELINE_BIN, *hive_params_list, "-u", jdbc_url]

    def _build_retry_jdbc_url_with_principal(self, jdbc_url_value: str) -> str:
        if "principal=" in jdbc_url_value.lower() or not HIVE_JDBC_PRINCIPAL:
            return jdbc_url_value
        sep = "" if jdbc_url_value.endswith(";") else ";"
        return f"{jdbc_url_value}{sep}principal={HIVE_JDBC_PRINCIPAL}"

    def _run_beeline_cmd(self, hive_cmd: list[Any], proc_env: dict[str, str], verbose: bool) -> tuple[int, str]:
        if verbose:
            self.log.info("%s", " ".join(hive_cmd))
            self.log.info("Using beeline binary: %s", BEELINE_BIN)
            self.log.info("Using JAVA_HOME: %s", proc_env.get("JAVA_HOME", ""))

        sub_process: Any = subprocess.Popen(
            hive_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            close_fds=True,
            env=proc_env,
        )
        self.sub_process = sub_process
        stdout = ""
        for line_raw in iter(sub_process.stdout.readline, b""):
            line = line_raw.decode(errors="replace")
            stdout += line
            if verbose:
                self.log.info(line.strip())
        sub_process.wait()
        return sub_process.returncode, stdout

    def run_cli(
        self,
        hql: str,
        schema: str | None = None,
        verbose: bool = True,
        hive_conf: dict[Any, Any] | None = None,  # noqa: ARG002
    ) -> Any:
        """
        Run beeline with a fixed command shape:
        beeline --hiveconf hive.exec.local.scratchdir=... -u "<fixed_jdbc>" -e "<sql>"
        """
        conn = self.conn
        schema = schema or conn.schema or ""
        if schema:
            hql = f"USE {schema};\n{hql}"

        hql = hql.strip()
        if not hql.endswith(";"):
            hql += ";"

        proc_env = os.environ.copy()
        if BEELINE_JAVA_HOME:
            proc_env["JAVA_HOME"] = BEELINE_JAVA_HOME
            proc_env["PATH"] = f"{BEELINE_JAVA_HOME}/bin:{proc_env.get('PATH', '')}"
        proc_env["TMPDIR"] = AIRFLOW_TMP_DIR
        proc_env["TMP"] = AIRFLOW_TMP_DIR
        proc_env["TEMP"] = AIRFLOW_TMP_DIR

        hive_cmd = self._prepare_cli_cmd(HIVE_JDBC_URL)
        hive_cmd.extend(["-e", hql])
        rc, stdout = self._run_beeline_cmd(hive_cmd, proc_env, verbose)
        if rc == 0:
            return stdout

        low_stdout = stdout.lower()
        needs_principal_retry = (
            "sasl negotiation failure" in low_stdout or "gss initiate failed" in low_stdout
        ) and "principal=" not in HIVE_JDBC_URL.lower()

        if needs_principal_retry:
            retry_jdbc_url = self._build_retry_jdbc_url_with_principal(HIVE_JDBC_URL)
            self.log.warning(
                "Beeline auth failed with current JDBC URL. Retrying once with principal in URL: %s",
                HIVE_JDBC_PRINCIPAL,
            )
            retry_cmd = self._prepare_cli_cmd(retry_jdbc_url)
            retry_cmd.extend(["-e", hql])
            rc_retry, stdout_retry = self._run_beeline_cmd(retry_cmd, proc_env, verbose)
            if rc_retry == 0:
                return stdout_retry
            stdout = stdout + "\n\n[Retry with principal failed]\n" + stdout_retry

        raise AirflowException(stdout)

        


class HiveOperatorFixedJdbc(HiveOperator):
    @cached_property
    def hook(self) -> HiveCliHook:
        return HiveCliHookFixedJdbc(
            hive_cli_conn_id=self.hive_cli_conn_id,
            mapred_queue=self.mapred_queue,
            mapred_queue_priority=self.mapred_queue_priority,
            mapred_job_name=self.mapred_job_name,
            hive_cli_params=self.hive_cli_params,
            auth=self.auth,
            proxy_user=self.proxy_user,
        )


with DAG(
    dag_id=DAG_ID,
    description="Hive smoke test: kinit -> create table -> insert -> query and print",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": RUN_USER, "retries": 0},
    tags=["hive", "smoke-test", "GDTET_GLOBAL_DAG"],
) as dag:
    kinit_env = {
        "RUN_USER": RUN_USER,
        "HOME": USER_HOME,
        "KERBEROS_PRINCIPAL": KERBEROS_PRINCIPAL,
        "KEYTAB_PATH": KEYTAB_PATH,
        "HIVE_LOCAL_SCRATCHDIR": HIVE_LOCAL_SCRATCHDIR,
        "BEELINE_BIN": BEELINE_BIN,
        "BASH_INIT_FILE": BASH_INIT_FILE,
        "JAVA_HOME": BEELINE_JAVA_HOME,
        "TMPDIR": AIRFLOW_TMP_DIR,
        "TMP": AIRFLOW_TMP_DIR,
        "TEMP": AIRFLOW_TMP_DIR,
    }

    source_user_bash_profile = BashOperator(
        task_id="source_user_bash_profile",
        queue=TASK_QUEUE,
        env=kinit_env,
        bash_command="""
        set -euo pipefail
        if [ -f "${BASH_INIT_FILE}" ]; then
          source "${BASH_INIT_FILE}"
          echo "Sourced ${BASH_INIT_FILE}"
        else
          echo "Bash init file not found: ${BASH_INIT_FILE}"
        fi
        home_dir="${HOME:-}"
        if [ -n "${home_dir}" ] && [ -f "${home_dir}/.bashrc" ] && [ "${BASH_INIT_FILE}" != "${home_dir}/.bashrc" ]; then
          source "${home_dir}/.bashrc"
          echo "Sourced ${home_dir}/.bashrc"
        fi
        which java || true
        java -version || true
        command -v "${BEELINE_BIN:-beeline}" || true
        """,
    )

    print_runtime_context = BashOperator(
        task_id="print_runtime_context",
        queue=TASK_QUEUE,
        env=kinit_env,
        bash_command="""
        echo "Run user: ${RUN_USER}"
        echo "Airflow env: {{ params.airflow_env }}"
        echo "Airflow queue: {{ params.task_queue }}"
        echo "Hive conn id: {{ params.hive_cli_conn_id }}"
        echo "Hive JDBC URL: {{ params.hive_jdbc_url }}"
        echo "Hive JDBC principal: {{ params.hive_jdbc_principal }}"
        echo "hive.exec.local.scratchdir: {{ params.hive_local_scratchdir }}"
        echo "BEELINE_BIN: {{ params.beeline_bin }}"
        echo "BASH_INIT_FILE: {{ params.bash_init_file }}"
        echo "JAVA_HOME: {{ params.java_home }}"
        echo "Kerberos principal: ${KERBEROS_PRINCIPAL}"
        echo "Keytab: ${KEYTAB_PATH}"
        """,
        params={
            "task_queue": TASK_QUEUE,
            "airflow_env": AIRFLOW_ENV,
            "hive_cli_conn_id": HIVE_CLI_CONN_ID,
            "hive_jdbc_url": HIVE_JDBC_URL,
            "hive_jdbc_principal": HIVE_JDBC_PRINCIPAL,
            "hive_local_scratchdir": HIVE_LOCAL_SCRATCHDIR,
            "beeline_bin": BEELINE_BIN,
            "bash_init_file": BASH_INIT_FILE,
            "java_home": BEELINE_JAVA_HOME,
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

    check_java_for_beeline = BashOperator(
        task_id="check_java_for_beeline",
        queue=TASK_QUEUE,
        env=kinit_env,
        bash_command="""
        set -euo pipefail
        echo "BEELINE_BIN=${BEELINE_BIN:-beeline}"
        command -v "${BEELINE_BIN:-beeline}"
        echo "JAVA_HOME=${JAVA_HOME:-}"
        which java
        java -version

        version="$(java -version 2>&1 | awk -F'"' '/version/ {print $2; exit}')"
        major="$(echo "${version}" | awk -F. '{if ($1 == 1) print $2; else print $1}')"
        echo "Detected Java major version: ${major}"
        if [ "${major}" -lt 11 ]; then
          echo "Java ${major} is too old for current beeline/spark libs. Please use Java 11+." >&2
          exit 1
        fi
        """,
    )

    create_table = HiveOperatorFixedJdbc(
        task_id="create_hive_test_table",
        queue=TASK_QUEUE,
        hive_cli_conn_id=HIVE_CLI_CONN_ID,
        hive_cli_params=HIVE_CLI_PARAMS,
        schema="default",
        hql=CREATE_TABLE_SQL,
    )

    insert_rows = HiveOperatorFixedJdbc(
        task_id="insert_hive_test_data",
        queue=TASK_QUEUE,
        hive_cli_conn_id=HIVE_CLI_CONN_ID,
        hive_cli_params=HIVE_CLI_PARAMS,
        schema="default",
        hql=INSERT_SQL,
    )

    query_and_print = HiveOperatorFixedJdbc(
        task_id="query_and_print_hive_data",
        queue=TASK_QUEUE,
        hive_cli_conn_id=HIVE_CLI_CONN_ID,
        hive_cli_params=HIVE_CLI_PARAMS,
        schema="default",
        hql=SELECT_SQL,
    )

    source_user_bash_profile >> print_runtime_context
    print_runtime_context >> check_tmpdir >> check_keytab_file >> run_kinit >> show_klist
    show_klist >> check_java_for_beeline >> create_table >> insert_rows >> query_and_print
