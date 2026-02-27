#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -n "${AIRFLOW_HOME:-}" ]]; then
  OUTPUT_DIR="${AIRFLOW_HOME}/dags"
else
  OUTPUT_DIR="${REPO_ROOT}/dags"
fi
DRY_RUN=false

US_SOURCE_VALUES=(
  "camp-us"
  "ucm"
  "norkom"
  "mdm"
)

TAG_US="GDTET_US_DAG"
TAG_GLOBAL="GDTET_GLOBAL_DAG"
NO_SOURCE_SENTINEL="__NO_SOURCE__"

usage() {
  cat <<'USAGE'
Usage:
  generate_tag_rbac_test_dags.sh [options]

Options:
  -o, --output-dir <path>     Base dags directory (default: $AIRFLOW_HOME/dags or <repo>/dags)
  -n, --dry-run               Print plan only
  -h, --help                  Show this help

This script generates 5 DAG files for tag-based RBAC verification with fixed paths:
  1) rbac_tag_test_1_camp_us.py   -> <base>/camp_us/ (source=camp-us, US)
  2) rbac_tag_test_2_mdm.py       -> <base>/mdm/     (source=mdm, US)
  3) rbac_tag_test_3_abc.py       -> <base>/global/  (source=abc, GLOBAL)
  4) rbac_tag_test_4_hub.py       -> <base>/global/  (source=hub, GLOBAL)
  5) rbac_tag_test_5_no_source.py -> <base>/         (no source, illegal DAG by policy)
USAGE
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

run_cmd() {
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[DRY-RUN] %q' "$1"
    shift
    for arg in "$@"; do
      printf ' %q' "$arg"
    done
    printf '\n'
    return 0
  fi
  "$@"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -o|--output-dir)
        OUTPUT_DIR="${2:-}"
        shift 2
        ;;
      -n|--dry-run)
        DRY_RUN=true
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        usage
        echo "ERROR: unknown argument: $1" >&2
        exit 1
        ;;
    esac
  done
}

is_us_source() {
  local source_value="$1"
  local us_source
  for us_source in "${US_SOURCE_VALUES[@]}"; do
    if [[ "$source_value" == "$us_source" ]]; then
      return 0
    fi
  done
  return 1
}

classification_tag_from_source() {
  local source_value="$1"
  if [[ "$source_value" == "$NO_SOURCE_SENTINEL" ]]; then
    printf ''
    return 0
  fi
  if is_us_source "$source_value"; then
    printf '%s' "$TAG_US"
  else
    printf '%s' "$TAG_GLOBAL"
  fi
}

classification_label_from_tag() {
  local tag="$1"
  if [[ "$tag" == "$TAG_US" ]]; then
    printf 'us_dag'
  elif [[ "$tag" == "$TAG_GLOBAL" ]]; then
    printf 'global_dag'
  else
    printf 'illegal_dag'
  fi
}

legacy_file_path() {
  local dag_id="$1"
  printf '%s/rbac_tag_tests/%s.py' "$OUTPUT_DIR" "$dag_id"
}

write_dag_file() {
  local index="$1"
  local name_suffix="$2"
  local source_value="$3"
  local relative_dir="$4"
  local dag_id="rbac_tag_test_${index}_${name_suffix}"
  local target_dir="$OUTPUT_DIR"
  local legacy_path
  local classification_tag
  local classification_label
  local source_line=""
  local tags_line=""

  if [[ -n "$relative_dir" ]]; then
    target_dir="${OUTPUT_DIR}/${relative_dir}"
  fi

  local file_path="${target_dir}/${dag_id}.py"
  classification_tag="$(classification_tag_from_source "$source_value")"
  classification_label="$(classification_label_from_tag "$classification_tag")"
  legacy_path="$(legacy_file_path "$dag_id")"

  if [[ "$source_value" != "$NO_SOURCE_SENTINEL" ]]; then
    source_line="source = \"${source_value}\""
  fi

  if [[ -n "$classification_tag" ]]; then
    tags_line="    tags=[\"${classification_tag}\"],"
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    log "Plan: ${file_path} (${classification_label}, source=${source_value})"
    if [[ "$legacy_path" != "$file_path" && -f "$legacy_path" ]]; then
      log "Plan: remove legacy file ${legacy_path}"
    fi
    return 0
  fi

  run_cmd mkdir -p "$target_dir"

  {
    echo "from __future__ import annotations"
    echo
    echo "from datetime import datetime"
    echo
    echo "from airflow.providers.standard.operators.bash import BashOperator"
    echo "from airflow.providers.standard.operators.python import PythonOperator"
    echo "from airflow.sdk import DAG"
    echo
    if [[ -n "$source_line" ]]; then
      echo "$source_line"
      echo
    fi
    echo "def _print_python_message(dag_id: str) -> None:"
    echo "    print(f\"python task executed in {dag_id}\")"
    echo
    echo "with DAG("
    echo "    dag_id=\"${dag_id}\","
    echo "    description=\"Generated DAG for tag-based RBAC testing\","
    echo "    start_date=datetime(2024, 1, 1),"
    echo "    schedule=None,"
    echo "    catchup=False,"
    if [[ -n "$tags_line" ]]; then
      echo "$tags_line"
    fi
    echo ") as dag:"
    echo "    bash_echo = BashOperator("
    echo "        task_id=\"bash_echo\","
    echo "        bash_command=\"echo 'bash task executed for ${dag_id}'\","
    echo "    )"
    echo
    echo "    python_echo = PythonOperator("
    echo "        task_id=\"python_echo\","
    echo "        python_callable=_print_python_message,"
    echo "        op_kwargs={\"dag_id\": \"${dag_id}\"},"
    echo "    )"
    echo
    echo "    sleep_task = BashOperator("
    echo "        task_id=\"sleep_task\","
    echo "        bash_command=\"sleep 5\","
    echo "    )"
    echo
    echo "    bash_echo >> python_echo >> sleep_task"
  } >"$file_path"

  if [[ "$legacy_path" != "$file_path" && -f "$legacy_path" ]]; then
    run_cmd rm -f "$legacy_path"
    log "Removed legacy file ${legacy_path}"
  fi

  log "Created ${file_path} (${classification_label}, source=${source_value})"
}

main() {
  parse_args "$@"

  run_cmd mkdir -p "$OUTPUT_DIR"

  # 1) source=camp-us -> US
  write_dag_file "1" "camp_us" "camp-us" "camp_us"
  # 2) source=mdm -> US
  write_dag_file "2" "mdm" "mdm" "mdm"
  # 3) source=abc -> global
  write_dag_file "3" "abc" "abc" "global"
  # 4) source=hub -> global
  write_dag_file "4" "hub" "hub" "global"
  # 5) no source variable -> illegal DAG (no classification tag)
  write_dag_file "5" "no_source" "$NO_SOURCE_SENTINEL" ""

  log "Done. Generated test DAG files under base dir: ${OUTPUT_DIR}"
}

main "$@"
