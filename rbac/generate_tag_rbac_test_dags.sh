#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

OUTPUT_DIR="${REPO_ROOT}/dags/rbac_tag_tests"
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
  -o, --output-dir <path>     Output directory for generated DAG files
  -n, --dry-run               Print plan only
  -h, --help                  Show this help

This script generates 5 DAG files for tag-based RBAC verification:
  1) source=camp-us -> GDTET_US_DAG
  2) source=mdm     -> GDTET_US_DAG
  3) source=abc     -> GDTET_GLOBAL_DAG
  4) source=hub     -> GDTET_GLOBAL_DAG
  5) no source var  -> no classification tag (illegal DAG by policy)
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

write_dag_file() {
  local index="$1"
  local name_suffix="$2"
  local source_value="$3"
  local dag_id="rbac_tag_test_${index}_${name_suffix}"
  local file_path="${OUTPUT_DIR}/${dag_id}.py"
  local classification_tag
  local classification_label
  local source_line=""
  local tags_line=""

  classification_tag="$(classification_tag_from_source "$source_value")"
  classification_label="$(classification_label_from_tag "$classification_tag")"

  if [[ "$source_value" != "$NO_SOURCE_SENTINEL" ]]; then
    source_line="source = \"${source_value}\""
  fi

  if [[ -n "$classification_tag" ]]; then
    tags_line="    tags=[\"${classification_tag}\"],"
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    log "Plan: ${file_path} (${classification_label}, source=${source_value})"
    return 0
  fi

  {
    echo "from __future__ import annotations"
    echo
    echo "from datetime import datetime"
    echo
    echo "from airflow import DAG"
    echo "from airflow.operators.empty import EmptyOperator"
    echo
    if [[ -n "$source_line" ]]; then
      echo "$source_line"
      echo
    fi
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
    echo "    EmptyOperator(task_id=\"start\")"
  } >"$file_path"

  log "Created ${file_path} (${classification_label}, source=${source_value})"
}

main() {
  parse_args "$@"

  run_cmd mkdir -p "$OUTPUT_DIR"

  # 1) source=camp-us -> US
  write_dag_file "1" "camp_us" "camp-us"
  # 2) source=mdm -> US
  write_dag_file "2" "mdm" "mdm"
  # 3) source=abc -> global
  write_dag_file "3" "abc" "abc"
  # 4) source=hub -> global
  write_dag_file "4" "hub" "hub"
  # 5) no source variable -> illegal DAG (no classification tag)
  write_dag_file "5" "no_source" "$NO_SOURCE_SENTINEL"

  log "Done. Generated test DAG files in: ${OUTPUT_DIR}"
}

main "$@"
