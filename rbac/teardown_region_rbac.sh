#!/usr/bin/env bash
set -euo pipefail

# Remove folder-based RBAC test users/roles safely and idempotently.
# This script is the rollback companion for apply_region_rbac.sh + create_test_users.sh.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_NAME="dev"
CONFIG_FILE=""
DRY_RUN=false
DISABLE_POLICY=false

CONDA_ACTIVATED=false

TEST_USERS=(
  "rbac_normal"
  "rbac_us_user"
  "rbac_nonus_priv"
  "rbac_us_priv"
)

CUSTOM_ROLES=(
  "AF_RERUN_ALL_NO_TRIGGER"
  "AF_TRIGGER_SCOPE_GLOBAL"
  "AF_TRIGGER_SCOPE_NONUS"
  "AF_TRIGGER_SCOPE_US"
  "AF_TRIGGER_SCOPE_MX"
  "AF_TRIGGER_SCOPE_CN"
)

usage() {
  cat <<'USAGE'
Usage:
  teardown_region_rbac.sh [options]

Options:
  -e, --env <dev|uat|prod>    Select environment config (default: dev)
  -c, --config <path>         Path to airflow-manager config file
  -d, --disable-policy        Move AIRFLOW_HOME/airflow_local_settings.py to .bak.<timestamp>
  -n, --dry-run               Print commands only
  -h, --help                  Show this help

Behavior:
  1) Delete RBAC test users (if present).
  2) Delete custom RBAC roles (if present).
  3) Sync permissions.
  4) Optionally disable folder-based dag_policy file.

Examples:
  ./rbac/teardown_region_rbac.sh --env dev --config /home/max/development/airflow/conf/airflow-manager-dev.conf
  ./rbac/teardown_region_rbac.sh --env prod --disable-policy
USAGE
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  printf '[%s] [ERROR] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
  exit 1
}

run_cmd() {
  # Single execution gateway to keep dry-run behavior consistent.
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

deactivate_conda_if_needed() {
  if [[ "$CONDA_ACTIVATED" == "true" ]]; then
    conda deactivate >/dev/null 2>&1 || true
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -e|--env)
        ENV_NAME="$(tr '[:upper:]' '[:lower:]' <<<"${2:-}")"
        shift 2
        ;;
      -c|--config)
        CONFIG_FILE="${2:-}"
        shift 2
        ;;
      -d|--disable-policy)
        DISABLE_POLICY=true
        shift
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
        die "Unknown argument: $1"
        ;;
    esac
  done
}

load_config() {
  if [[ -z "$CONFIG_FILE" ]]; then
    CONFIG_FILE="${REPO_ROOT}/conf/airflow-manager-${ENV_NAME}.conf"
  fi
  [[ -f "$CONFIG_FILE" ]] || die "Config file not found: $CONFIG_FILE"

  # shellcheck disable=SC1090
  source "$CONFIG_FILE"

  # Ensure airflow_local_settings.py under AIRFLOW_HOME is importable for CLI paths.
  if [[ -n "${AIRFLOW_HOME:-}" ]]; then
    case ":${PYTHONPATH:-}:" in
      *":${AIRFLOW_HOME}:"*) ;;
      *) export PYTHONPATH="${AIRFLOW_HOME}:${PYTHONPATH:-}" ;;
    esac
  fi
}

activate_conda_if_configured() {
  if [[ -z "${CONDA_BASE:-}" || -z "${CONDA_ENV_NAME:-}" ]]; then
    log "Skip conda activation (CONDA_BASE / CONDA_ENV_NAME not configured)."
    return 0
  fi

  if [[ "${CONDA_DEFAULT_ENV:-}" == "$CONDA_ENV_NAME" ]]; then
    log "Conda env already active: $CONDA_ENV_NAME"
    return 0
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    log "Dry-run: skip conda activation ($CONDA_ENV_NAME)."
    return 0
  fi

  if [[ -f "$CONDA_BASE/etc/profile.d/conda.sh" ]]; then
    # shellcheck disable=SC1090
    source "$CONDA_BASE/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"
    CONDA_ACTIVATED=true
    log "Activated conda env: $CONDA_ENV_NAME"
    return 0
  fi

  if [[ -f "$CONDA_BASE/bin/activate" ]]; then
    # shellcheck disable=SC1090
    source "$CONDA_BASE/bin/activate" "$CONDA_ENV_NAME"
    CONDA_ACTIVATED=true
    log "Activated conda env: $CONDA_ENV_NAME"
    return 0
  fi

  die "Cannot find conda activation scripts under CONDA_BASE=$CONDA_BASE"
}

check_prereqs() {
  command -v airflow >/dev/null 2>&1 || die "airflow command not found in PATH."

  local auth_manager
  auth_manager="$(airflow config get-value core auth_manager 2>/dev/null || true)"
  if [[ "$auth_manager" != *"fab_auth_manager"* ]]; then
    die "core.auth_manager is '$auth_manager'. Please switch to FabAuthManager before RBAC teardown."
  fi
}

user_exists() {
  # In dry-run mode, skip DB calls and print delete plan for all test users.
  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi
  local username="$1"
  # Airflow 3 plain output starts with numeric ID column; older formats may not.
  airflow users list -o plain \
    | awk 'NR>1 {if ($1 ~ /^[0-9]+$/ && $2 != "") print $2; else print $1}' \
    | grep -Fxq "$username"
}

role_exists() {
  # In dry-run mode, skip DB calls and print delete plan for all custom roles.
  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi
  local role="$1"
  airflow roles list -o plain | awk 'NR>1 {print $1}' | grep -Fxq "$role"
}

delete_user_if_exists() {
  local username="$1"
  if user_exists "$username"; then
    run_cmd airflow users delete -u "$username"
  else
    log "User not found, skip: $username"
  fi
}

delete_role_if_exists() {
  local role="$1"
  if role_exists "$role"; then
    run_cmd airflow roles delete "$role"
  else
    log "Role not found, skip: $role"
  fi
}

sync_permissions() {
  if airflow sync-perm --help 2>/dev/null | grep -q -- "--include-dags"; then
    run_cmd airflow sync-perm --include-dags
  else
    run_cmd airflow sync-perm
  fi
}

disable_policy_file_if_requested() {
  if [[ "$DISABLE_POLICY" != "true" ]]; then
    return 0
  fi

  [[ -n "${AIRFLOW_HOME:-}" ]] || die "AIRFLOW_HOME is not set; cannot disable policy file."

  local policy_file="${AIRFLOW_HOME}/airflow_local_settings.py"
  if [[ -f "$policy_file" ]]; then
    local ts
    ts="$(date '+%Y%m%d%H%M%S')"
    run_cmd mv "$policy_file" "${policy_file}.bak.${ts}"
    log "Disabled policy file: ${policy_file}.bak.${ts}"
  else
    log "Policy file not found, skip disable: $policy_file"
  fi
}

teardown_users() {
  local username
  for username in "${TEST_USERS[@]}"; do
    delete_user_if_exists "$username"
  done
}

teardown_roles() {
  local role
  for role in "${CUSTOM_ROLES[@]}"; do
    delete_role_if_exists "$role"
  done
}

print_summary() {
  cat <<'EOT'

Teardown complete.

Recommended follow-up:
  1) Restart scheduler + dag-processor + api-server.
  2) Verify users/roles are removed from Airflow UI/CLI.
EOT
}

main() {
  trap deactivate_conda_if_needed EXIT

  parse_args "$@"
  load_config
  activate_conda_if_configured
  check_prereqs

  log "Starting RBAC teardown for env=$ENV_NAME"
  teardown_users
  teardown_roles
  disable_policy_file_if_requested
  sync_permissions
  print_summary
}

main "$@"
