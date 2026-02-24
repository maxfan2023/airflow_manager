#!/usr/bin/env bash
set -euo pipefail

# This helper script creates local users for quick RBAC verification.
# It intentionally uses fixed usernames so repeated runs stay idempotent.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults; override with CLI flags.
ENV_NAME="dev"
CONFIG_FILE=""
PASSWORD=""
DRY_RUN=false

CONDA_ACTIVATED=false

usage() {
  cat <<'EOF'
Usage:
  create_test_users.sh [options]

Options:
  -e, --env <dev|uat|prod>    Select environment config (default: dev)
  -c, --config <path>         Path to airflow-manager config file
  -p, --password <text>       Password for all test users (required)
  -n, --dry-run               Print commands only
  -h, --help                  Show this help

Creates 4 local users for RBAC testing:
  rbac_normal      -> Viewer + AF_RERUN_ALL_NO_TRIGGER
  rbac_us_user     -> Viewer + AF_TRIGGER_SCOPE_US
  rbac_nonus_priv  -> Viewer + AF_TRIGGER_SCOPE_GLOBAL
  rbac_us_priv     -> Viewer + AF_TRIGGER_SCOPE_US + AF_TRIGGER_SCOPE_GLOBAL
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  printf '[%s] [ERROR] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
  exit 1
}

run_cmd() {
  # Central execution wrapper to support --dry-run safely.
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
  # Keep shell environment clean after script finishes.
  if [[ "$CONDA_ACTIVATED" == "true" ]]; then
    conda deactivate >/dev/null 2>&1 || true
  fi
}

parse_args() {
  # Parse required/optional arguments.
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
      -p|--password)
        PASSWORD="${2:-}"
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
        die "Unknown argument: $1"
        ;;
    esac
  done
}

load_config() {
  # Reuse same env config as airflow-manager scripts.
  if [[ -z "$CONFIG_FILE" ]]; then
    CONFIG_FILE="${REPO_ROOT}/conf/airflow-manager-${ENV_NAME}.conf"
  fi
  [[ -f "$CONFIG_FILE" ]] || die "Config file not found: $CONFIG_FILE"
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"

  # Keep user-management CLI behavior aligned with AIRFLOW_HOME local settings.
  if [[ -n "${AIRFLOW_HOME:-}" ]]; then
    case ":${PYTHONPATH:-}:" in
      *":${AIRFLOW_HOME}:"*) ;;
      *) export PYTHONPATH="${AIRFLOW_HOME}:${PYTHONPATH:-}" ;;
    esac
  fi
}

activate_conda_if_configured() {
  # Airflow CLI must be available in active Python environment.
  if [[ -z "${CONDA_BASE:-}" || -z "${CONDA_ENV_NAME:-}" ]]; then
    return 0
  fi

  if [[ "${CONDA_DEFAULT_ENV:-}" == "$CONDA_ENV_NAME" ]]; then
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
    return 0
  fi

  if [[ -f "$CONDA_BASE/bin/activate" ]]; then
    # shellcheck disable=SC1090
    source "$CONDA_BASE/bin/activate" "$CONDA_ENV_NAME"
    CONDA_ACTIVATED=true
    return 0
  fi

  die "Cannot find conda activation scripts under CONDA_BASE=$CONDA_BASE"
}

user_exists() {
  # Query by username from Airflow user table.
  local username="$1"
  airflow users list -o plain | awk 'NR>1 {print $1}' | grep -Fxq "$username"
}

create_user_if_missing() {
  local username="$1"
  local firstname="$2"
  local lastname="$3"
  local email="$4"

  # Keep runs repeatable: skip create if user already exists.
  if user_exists "$username"; then
    log "User exists: $username"
    return 0
  fi

  run_cmd airflow users create \
    --username "$username" \
    --firstname "$firstname" \
    --lastname "$lastname" \
    --email "$email" \
    --role "Viewer" \
    --password "$PASSWORD"
}

grant_user_role() {
  # Role grants are additive and idempotent in Airflow CLI.
  local username="$1"
  local role="$2"
  run_cmd airflow users add-role -u "$username" -r "$role"
}

main() {
  # Ensure conda env deactivation no matter how script exits.
  trap deactivate_conda_if_needed EXIT

  parse_args "$@"
  [[ -n "$PASSWORD" ]] || die "--password is required."
  load_config
  activate_conda_if_configured
  command -v airflow >/dev/null 2>&1 || die "airflow command not found in PATH."

  # 1) Normal user: can view + rerun, cannot trigger new DagRun.
  create_user_if_missing "rbac_normal" "RBAC" "Normal" "rbac_normal@example.local"
  grant_user_role "rbac_normal" "AF_RERUN_ALL_NO_TRIGGER"

  # 2) US user: can trigger only US-scoped DAGs.
  create_user_if_missing "rbac_us_user" "RBAC" "USUser" "rbac_us_user@example.local"
  grant_user_role "rbac_us_user" "AF_TRIGGER_SCOPE_US"

  # 3) Non-US privileged: can trigger global DAGs.
  create_user_if_missing "rbac_nonus_priv" "RBAC" "NonUSPriv" "rbac_nonus_priv@example.local"
  grant_user_role "rbac_nonus_priv" "AF_TRIGGER_SCOPE_GLOBAL"

  # 4) US privileged: can trigger both US and global DAG sets.
  create_user_if_missing "rbac_us_priv" "RBAC" "USPriv" "rbac_us_priv@example.local"
  grant_user_role "rbac_us_priv" "AF_TRIGGER_SCOPE_US"
  grant_user_role "rbac_us_priv" "AF_TRIGGER_SCOPE_GLOBAL"

  log "Done. Verify with: airflow users list -o plain"
}

main "$@"
