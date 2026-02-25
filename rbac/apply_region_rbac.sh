#!/usr/bin/env bash
set -euo pipefail

# This script initializes RBAC roles for folder-based DAG scope control.
# It no longer assigns DAG:<dag_id> permissions directly.
# DAG-level grants are delegated to airflow_local_settings.py dag_policy.
if [[ -z "${BASH_VERSINFO:-}" || "${BASH_VERSINFO[0]}" -lt 4 ]]; then
  echo "ERROR: apply_region_rbac.sh requires bash >= 4 (RHEL8 default is supported)." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_NAME="dev"
CONFIG_FILE=""
SCOPES_CSV="global,us"
DRY_RUN=false

RERUN_ROLE="AF_RERUN_ALL_NO_TRIGGER"
CONDA_ACTIVATED=false
declare -A SCOPE_SET=()

usage() {
  cat <<'USAGE'
Usage:
  apply_region_rbac.sh [options]

Options:
  -e, --env <dev|uat|prod>    Select environment config (default: dev)
  -c, --config <path>         Path to airflow-manager config file
  -s, --scopes <csv>          Scope folders to manage (default: global,us)
  -n, --dry-run               Print commands only
  -h, --help                  Show this help

Behavior:
  1) Creates/updates baseline roles:
     - AF_RERUN_ALL_NO_TRIGGER
     - AF_TRIGGER_SCOPE_<SCOPE> for each scope in --scopes
  2) Grants baseline permissions required by each role.
  3) Does NOT assign DAG:<dag_id> directly; dag_policy handles that.
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
      -s|--scopes)
        SCOPES_CSV="${2:-}"
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
  if [[ -z "$CONFIG_FILE" ]]; then
    CONFIG_FILE="${REPO_ROOT}/conf/airflow-manager-${ENV_NAME}.conf"
  fi
  [[ -f "$CONFIG_FILE" ]] || die "Config file not found: $CONFIG_FILE"

  # shellcheck disable=SC1090
  source "$CONFIG_FILE"

  # Ensure airflow_local_settings.py under AIRFLOW_HOME is importable for
  # sync-perm and other CLI paths that evaluate DAG-level policies.
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
    die "core.auth_manager is '$auth_manager'. Please switch to FabAuthManager before running RBAC setup."
  fi
}

normalize_scopes() {
  local normalized
  normalized="$(tr '[:upper:]' '[:lower:]' <<<"$SCOPES_CSV" | tr -d '[:space:]')"
  [[ -n "$normalized" ]] || die "--scopes cannot be empty."

  local scope
  IFS=',' read -r -a _scopes <<<"$normalized"
  for scope in "${_scopes[@]}"; do
    [[ -n "$scope" ]] && SCOPE_SET["$scope"]=1
  done

  # Always include global scope so DAGs in dags/global are consistently supported.
  SCOPE_SET["global"]=1

  [[ "${#SCOPE_SET[@]}" -gt 0 ]] || die "No valid scopes parsed from --scopes."
}

scope_role_name() {
  local scope="$1"
  printf 'AF_TRIGGER_SCOPE_%s\n' "${scope^^}"
}

role_exists() {
  local role="$1"
  airflow roles list -o plain | awk 'NR>1 {print $1}' | grep -Fxq "$role"
}

create_role_if_missing() {
  local role="$1"
  if role_exists "$role"; then
    log "Role exists: $role"
  else
    run_cmd airflow roles create "$role"
  fi
}

grant_role_perm() {
  local role="$1"
  local action="$2"
  local resource="$3"
  run_cmd airflow roles add-perms "$role" -a "$action" -r "$resource"
}

sync_permissions() {
  if airflow sync-perm --help 2>/dev/null | grep -q -- "--include-dags"; then
    run_cmd airflow sync-perm --include-dags
  else
    run_cmd airflow sync-perm
  fi
}

configure_roles() {
  local scope role

  # Rerun-only capability role.
  create_role_if_missing "$RERUN_ROLE"

  # Scope trigger roles. Example: AF_TRIGGER_SCOPE_GLOBAL, AF_TRIGGER_SCOPE_US
  for scope in "${!SCOPE_SET[@]}"; do
    role="$(scope_role_name "$scope")"
    create_role_if_missing "$role"
  done

  # Rerun-only role: clear/retry but no DagRun create.
  # Caveat: Dags.can_edit also allows pause/unpause in standard FAB RBAC.
  grant_role_perm "$RERUN_ROLE" "can_edit" "DAGs"
  grant_role_perm "$RERUN_ROLE" "can_edit" "DAG Runs"
  grant_role_perm "$RERUN_ROLE" "can_edit" "Task Instances"
  grant_role_perm "$RERUN_ROLE" "can_delete" "Task Instances"

  # Trigger scope roles: create DagRun permission.
  # DAG:<dag_id>.can_edit is injected by dag_policy via dag.access_control.
  for scope in "${!SCOPE_SET[@]}"; do
    role="$(scope_role_name "$scope")"
    grant_role_perm "$role" "can_create" "DAG Runs"
  done
}

print_summary() {
  local scope
  cat <<'EOT'

Setup complete.

Next required step:
  1) Put airflow_local_settings.py into AIRFLOW_HOME.
  2) Organize DAG files by folders under dags/, e.g. dags/us, dags/global, dags/mx.
  3) Restart scheduler/webserver and run airflow sync-perm.

User-role examples:
  - normal user: Viewer + AF_RERUN_ALL_NO_TRIGGER
  - US user: Viewer + AF_TRIGGER_SCOPE_US + AF_RERUN_ALL_NO_TRIGGER
  - non-US privileged: Viewer + AF_TRIGGER_SCOPE_GLOBAL + AF_RERUN_ALL_NO_TRIGGER
  - US privileged: Viewer + AF_TRIGGER_SCOPE_GLOBAL + AF_TRIGGER_SCOPE_US + AF_RERUN_ALL_NO_TRIGGER
EOT

  log "Managed scope roles created in this run:"
  for scope in "${!SCOPE_SET[@]}"; do
    log "  - $(scope_role_name "$scope")"
  done
}

main() {
  trap deactivate_conda_if_needed EXIT

  parse_args "$@"
  load_config
  activate_conda_if_configured
  check_prereqs
  normalize_scopes

  log "Initializing folder-based RBAC roles for scopes: $(IFS=','; echo "${!SCOPE_SET[*]}")"
  sync_permissions
  configure_roles
  sync_permissions
  print_summary
}

main "$@"
