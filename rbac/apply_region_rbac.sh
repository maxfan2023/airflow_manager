#!/usr/bin/env bash
set -euo pipefail

# This script builds a region-aware RBAC model on top of Airflow FAB auth manager.
# Design goals:
# 1) Keep trigger power scoped by region (US / NON-US / future MX,CN...).
# 2) Support rerun-only users (clear/retry) without DagRun create permission.
# 3) Be safely re-runnable: remove stale DAG-level grants, then re-apply current mapping.
if [[ -z "${BASH_VERSINFO:-}" || "${BASH_VERSINFO[0]}" -lt 4 ]]; then
  echo "ERROR: apply_region_rbac.sh requires bash >= 4 (RHEL8 default is supported)." >&2
  exit 1
fi

# Resolve script paths to support running from any working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults; can be overridden by CLI flags.
ENV_NAME="dev"
CONFIG_FILE=""
RESTRICTED_SCOPES="us"
DAG_SCOPE_FILE=""
DAG_SCOPE_SEPARATOR="__"
DRY_RUN=false

# Canonical role names used by this script.
RERUN_ROLE="AF_RERUN_ALL_NO_TRIGGER"
NONUS_TRIGGER_ROLE="AF_TRIGGER_SCOPE_NONUS"

CONDA_ACTIVATED=false
# Map of dag_id -> scope loaded from optional CSV overrides.
declare -A DAG_SCOPE_OVERRIDES=()
# Set-like map of restricted scopes (us,mx,cn...).
declare -A RESTRICTED_SCOPE_SET=()

usage() {
  cat <<'EOF'
Usage:
  apply_region_rbac.sh [options]

Options:
  -e, --env <dev|uat|prod>           Select environment config (default: dev)
  -c, --config <path>                Path to airflow-manager config file
  -s, --restricted-scopes <csv>      Restricted scopes, e.g. "us,mx,cn" (default: us)
  -f, --dag-scope-file <path>        Optional CSV: dag_id,scope (for non-prefix DAG ids)
      --scope-separator <text>       DAG scope separator (default: __)
  -n, --dry-run                      Print commands only
  -h, --help                         Show this help

Behavior:
  1) Creates / updates roles:
     - AF_RERUN_ALL_NO_TRIGGER
     - AF_TRIGGER_SCOPE_NONUS
     - AF_TRIGGER_SCOPE_<SCOPE> for each scope in --restricted-scopes
  2) Grants baseline permissions.
  3) Classifies DAGs by "<scope><separator><name>" or dag-scope-file overrides.
  4) Grants DAG:<dag_id>.can_edit to scoped trigger roles.
EOF
}

trim() {
  local v="$1"
  v="${v#"${v%%[![:space:]]*}"}"
  v="${v%"${v##*[![:space:]]}"}"
  printf '%s' "$v"
}

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

warn() {
  printf '[%s] [WARN] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
}

die() {
  printf '[%s] [ERROR] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
  exit 1
}

run_cmd() {
  # Single execution gateway. In dry-run mode we print exact commands for audit.
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
  # Avoid leaking conda activation into caller shell/session.
  if [[ "$CONDA_ACTIVATED" == "true" ]]; then
    conda deactivate >/dev/null 2>&1 || true
  fi
}

parse_args() {
  # Minimal flag parser (long + short flags) with sane defaults.
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
      -s|--restricted-scopes)
        RESTRICTED_SCOPES="${2:-}"
        shift 2
        ;;
      -f|--dag-scope-file)
        DAG_SCOPE_FILE="${2:-}"
        shift 2
        ;;
      --scope-separator)
        DAG_SCOPE_SEPARATOR="${2:-}"
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
  # Reuse airflow-manager env config so both service/process and RBAC scripts
  # share one source of truth (AIRFLOW_HOME, conda env name, etc.).
  if [[ -z "$CONFIG_FILE" ]]; then
    CONFIG_FILE="${REPO_ROOT}/conf/airflow-manager-${ENV_NAME}.conf"
  fi

  if [[ ! -f "$CONFIG_FILE" ]]; then
    die "Config file not found: $CONFIG_FILE"
  fi

  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
}

activate_conda_if_configured() {
  # Airflow CLI must run in the same Python environment where Airflow is installed.
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
  # RBAC commands are only valid when FAB auth manager is active.
  # If another auth manager is configured, role/resource behavior will differ.
  command -v airflow >/dev/null 2>&1 || die "airflow command not found in PATH."

  local auth_manager
  auth_manager="$(airflow config get-value core auth_manager 2>/dev/null || true)"
  if [[ "$auth_manager" != *"fab_auth_manager"* ]]; then
    die "core.auth_manager is '$auth_manager'. Please switch to FabAuthManager before running RBAC script."
  fi
}

normalize_restricted_scopes() {
  # Normalize "US, MX" -> {"us","mx"}.
  # This controls which scopes are treated as restricted.
  local normalized
  normalized="$(tr '[:upper:]' '[:lower:]' <<<"$RESTRICTED_SCOPES" | tr -d '[:space:]')"
  [[ -z "$normalized" ]] && die "--restricted-scopes cannot be empty."

  local scope
  IFS=',' read -r -a _scopes <<<"$normalized"
  for scope in "${_scopes[@]}"; do
    [[ -z "$scope" ]] && continue
    RESTRICTED_SCOPE_SET["$scope"]=1
  done

  if [[ "${#RESTRICTED_SCOPE_SET[@]}" -eq 0 ]]; then
    die "No valid restricted scopes found."
  fi
}

scope_role_name() {
  # Scope token -> role name convention.
  # us -> AF_TRIGGER_SCOPE_US
  local scope="$1"
  printf 'AF_TRIGGER_SCOPE_%s\n' "${scope^^}"
}

role_exists() {
  # Keep create idempotent by checking existing roles first.
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
  # In Airflow CLI, -r/--resource accepts multiple values, so role must come first.
  run_cmd airflow roles add-perms "$role" -a "$action" -r "$resource"
}

sync_permissions() {
  # Ensure newly discovered DAG views/resources are present before granting perms.
  # Airflow versions differ on whether --include-dags is available.
  if airflow sync-perm --help 2>/dev/null | grep -q -- "--include-dags"; then
    run_cmd airflow sync-perm --include-dags
  else
    run_cmd airflow sync-perm
  fi
}

load_scope_overrides() {
  # Optional file for legacy DAG IDs not following "<scope>__<...>" naming.
  # Format: dag_id,scope
  [[ -z "$DAG_SCOPE_FILE" ]] && return 0
  [[ -f "$DAG_SCOPE_FILE" ]] || die "DAG scope file not found: $DAG_SCOPE_FILE"

  local line dag_id scope parsed=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="$(trim "$line")"
    [[ -z "$line" ]] && continue

    IFS=',' read -r dag_id scope _rest <<<"$line"
    dag_id="$(trim "${dag_id:-}")"
    scope="$(trim "${scope:-}")"
    scope="$(tr '[:upper:]' '[:lower:]' <<<"$scope")"

    [[ -z "$dag_id" || -z "$scope" ]] && {
      warn "Skip invalid scope line: $line"
      continue
    }

    DAG_SCOPE_OVERRIDES["$dag_id"]="$scope"
    parsed=$((parsed + 1))
  done <"$DAG_SCOPE_FILE"

  log "Loaded $parsed DAG scope override(s) from $DAG_SCOPE_FILE"
}

collect_dags_json() {
  # Prefer JSON output for robust parsing.
  # Fallback to plain output in assign_dag_permissions().
  airflow dags list -o json 2>/dev/null | python -c 'import json, sys
raw = sys.stdin.read().strip()
if not raw:
    raise SystemExit(0)
rows = json.loads(raw)
for row in rows:
    if isinstance(row, dict):
        dag_id = row.get("dag_id")
        if dag_id:
            print(dag_id)'
}

collect_dags_plain() {
  airflow dags list -o plain 2>/dev/null | awk 'NR>1 && $1 != "" {print $1}'
}

scope_for_dag() {
  local dag_id="$1"

  # Priority 1: explicit CSV override.
  if [[ -n "${DAG_SCOPE_OVERRIDES[$dag_id]:-}" ]]; then
    printf '%s\n' "${DAG_SCOPE_OVERRIDES[$dag_id]}"
    return 0
  fi

  # Priority 2: parse prefix from naming convention, e.g. "us__sales__daily".
  if [[ "$dag_id" == *"$DAG_SCOPE_SEPARATOR"* ]]; then
    local prefix
    prefix="${dag_id%%"$DAG_SCOPE_SEPARATOR"*}"
    if [[ "$prefix" =~ ^[A-Za-z][A-Za-z0-9_-]*$ ]]; then
      printf '%s\n' "${prefix,,}"
      return 0
    fi
  fi

  # Default scope when nothing matches.
  printf 'global\n'
}

is_restricted_scope() {
  local scope="$1"
  [[ -n "${RESTRICTED_SCOPE_SET[$scope]:-}" ]]
}

assign_dag_permissions() {
  # Assign DAG:<dag_id>.can_edit to exactly one trigger role:
  # - restricted scope DAG -> AF_TRIGGER_SCOPE_<SCOPE>
  # - everything else      -> AF_TRIGGER_SCOPE_NONUS
  local dags_output
  local -a dag_ids=()
  local -A role_dag_counts=()

  dags_output="$(collect_dags_json || true)"
  if [[ -n "$dags_output" ]]; then
    while IFS= read -r dag_id; do
      [[ -n "$dag_id" ]] && dag_ids+=("$dag_id")
    done <<<"$dags_output"
  fi

  if [[ "${#dag_ids[@]}" -eq 0 ]]; then
    dags_output="$(collect_dags_plain || true)"
    while IFS= read -r dag_id; do
      [[ -n "$dag_id" ]] && dag_ids+=("$dag_id")
    done <<<"$dags_output"
  fi

  if [[ "${#dag_ids[@]}" -eq 0 ]]; then
    warn "No DAG IDs found. Skip DAG-level role grants."
    return 0
  fi

  log "Discovered ${#dag_ids[@]} DAG(s). Assigning DAG-level permissions..."

  local dag_id scope target_role
  for dag_id in "${dag_ids[@]}"; do
    scope="$(scope_for_dag "$dag_id")"
    if is_restricted_scope "$scope"; then
      target_role="$(scope_role_name "$scope")"
    else
      target_role="$NONUS_TRIGGER_ROLE"
    fi

    if grant_role_perm "$target_role" "can_edit" "DAG:${dag_id}"; then
      role_dag_counts["$target_role"]=$(( ${role_dag_counts["$target_role"]:-0} + 1 ))
    else
      warn "Failed to add DAG permission: role=$target_role dag_id=$dag_id"
    fi
  done

  log "DAG permission assignment summary:"
  for target_role in "${!role_dag_counts[@]}"; do
    log "  - $target_role => ${role_dag_counts[$target_role]} DAG(s)"
  done
}

cleanup_managed_dag_permissions() {
  # Critical for idempotency:
  # remove ALL existing DAG:* can_edit entries from managed trigger roles,
  # then assign_dag_permissions() re-builds grants from current rules.
  # This prevents stale grants when a DAG scope changes over time.
  local -a managed_roles=()
  local scope role resource

  managed_roles+=("$NONUS_TRIGGER_ROLE")
  for scope in "${!RESTRICTED_SCOPE_SET[@]}"; do
    managed_roles+=("$(scope_role_name "$scope")")
  done

  for role in "${managed_roles[@]}"; do
    while IFS= read -r resource; do
      [[ -z "$resource" ]] && continue
      if ! run_cmd airflow roles del-perms "$role" -a "can_edit" -r "$resource"; then
        warn "Failed to remove stale DAG permission: role=$role resource=$resource"
      fi
    done < <(
      airflow roles list -p -o plain \
        | awk -v role="$role" 'NR>1 && $1 == role && $2 ~ /^DAG:/ && $0 ~ /can_edit/ {print $2}'
    )
  done
}

configure_roles() {
  local scope

  # Create roles first; repeated runs are safe.
  create_role_if_missing "$RERUN_ROLE"
  create_role_if_missing "$NONUS_TRIGGER_ROLE"
  for scope in "${!RESTRICTED_SCOPE_SET[@]}"; do
    create_role_if_missing "$(scope_role_name "$scope")"
  done

  # Rerun-only role: can clear/retry tasks, but no Dag Runs.can_create.
  # Note: Dags.can_edit is required for clear/retry in standard FAB permissions.
  grant_role_perm "$RERUN_ROLE" "can_edit" "DAGs"
  grant_role_perm "$RERUN_ROLE" "can_edit" "DAG Runs"
  grant_role_perm "$RERUN_ROLE" "can_edit" "Task Instances"
  grant_role_perm "$RERUN_ROLE" "can_delete" "Task Instances"

  # Trigger roles: Dag Runs.can_create + DAG:<dag_id>.can_edit (assigned later).
  # Without DAG:<dag_id>.can_edit, user cannot trigger that DAG.
  grant_role_perm "$NONUS_TRIGGER_ROLE" "can_create" "DAG Runs"
  for scope in "${!RESTRICTED_SCOPE_SET[@]}"; do
    grant_role_perm "$(scope_role_name "$scope")" "can_create" "DAG Runs"
  done
}

print_postcheck() {
  cat <<'EOF'

Post-check commands:
  airflow roles list -o plain
  airflow roles list -p -o plain | grep -E 'AF_RERUN_ALL_NO_TRIGGER|AF_TRIGGER_SCOPE_'
  airflow users list -o plain

Reminder:
  - Normal user should get: Viewer + AF_RERUN_ALL_NO_TRIGGER
  - US user should get: Viewer + AF_TRIGGER_SCOPE_US
  - Non-US privileged should get: Viewer + AF_TRIGGER_SCOPE_NONUS
  - US privileged should get: Viewer + AF_TRIGGER_SCOPE_US + AF_TRIGGER_SCOPE_NONUS
EOF
}

main() {
  # Always restore shell state on exit.
  trap deactivate_conda_if_needed EXIT

  # Order matters:
  # 1) parse/load/activate/check
  # 2) sync resources
  # 3) ensure base role perms
  # 4) cleanup old DAG grants
  # 5) assign fresh DAG grants
  parse_args "$@"
  load_config
  activate_conda_if_configured
  check_prereqs
  normalize_restricted_scopes
  load_scope_overrides

  log "Applying RBAC with restricted scopes: $(IFS=','; echo "${!RESTRICTED_SCOPE_SET[*]}")"
  sync_permissions
  configure_roles
  cleanup_managed_dag_permissions
  assign_dag_permissions
  print_postcheck
}

main "$@"
