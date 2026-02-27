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
PASSWORD_FILE=""
DRY_RUN=false
STRICT_TRIGGER_ISOLATION=false
LEGACY_ROLE_COMBO_FLAG=false

CONDA_ACTIVATED=false

# Roles managed by this script for test users.
# Extra roles outside this set are left untouched.
MANAGED_TEST_ROLES=(
  "Viewer"
  "AF_RERUN_ALL_NO_TRIGGER"
  "AF_TRIGGER_SCOPE_GLOBAL"
  "AF_TRIGGER_SCOPE_NONUS"
  "AF_TRIGGER_SCOPE_US"
  "AF_TRIGGER_SCOPE_MX"
  "AF_TRIGGER_SCOPE_CN"
)

usage() {
  cat <<'EOF'
Usage:
  create_test_users.sh [options]

Options:
  -e, --env <dev|uat|prod>    Select environment config (default: dev)
  -c, --config <path>         Path to airflow-manager config file
  -p, --password <text>       Password for all test users
  -P, --password-file <path>  Read password from file (first line)
  --strict-trigger-isolation  Trigger users do NOT get AF_RERUN_ALL_NO_TRIGGER
  --legacy-role-combo         Deprecated alias; same as default behavior
  -n, --dry-run               Print commands only
  -h, --help                  Show this help

Creates 4 local users for RBAC testing (default target matrix):
  rbac_normal      -> Viewer + AF_RERUN_ALL_NO_TRIGGER
  rbac_us_user     -> Viewer + AF_RERUN_ALL_NO_TRIGGER + AF_TRIGGER_SCOPE_US
  rbac_nonus_priv  -> Viewer + AF_RERUN_ALL_NO_TRIGGER + AF_TRIGGER_SCOPE_GLOBAL
  rbac_us_priv     -> Viewer + AF_RERUN_ALL_NO_TRIGGER + AF_TRIGGER_SCOPE_US + AF_TRIGGER_SCOPE_GLOBAL

Note:
  Scope isolation depends on DAG Run:<dag_id>.can_create grants managed by apply_region_rbac.sh.
  Do not manually grant global DAG Runs.can_create to AF_TRIGGER_SCOPE_* roles.
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
      -P|--password-file)
        PASSWORD_FILE="${2:-}"
        shift 2
        ;;
      --strict-trigger-isolation)
        STRICT_TRIGGER_ISOLATION=true
        shift
        ;;
      --legacy-role-combo)
        LEGACY_ROLE_COMBO_FLAG=true
        STRICT_TRIGGER_ISOLATION=false
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

load_password() {
  # Do not allow conflicting secret sources.
  if [[ -n "$PASSWORD" && -n "$PASSWORD_FILE" ]]; then
    die "Use either --password or --password-file, not both."
  fi

  # Password file keeps secrets out of process list and shell history.
  if [[ -n "$PASSWORD_FILE" ]]; then
    [[ -f "$PASSWORD_FILE" ]] || die "Password file not found: $PASSWORD_FILE"
    [[ -r "$PASSWORD_FILE" ]] || die "Password file is not readable: $PASSWORD_FILE"
    PASSWORD="$(<"$PASSWORD_FILE")"
    # Trim Windows CR if present.
    PASSWORD="${PASSWORD%$'\r'}"
  fi

  [[ -n "$PASSWORD" ]] || die "Either --password or --password-file is required."
}

user_exists() {
  # Query by username from Airflow user table.
  # In dry-run mode, skip DB calls and show full create/grant plan.
  if [[ "$DRY_RUN" == "true" ]]; then
    return 1
  fi
  local username="$1"
  # Airflow 3 plain output starts with numeric ID column; older formats may not.
  # Keep compatibility by selecting column 2 when column 1 is numeric, else column 1.
  airflow users list -o plain \
    | awk 'NR>1 {if ($1 ~ /^[0-9]+$/ && $2 != "") print $2; else print $1}' \
    | grep -Fxq "$username"
}

role_in_array() {
  local needle="$1"
  shift
  local role
  for role in "$@"; do
    if [[ "$role" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

get_user_roles() {
  # Return one role per line for a given username, parsed from "users list -o plain".
  local username="$1"
  local row roles_blob raw role_clean

  row="$(
    airflow users list -o plain | awk -v username="$username" '
      NR > 1 {
        user_col = ($1 ~ /^[0-9]+$/ && $2 != "") ? $2 : $1
        if (user_col == username) {
          print
          exit
        }
      }
    '
  )"

  [[ -n "$row" ]] || return 1

  # Role list is rendered as Python-like list: ['Viewer', 'RoleA']
  roles_blob="${row#*\[}"
  if [[ "$roles_blob" == "$row" ]]; then
    return 0
  fi
  roles_blob="${roles_blob%]*}"

  IFS=',' read -r -a raw_roles <<<"$roles_blob"
  for raw in "${raw_roles[@]}"; do
    role_clean="$(printf '%s' "$raw" | sed -e "s/[[:space:]]//g" -e "s/'//g" -e 's/"//g')"
    [[ -n "$role_clean" ]] && printf '%s\n' "$role_clean"
  done
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

remove_user_role() {
  local username="$1"
  local role="$2"
  run_cmd airflow users remove-role -u "$username" -r "$role"
}

reconcile_user_roles() {
  # Ensure user has exactly required roles within MANAGED_TEST_ROLES.
  # Missing roles will be added; managed-but-unwanted roles will be removed.
  local username="$1"
  shift
  local desired_roles=("$@")
  local current_roles=()
  local role
  local changed=false

  if [[ "$DRY_RUN" == "true" ]]; then
    # Dry-run avoids DB reads; print intended grants only.
    for role in "${desired_roles[@]}"; do
      grant_user_role "$username" "$role"
    done
    log "Dry-run: skipped role diff check for $username"
    return 0
  fi

  while IFS= read -r role; do
    [[ -n "$role" ]] && current_roles+=("$role")
  done < <(get_user_roles "$username" || true)

  # Add missing desired roles.
  for role in "${desired_roles[@]}"; do
    if ! role_in_array "$role" "${current_roles[@]}"; then
      grant_user_role "$username" "$role"
      changed=true
    fi
  done

  # Remove roles that this script manages but are not desired for this user.
  for role in "${current_roles[@]}"; do
    if role_in_array "$role" "${MANAGED_TEST_ROLES[@]}" && ! role_in_array "$role" "${desired_roles[@]}"; then
      remove_user_role "$username" "$role"
      changed=true
    fi
  done

  if [[ "$changed" == "false" ]]; then
    log "Roles already aligned: $username"
  fi
}

main() {
  # Ensure conda env deactivation no matter how script exits.
  trap deactivate_conda_if_needed EXIT

  parse_args "$@"
  load_password
  load_config
  activate_conda_if_configured
  command -v airflow >/dev/null 2>&1 || die "airflow command not found in PATH."

  if [[ "$STRICT_TRIGGER_ISOLATION" == "true" ]]; then
    log "Strict trigger isolation is ON: trigger users will not get AF_RERUN_ALL_NO_TRIGGER."
  else
    log "Cross-DAG rerun is ON for trigger users: AF_RERUN_ALL_NO_TRIGGER will be assigned."
  fi
  if [[ "$LEGACY_ROLE_COMBO_FLAG" == "true" ]]; then
    log "NOTE: --legacy-role-combo is deprecated and equals default behavior."
  fi

  # 1) Normal user: can view + rerun, cannot trigger new DagRun.
  create_user_if_missing "rbac_normal" "RBAC" "Normal" "rbac_normal@example.local"
  reconcile_user_roles "rbac_normal" \
    "Viewer" \
    "AF_RERUN_ALL_NO_TRIGGER"

  # 2) US user: trigger US DAGs only; optionally keep cross-DAG rerun.
  local us_user_roles=("Viewer" "AF_TRIGGER_SCOPE_US" "AF_RERUN_ALL_NO_TRIGGER")
  if [[ "$STRICT_TRIGGER_ISOLATION" == "true" ]]; then
    us_user_roles=("Viewer" "AF_TRIGGER_SCOPE_US")
  fi
  create_user_if_missing "rbac_us_user" "RBAC" "USUser" "rbac_us_user@example.local"
  reconcile_user_roles "rbac_us_user" "${us_user_roles[@]}"

  # 3) Non-US privileged: trigger global DAGs only; optionally keep cross-DAG rerun.
  local nonus_priv_roles=("Viewer" "AF_TRIGGER_SCOPE_GLOBAL" "AF_RERUN_ALL_NO_TRIGGER")
  if [[ "$STRICT_TRIGGER_ISOLATION" == "true" ]]; then
    nonus_priv_roles=("Viewer" "AF_TRIGGER_SCOPE_GLOBAL")
  fi
  create_user_if_missing "rbac_nonus_priv" "RBAC" "NonUSPriv" "rbac_nonus_priv@example.local"
  reconcile_user_roles "rbac_nonus_priv" "${nonus_priv_roles[@]}"

  # 4) US privileged: trigger both US/global DAGs; optionally keep cross-DAG rerun.
  local us_priv_roles=("Viewer" "AF_TRIGGER_SCOPE_US" "AF_TRIGGER_SCOPE_GLOBAL" "AF_RERUN_ALL_NO_TRIGGER")
  if [[ "$STRICT_TRIGGER_ISOLATION" == "true" ]]; then
    us_priv_roles=("Viewer" "AF_TRIGGER_SCOPE_US" "AF_TRIGGER_SCOPE_GLOBAL")
  fi
  create_user_if_missing "rbac_us_priv" "RBAC" "USPriv" "rbac_us_priv@example.local"
  reconcile_user_roles "rbac_us_priv" "${us_priv_roles[@]}"

  log "Done. Verify with: airflow users list -o plain"
}

main "$@"
