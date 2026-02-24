#!/usr/bin/env bash
# Exit on undefined variables and keep pipeline errors visible.
set -uo pipefail

# Manage one Celery worker process for one environment.
# Typical usage per worker account:
#   ./celery-worker-manager.sh dev start
#   ./celery-worker-manager.sh dev stop
#   ./celery-worker-manager.sh dev status

SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Runtime flags and arguments resolved from CLI input.
DRY_RUN=false
MANAGER_DEBUG="${MANAGER_DEBUG:-false}"
ENV_NAME=""
ACTION=""
WORKER_NAME=""
CONFIG_FILE=""
RUN_LOG_FILE=""
CONDA_ACTIVATED=false
OVERALL_RC=0
CURRENT_UID=""

# Allowed CLI values.
VALID_ENVS=(dev uat prod)
VALID_ACTIONS=(start stop restart status)

usage() {
  cat <<'EOF'
Usage:
  celery-worker-manager.sh <env> <action> [worker-name] [--dry-run|-n] [--debug|-d]

Arguments:
  env         : dev | uat | prod
  action      : start | stop | restart | status
  worker-name : optional, default from CELERY_WORKER in config or current login user

Examples:
  celery-worker-manager.sh dev start
  celery-worker-manager.sh uat status hdp11-ss-gscdd
  celery-worker-manager.sh prod restart fap41-abibatch-01 --dry-run
  celery-worker-manager.sh dev status hdp11-ss-et-airflow --debug
EOF
}

to_lower() {
  # Normalize user input to avoid case-sensitive surprises.
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

contains_item() {
  # Generic helper for validating args against allow-lists.
  local needle="$1"
  shift
  local item
  for item in "$@"; do
    if [[ "$needle" == "$item" ]]; then
      return 0
    fi
  done
  return 1
}

log() {
  # Single logging function for both console and manager log file.
  local level="$1"
  shift
  local line
  line="$(printf '[%s] [%s] %s' "$(date '+%Y-%m-%d %H:%M:%S')" "$level" "$*")"
  if [[ -n "$RUN_LOG_FILE" ]]; then
    printf '%s\n' "$line" | tee -a "$RUN_LOG_FILE"
  else
    printf '%s\n' "$line"
  fi
}

debug_log() {
  # Extra diagnostics controlled by MANAGER_DEBUG=true.
  if [[ "$MANAGER_DEBUG" == "true" ]]; then
    local line
    line="$(printf '[%s] [DEBUG] %s' "$(date '+%Y-%m-%d %H:%M:%S')" "$*")"
    if [[ -n "$RUN_LOG_FILE" ]]; then
      printf '%s\n' "$line" | tee -a "$RUN_LOG_FILE" >&2
    else
      printf '%s\n' "$line" >&2
    fi
  fi
}

run_shell() {
  # Wrapper used by every external command to support dry-run mode.
  local cmd="$1"
  if [[ "$DRY_RUN" == "true" ]]; then
    log "DRYRUN" "$cmd"
    return 0
  fi
  debug_log "EXEC: $cmd"
  local rc=0
  if [[ -n "$RUN_LOG_FILE" ]]; then
    bash -c "$cmd" >>"$RUN_LOG_FILE" 2>&1 || rc=$?
  else
    bash -c "$cmd" || rc=$?
  fi
  debug_log "EXEC-RESULT: rc=$rc cmd=$cmd"
  return "$rc"
}

print_row() {
  # Status tables are written to both stdout and manager log.
  local format="$1"
  shift
  local line
  printf -v line "$format" "$@"
  if [[ -n "$RUN_LOG_FILE" ]]; then
    printf '%s\n' "$line" | tee -a "$RUN_LOG_FILE"
  else
    printf '%s\n' "$line"
  fi
}

parse_args() {
  # Accept positional args plus flags in any order.
  local positional=()
  local arg

  for arg in "$@"; do
    case "$arg" in
      --dry-run|-n)
        DRY_RUN=true
        ;;
      --debug|-d)
        MANAGER_DEBUG=true
        ;;
      *)
        positional+=("$arg")
        ;;
    esac
  done

  if [[ ${#positional[@]} -lt 2 || ${#positional[@]} -gt 3 ]]; then
    usage
    exit 1
  fi

  ENV_NAME="$(to_lower "${positional[0]}")"
  ACTION="$(to_lower "${positional[1]}")"
  WORKER_NAME="${positional[2]:-}"
}

validate_input() {
  # Fail fast before loading config or touching runtime files.
  if ! contains_item "$ENV_NAME" "${VALID_ENVS[@]}"; then
    echo "ERROR: invalid env '$ENV_NAME'."
    usage
    exit 1
  fi

  if ! contains_item "$ACTION" "${VALID_ACTIONS[@]}"; then
    echo "ERROR: invalid action '$ACTION'."
    usage
    exit 1
  fi
}

load_config() {
  # Reuse the same env config files as airflow-manager.sh.
  CONFIG_FILE="${CELERY_WORKER_MANAGER_CONFIG:-$SCRIPT_DIR/conf/airflow-manager-${ENV_NAME}.conf}"
  if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: config file not found: $CONFIG_FILE"
    exit 1
  fi

  # shellcheck disable=SC1090
  source "$CONFIG_FILE"

  # Ensure AIRFLOW_HOME local settings are importable in worker CLI subprocesses.
  # This keeps policy behavior consistent with scheduler/webserver.
  if [[ -n "${AIRFLOW_HOME:-}" ]]; then
    case ":${PYTHONPATH:-}:" in
      *":${AIRFLOW_HOME}:"*) ;;
      *) export PYTHONPATH="${AIRFLOW_HOME}:${PYTHONPATH:-}" ;;
    esac
  fi
}

set_defaults() {
  # Keep config minimal by filling in safe defaults here.
  : "${AIRFLOW_HOME:?AIRFLOW_HOME must be set in config file: $CONFIG_FILE}"
  : "${MANAGER_LOG_DIR:="$AIRFLOW_HOME/logs/manager"}"
  : "${STOP_WAIT_SECONDS:=10}"
  : "${FORCE_KILL_WAIT_SECONDS:=5}"
  : "${START_VERIFY_RETRIES:=6}"
  : "${START_VERIFY_INTERVAL_SECONDS:=1}"

  : "${WORKER_NM:=4}"
  : "${WORKER_LOG_LEVEL:=INFO}"
  : "${WORKER_QUEUE_PREFIX:=queue_worker_}"
  : "${WORKER_APP:=airflow.providers.celery.executors.celery_executor.app}"
  : "${WORKER_PATTERN:=airflow[[:space:]]+celery[[:space:]]+worker|celery([[:space:]]+-A[[:space:]]+${WORKER_APP})?[[:space:]]+worker|celeryd:.*celery worker}"
  MANAGER_DEBUG="$(to_lower "${MANAGER_DEBUG:-false}")"
  CURRENT_UID="${WORKER_OS_UID:-$(id -u)}"

  if [[ -z "$WORKER_NAME" ]]; then
    # Priority: CLI argument > config CELERY_WORKER > current login user.
    WORKER_NAME="${CELERY_WORKER:-$(id -un)}"
  fi

  : "${WORKER_RUN_DIR:="$AIRFLOW_HOME/run/worker_${WORKER_NAME}"}"
  : "${WORKER_LOG_FILE:="$AIRFLOW_HOME/logs/worker_${WORKER_NAME}/celery.log"}"
  : "${WORKER_PID_FILE:="$WORKER_RUN_DIR/celery.pid"}"
}

setup_runtime_logging() {
  # Create runtime directories early so later commands can rely on them.
  mkdir -p "$MANAGER_LOG_DIR"

  # Remove manager logs older than 90 days.
  find "$MANAGER_LOG_DIR" -type f -name "${SCRIPT_NAME%.sh}-${ENV_NAME}-*.log" -mtime +90 -print -delete 2>/dev/null || true

  RUN_LOG_FILE="$MANAGER_LOG_DIR/${SCRIPT_NAME%.sh}-${ENV_NAME}-$(date '+%Y-%m-%d').log"
  touch "$RUN_LOG_FILE"

  log "INFO" "Config file: $CONFIG_FILE"
  log "INFO" "Run mode: env=$ENV_NAME action=$ACTION worker=$WORKER_NAME dry_run=$DRY_RUN debug=$MANAGER_DEBUG"
}

activate_conda_if_requested() {
  # Conda activation is optional and controlled by config.
  if [[ -z "${CONDA_BASE:-}" || -z "${CONDA_ENV_NAME:-}" ]]; then
    debug_log "Skip conda activation (CONDA_BASE/CONDA_ENV_NAME not fully configured)."
    return 0
  fi

  # If the expected env is already active, avoid noisy warnings/activation calls.
  if [[ "${CONDA_DEFAULT_ENV:-}" == "$CONDA_ENV_NAME" ]]; then
    debug_log "Conda env already active: $CONDA_DEFAULT_ENV"
    return 0
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    log "DRYRUN" "source \"$CONDA_BASE/etc/profile.d/conda.sh\" && conda activate \"$CONDA_ENV_NAME\""
    return 0
  fi

  if [[ -f "$CONDA_BASE/etc/profile.d/conda.sh" ]]; then
    # Preferred activation path for modern conda installs.
    # shellcheck disable=SC1090
    source "$CONDA_BASE/etc/profile.d/conda.sh"
    conda activate "$CONDA_ENV_NAME"
    CONDA_ACTIVATED=true
    return 0
  fi

  if [[ -f "$CONDA_BASE/bin/activate" ]]; then
    # Backward-compatible activation path.
    # shellcheck disable=SC1090
    source "$CONDA_BASE/bin/activate" "$CONDA_ENV_NAME"
    CONDA_ACTIVATED=true
    return 0
  fi

  log "WARN" "Conda activation script not found under CONDA_BASE=$CONDA_BASE."
}

deactivate_conda_if_requested() {
  # Ensure we do not leak activated env to caller shell context.
  if [[ "$CONDA_ACTIVATED" != "true" || "$DRY_RUN" == "true" ]]; then
    return 0
  fi
  if command -v conda >/dev/null 2>&1; then
    conda deactivate || true
  fi
}

ensure_directories() {
  # Worker writes its own PID/log files under worker-specific folders.
  mkdir -p "$(dirname "$WORKER_PID_FILE")" "$(dirname "$WORKER_LOG_FILE")"
}

read_pid_file() {
  # Trim whitespace/newlines so PID checks stay robust.
  if [[ -f "$WORKER_PID_FILE" ]]; then
    tr -d '[:space:]' <"$WORKER_PID_FILE"
  fi
}

is_pid_running() {
  # kill -0 checks existence/permission without sending a real signal.
  local pid="$1"
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null
}

worker_pattern() {
  # Pattern fallback for stale/missing PID files.
  if [[ -n "${WORKER_PATTERN:-}" ]]; then
    printf '%s' "$WORKER_PATTERN"
  fi
}

pgrep_worker_pids() {
  # Limit process discovery to one OS user to avoid cross-account interference.
  local pattern="$1"
  local matched
  matched="$(pgrep -u "$CURRENT_UID" -f "$pattern" 2>/dev/null || true)"
  if [[ -n "$matched" ]]; then
    printf '%s\n' "$matched"
    return 0
  fi

  # Fallback for environments where user filtering is not available.
  pgrep -f "$pattern" 2>/dev/null || true
}

running_pid() {
  # PID discovery order:
  # 1) worker PID file
  # 2) process pattern search
  local pid
  pid="$(read_pid_file)"
  debug_log "running_pid(worker=$WORKER_NAME): pid_file=$WORKER_PID_FILE pid_from_file=${pid:-none}"
  if is_pid_running "$pid"; then
    debug_log "running_pid(worker=$WORKER_NAME): pid=$pid is alive (from pid file)."
    echo "$pid"
    return 0
  fi

  local pattern matched_pids
  pattern="$(worker_pattern)"
  matched_pids="$(pgrep_worker_pids "$pattern")"
  debug_log "running_pid(worker=$WORKER_NAME): uid=$CURRENT_UID pattern=$pattern matches=${matched_pids//$'\n'/,}"
  pid="$(printf '%s\n' "$matched_pids" | head -n1 || true)"
  if is_pid_running "$pid"; then
    debug_log "running_pid(worker=$WORKER_NAME): pid=$pid selected from pattern search."
    echo "$pid"
    return 0
  fi

  debug_log "running_pid(worker=$WORKER_NAME): no running process detected."
  echo ""
}

show_recent_worker_logs() {
  # When worker exits right after start, emit recent logs for quick diagnosis.
  local line
  if [[ -f "$WORKER_LOG_FILE" ]]; then
    log "ERROR" "Recent log tail for worker ($WORKER_LOG_FILE):"
    while IFS= read -r line; do
      log "ERROR" "$line"
    done < <(tail -n 30 "$WORKER_LOG_FILE")
  else
    log "ERROR" "Worker log file not found: $WORKER_LOG_FILE"
  fi
}

wait_for_worker_start() {
  # Validate worker survives startup; background command success alone is insufficient.
  local attempt pid

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  for ((attempt=1; attempt<=START_VERIFY_RETRIES; attempt++)); do
    pid="$(running_pid)"
    if [[ -n "$pid" ]]; then
      debug_log "wait_for_worker_start(worker=$WORKER_NAME): running pid=$pid at attempt=$attempt/$START_VERIFY_RETRIES"
      return 0
    fi
    debug_log "wait_for_worker_start(worker=$WORKER_NAME): not running at attempt=$attempt/$START_VERIFY_RETRIES"
    if (( attempt < START_VERIFY_RETRIES )); then
      sleep "$START_VERIFY_INTERVAL_SECONDS"
    fi
  done

  log "ERROR" "Worker did not stay running after startup command."
  remove_pid_file
  show_recent_worker_logs
  return 1
}

remove_pid_file() {
  # PID cleanup prevents stale status after forced stops.
  if [[ -f "$WORKER_PID_FILE" ]]; then
    if [[ "$DRY_RUN" == "true" ]]; then
      log "DRYRUN" "rm -f \"$WORKER_PID_FILE\""
    else
      rm -f "$WORKER_PID_FILE"
    fi
  fi
}

stop_pid_gracefully() {
  # Two-phase stop: SIGTERM first, SIGKILL only if still alive.
  local pid="$1"
  if ! is_pid_running "$pid"; then
    return 0
  fi

  log "INFO" "Stopping worker pid=$pid with SIGTERM."
  run_shell "kill \"$pid\"" || true
  if [[ "$DRY_RUN" == "false" ]]; then
    sleep "$STOP_WAIT_SECONDS"
  fi

  if is_pid_running "$pid"; then
    log "WARN" "Worker pid=$pid still running. Sending SIGKILL."
    run_shell "kill -9 \"$pid\"" || true
    if [[ "$DRY_RUN" == "false" ]]; then
      sleep "$FORCE_KILL_WAIT_SECONDS"
    fi
  fi
}

start_worker() {
  # Start worker with the exact command style requested by operations.
  # Queue list always includes the dedicated worker queue plus "default".
  local pid queue_name cmd
  pid="$(running_pid)"
  if [[ -n "$pid" ]]; then
    log "INFO" "Worker is already running (pid=$pid)."
    return 0
  fi

  ensure_directories
  queue_name="${WORKER_QUEUE_NAME:-${WORKER_QUEUE_PREFIX}${WORKER_NAME}}"
  if [[ ",$queue_name," != *",default,"* ]]; then
    queue_name="${queue_name},default"
  fi

  cmd="celery -A \"$WORKER_APP\" worker --hostname \"${WORKER_NAME}@%h\" --loglevel INFO --concurrency \"$WORKER_NM\" --queues \"$queue_name\" --events --pidfile \"$WORKER_PID_FILE\" --logfile \"$WORKER_LOG_FILE\" >> \"$WORKER_LOG_FILE\" 2>&1 &"

  log "INFO" "Starting worker (worker_name=$WORKER_NAME queue=$queue_name)."
  run_shell "$cmd" || return 1
  wait_for_worker_start
}

stop_worker() {
  # Stop target from PID file first, then sweep leftovers by pattern.
  local pid extra_pids extra_pid

  pid="$(read_pid_file)"
  debug_log "stop_worker(worker=$WORKER_NAME): pid_file=$WORKER_PID_FILE pid_from_file=${pid:-none}"
  if [[ -n "$pid" ]]; then
    stop_pid_gracefully "$pid"
  fi

  local pattern
  pattern="$(worker_pattern)"
  extra_pids="$(pgrep_worker_pids "$pattern")"
  debug_log "stop_worker(worker=$WORKER_NAME): uid=$CURRENT_UID pattern=$pattern matched=${extra_pids//$'\n'/,}"
  for extra_pid in $extra_pids; do
    if [[ "$extra_pid" != "$$" ]]; then
      stop_pid_gracefully "$extra_pid"
    fi
  done

  remove_pid_file
}

print_status_table() {
  # Human-readable status report required by operations team.
  local pid status
  pid="$(running_pid)"
  if [[ -n "$pid" ]]; then
    status="running"
  else
    status="stopped"
    pid="-"
  fi

  print_row '%-8s %-20s %-10s %-10s' "env" "service" "status" "pid"
  print_row '%-8s %-20s %-10s %-10s' "--------" "--------------------" "---------" "----------"
  print_row '%-8s %-20s %-10s %-10s' "$ENV_NAME" "worker:$WORKER_NAME" "$status" "$pid"
}

main() {
  # Main orchestration flow:
  # parse -> validate -> load config -> prepare runtime -> execute action.
  parse_args "$@"
  validate_input
  load_config
  set_defaults
  setup_runtime_logging

  trap deactivate_conda_if_requested EXIT
  activate_conda_if_requested

  case "$ACTION" in
    start)
      start_worker || OVERALL_RC=1
      ;;
    stop)
      stop_worker || OVERALL_RC=1
      ;;
    restart)
      stop_worker || OVERALL_RC=1
      start_worker || OVERALL_RC=1
      ;;
    status)
      print_status_table
      ;;
    *)
      log "ERROR" "Unsupported action: $ACTION"
      OVERALL_RC=1
      ;;
  esac

  if [[ "$OVERALL_RC" -eq 0 ]]; then
    log "INFO" "Completed successfully."
  else
    log "ERROR" "Completed with errors."
  fi

  return "$OVERALL_RC"
}

main "$@"
