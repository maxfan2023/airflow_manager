#!/usr/bin/env bash
# Exit on undefined variables and keep pipeline errors visible.
set -uo pipefail

# Manage Airflow platform services for one environment.
# Usage examples:
#   ./airflow-manager.sh dev start all-services
#   ./airflow-manager.sh dev stop redis
#   ./airflow-manager.sh dev status all-services --dry-run

SCRIPT_NAME="$(basename "$0")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Runtime flags and arguments resolved from CLI input.
DRY_RUN=false
MANAGER_DEBUG="${MANAGER_DEBUG:-false}"
ENV_NAME=""
ACTION=""
TARGET_SERVICE="all-services"
CONFIG_FILE=""
RUN_LOG_FILE=""
CONDA_ACTIVATED=false
OVERALL_RC=0

# "all-services" expands from this list. PostgreSQL is handled separately.
BASE_SERVICES=(redis api-server scheduler dag-processor triggerer worker flower)
VALID_ENVS=(dev uat prod)
VALID_ACTIONS=(start stop restart status)

usage() {
  cat <<'EOF'
Usage:
  airflow-manager.sh <env> <action> [service] [--dry-run|-n] [--debug|-d]

Arguments:
  env      : dev | uat | prod
  action   : start | stop | restart | status
  service  : all-services | pg | redis | api-server | scheduler | dag-processor | triggerer | worker | flower
             (pg is allowed only for dev when MANAGE_POSTGRESQL_IN_THIS_ENV=true)

Examples:
  airflow-manager.sh dev start all-services
  airflow-manager.sh dev start redis
  airflow-manager.sh dev stop api-server
  airflow-manager.sh dev restart all-services
  airflow-manager.sh prod status scheduler
  airflow-manager.sh uat status all-services --dry-run
  airflow-manager.sh dev status api-server --debug
EOF
}

to_lower() {
  # Normalize user/config input to avoid case-sensitive surprises.
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
  TARGET_SERVICE="$(to_lower "${positional[2]:-all-services}")"
}

validate_basic_input() {
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
  # Allow runtime override via AIRFLOW_MANAGER_CONFIG, else use env-based default.
  CONFIG_FILE="${AIRFLOW_MANAGER_CONFIG:-$SCRIPT_DIR/conf/airflow-manager-${ENV_NAME}.conf}"
  if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: config file not found: $CONFIG_FILE"
    exit 1
  fi

  # shellcheck disable=SC1090
  source "$CONFIG_FILE"

  # Ensure AIRFLOW_HOME local settings are importable in every subprocess.
  # This avoids silent policy misses when the caller shell has no PYTHONPATH.
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
  : "${PID_DIR:="$AIRFLOW_HOME/run/pids"}"
  : "${COMMON_LOGDIR:="$AIRFLOW_HOME/logs"}"
  : "${MANAGER_LOG_DIR:="$AIRFLOW_HOME/logs/manager"}"

  : "${STOP_WAIT_SECONDS:=10}"
  : "${FORCE_KILL_WAIT_SECONDS:=5}"
  : "${START_VERIFY_RETRIES:=6}"
  : "${START_VERIFY_INTERVAL_SECONDS:=1}"

  : "${API_SERVER_PORT:=8181}"
  : "${FLOWER_PORT:=5555}"
  : "${FLOWER_ENABLE_SSL:=false}"
  FLOWER_ENABLE_SSL="$(to_lower "$FLOWER_ENABLE_SSL")"
  : "${FLOWER_CONF:="$AIRFLOW_HOME/flower_config.py"}"
  : "${HOSTNAME_OVERRIDE:="${HOSTNAME:-$(hostname -s)}"}"

  : "${REDIS_HOST:=127.0.0.1}"
  : "${REDIS_PORT:=6379}"
  : "${REDIS_CONF:="${REDIS_DIR:-}/redis.conf"}"

  : "${WORKER_NM:=4}"
  : "${WORKER_LOG_LEVEL:=INFO}"
  : "${WORKER_QUEUE_PREFIX:=queue_worker_}"
  : "${CELERY_WORKER:="$(id -un)"}"
  : "${WORKER_APP:=airflow.providers.celery.executors.celery_executor.app}"
  : "${WORKER_PATTERN:=airflow[[:space:]]+celery[[:space:]]+worker|celery([[:space:]]+-A[[:space:]]+${WORKER_APP})?[[:space:]]+worker|celeryd:.*celery worker}"
  MANAGER_DEBUG="$(to_lower "${MANAGER_DEBUG:-false}")"

  if [[ "$ENV_NAME" == "dev" ]]; then
    # In dev we usually own PG lifecycle; in higher envs we typically do not.
    : "${MANAGE_POSTGRESQL_IN_THIS_ENV:=true}"
  else
    : "${MANAGE_POSTGRESQL_IN_THIS_ENV:=false}"
  fi
  MANAGE_POSTGRESQL_IN_THIS_ENV="$(to_lower "$MANAGE_POSTGRESQL_IN_THIS_ENV")"
}

setup_runtime_logging() {
  # Create runtime directories early so later commands can rely on them.
  mkdir -p "$PID_DIR" "$COMMON_LOGDIR" "$MANAGER_LOG_DIR"

  # Remove manager logs older than 90 days.
  find "$MANAGER_LOG_DIR" -type f -name "${SCRIPT_NAME%.sh}-${ENV_NAME}-*.log" -mtime +90 -print -delete 2>/dev/null || true

  RUN_LOG_FILE="$MANAGER_LOG_DIR/${SCRIPT_NAME%.sh}-${ENV_NAME}-$(date '+%Y-%m-%d').log"
  touch "$RUN_LOG_FILE"

  log "INFO" "Config file: $CONFIG_FILE"
  log "INFO" "Run mode: env=$ENV_NAME action=$ACTION service=$TARGET_SERVICE dry_run=$DRY_RUN debug=$MANAGER_DEBUG"
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

should_manage_pg_service() {
  # PostgreSQL is intentionally scoped to dev unless explicitly enabled.
  [[ "$ENV_NAME" == "dev" && "$MANAGE_POSTGRESQL_IN_THIS_ENV" == "true" ]]
}

validate_target_service() {
  # "pg" is only valid when PG lifecycle is enabled for current env.
  local valid_services=("${BASE_SERVICES[@]}")
  if should_manage_pg_service; then
    valid_services+=(pg)
  fi

  if [[ "$TARGET_SERVICE" == "all-services" ]]; then
    return 0
  fi

  if ! contains_item "$TARGET_SERVICE" "${valid_services[@]}"; then
    echo "ERROR: invalid service '$TARGET_SERVICE' for env '$ENV_NAME'."
    usage
    exit 1
  fi
}

service_pid_file() {
  # Centralized PID file mapping keeps start/stop/status consistent.
  local service="$1"
  local worker_pid_default="$AIRFLOW_HOME/run/worker_${CELERY_WORKER}/celery.pid"
  case "$service" in
    pg) echo "${PG_PID_FILE:-$PID_DIR/postgresql.pid}" ;;
    redis) echo "${REDIS_PID_FILE:-$PID_DIR/redis.pid}" ;;
    api-server) echo "${API_SERVER_PID_FILE:-$PID_DIR/api-server.pid}" ;;
    scheduler) echo "${SCHEDULER_PID_FILE:-$PID_DIR/scheduler.pid}" ;;
    dag-processor) echo "${DAG_PROCESSOR_PID_FILE:-$PID_DIR/dag-processor.pid}" ;;
    triggerer) echo "${TRIGGERER_PID_FILE:-$PID_DIR/triggerer.pid}" ;;
    flower) echo "${FLOWER_PID_FILE:-$PID_DIR/flower.pid}" ;;
    worker) echo "${WORKER_PID_FILE:-$worker_pid_default}" ;;
    *) echo "" ;;
  esac
}

service_log_file() {
  # Centralized service log mapping.
  local service="$1"
  local worker_log_default="$AIRFLOW_HOME/logs/worker_${CELERY_WORKER}/celery.log"
  case "$service" in
    pg) echo "${PG_START_LOG:-$PG_HOME/pg15.log}" ;;
    redis) echo "${REDIS_LOG_FILE:-$COMMON_LOGDIR/redis.log}" ;;
    api-server) echo "${API_SERVER_LOG_FILE:-$COMMON_LOGDIR/webserver.log}" ;;
    scheduler) echo "${SCHEDULER_LOG_FILE:-$COMMON_LOGDIR/scheduler.log}" ;;
    dag-processor) echo "${DAG_PROCESSOR_LOG_FILE:-$COMMON_LOGDIR/dag-processor.log}" ;;
    triggerer) echo "${TRIGGERER_LOG_FILE:-$COMMON_LOGDIR/triggerer.log}" ;;
    flower) echo "${FLOWER_LOG_FILE:-$COMMON_LOGDIR/flower.log}" ;;
    worker) echo "${WORKER_LOG_FILE:-$worker_log_default}" ;;
    *) echo "$COMMON_LOGDIR/unknown.log" ;;
  esac
}

service_pattern() {
  # Fallback process patterns used when PID files are stale or missing.
  local service="$1"
  case "$service" in
    pg) echo "${PG_PATTERN:-postgres}" ;;
    redis) echo "${REDIS_PATTERN:-redis-server}" ;;
    # Airflow 3 may expose "api_server" in process list even when started as "api-server".
    api-server) echo "${API_SERVER_PATTERN:-airflow[[:space:]]+api[-_]server}" ;;
    scheduler) echo "${SCHEDULER_PATTERN:-airflow[[:space:]]+scheduler}" ;;
    dag-processor) echo "${DAG_PROCESSOR_PATTERN:-airflow[[:space:]]+dag[-_]processor}" ;;
    triggerer) echo "${TRIGGERER_PATTERN:-airflow[[:space:]]+triggerer}" ;;
    flower) echo "${FLOWER_PATTERN:-airflow[[:space:]]+celery[[:space:]]+flower}" ;;
    worker) echo "${WORKER_PATTERN}" ;;
    *) echo "" ;;
  esac
}

ensure_directory_for_file() {
  # Service commands write logs/PID files under nested folders.
  local file_path="$1"
  local dir_path
  dir_path="$(dirname "$file_path")"
  mkdir -p "$dir_path"
}

read_pid_file() {
  # Trim whitespace/newlines so PID checks stay robust.
  local pid_file="$1"
  if [[ -f "$pid_file" ]]; then
    tr -d '[:space:]' <"$pid_file"
  fi
}

is_pid_running() {
  # kill -0 checks existence/permission without sending a real signal.
  local pid="$1"
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null
}

pg_pid_from_pgctl() {
  # PostgreSQL truth source: ask pg_ctl status when possible.
  local pg_ctl_bin="${PG_HOME:-}/bin/pg_ctl"
  if [[ -z "${PG_HOME:-}" || -z "${PGDATA:-}" || ! -x "$pg_ctl_bin" ]]; then
    echo ""
    return 0
  fi

  local output pid
  if output="$("$pg_ctl_bin" -D "$PGDATA" status 2>/dev/null)"; then
    pid="$(echo "$output" | sed -n 's/.*(PID: \([0-9][0-9]*\)).*/\1/p' | head -n1)"
    echo "$pid"
    return 0
  fi

  echo ""
}

running_pid_for_service() {
  # PID discovery order:
  # 1) pg_ctl (for PG only)
  # 2) PID file
  # 3) process pattern
  local service="$1"
  local pid_file pid pattern

  if [[ "$service" == "pg" ]]; then
    pid="$(pg_pid_from_pgctl)"
    if is_pid_running "$pid"; then
      debug_log "running_pid_for_service($service): found pid=$pid from pg_ctl."
      echo "$pid"
      return 0
    fi
    debug_log "running_pid_for_service($service): no active pid from pg_ctl."
  fi

  pid_file="$(service_pid_file "$service")"
  pid="$(read_pid_file "$pid_file")"
  debug_log "running_pid_for_service($service): pid_file=$pid_file pid_from_file=${pid:-none}"
  if is_pid_running "$pid"; then
    debug_log "running_pid_for_service($service): pid=$pid is alive (from pid file)."
    echo "$pid"
    return 0
  fi

  pattern="$(service_pattern "$service")"
  if [[ -n "$pattern" ]]; then
    local matched_pids
    matched_pids="$(pgrep -f "$pattern" 2>/dev/null || true)"
    debug_log "running_pid_for_service($service): pattern=$pattern matches=${matched_pids//$'\n'/,}"
    pid="$(printf '%s\n' "$matched_pids" | head -n1 || true)"
    if is_pid_running "$pid"; then
      debug_log "running_pid_for_service($service): pid=$pid selected from pattern search."
      echo "$pid"
      return 0
    fi
  fi

  debug_log "running_pid_for_service($service): no running process detected."
  echo ""
}

show_recent_service_logs() {
  # When a service exits right after start, emit recent logs for quick diagnosis.
  local service="$1"
  local log_file line
  log_file="$(service_log_file "$service")"
  if [[ -f "$log_file" ]]; then
    log "ERROR" "Recent log tail for $service ($log_file):"
    while IFS= read -r line; do
      log "ERROR" "$line"
    done < <(tail -n 30 "$log_file")
  else
    log "ERROR" "Log file not found for $service: $log_file"
  fi
}

wait_for_service_start() {
  # Validate service survives startup; background command success alone is insufficient.
  local service="$1"
  local attempt pid

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi

  for ((attempt=1; attempt<=START_VERIFY_RETRIES; attempt++)); do
    pid="$(running_pid_for_service "$service")"
    if [[ -n "$pid" ]]; then
      debug_log "wait_for_service_start($service): running pid=$pid at attempt=$attempt/$START_VERIFY_RETRIES"
      return 0
    fi
    debug_log "wait_for_service_start($service): not running at attempt=$attempt/$START_VERIFY_RETRIES"
    if (( attempt < START_VERIFY_RETRIES )); then
      sleep "$START_VERIFY_INTERVAL_SECONDS"
    fi
  done

  log "ERROR" "$service did not stay running after startup command."
  remove_pid_file "$(service_pid_file "$service")"
  show_recent_service_logs "$service"
  return 1
}

remove_pid_file() {
  # PID cleanup prevents stale status after forced stops.
  local pid_file="$1"
  if [[ -f "$pid_file" ]]; then
    if [[ "$DRY_RUN" == "true" ]]; then
      log "DRYRUN" "rm -f \"$pid_file\""
    else
      rm -f "$pid_file"
    fi
  fi
}

stop_pid_gracefully() {
  # Two-phase stop: SIGTERM first, SIGKILL only if still alive.
  local service="$1"
  local pid="$2"

  if ! is_pid_running "$pid"; then
    return 0
  fi

  log "INFO" "Stopping $service pid=$pid with SIGTERM."
  run_shell "kill \"$pid\"" || true
  if [[ "$DRY_RUN" == "false" ]]; then
    sleep "$STOP_WAIT_SECONDS"
  fi

  if is_pid_running "$pid"; then
    log "WARN" "$service pid=$pid still running. Sending SIGKILL."
    run_shell "kill -9 \"$pid\"" || true
    if [[ "$DRY_RUN" == "false" ]]; then
      sleep "$FORCE_KILL_WAIT_SECONDS"
    fi
  fi
}

stop_by_pid_and_pattern() {
  # Stop target from PID file first, then sweep leftovers by pattern.
  local service="$1"
  local pid_file pid pattern matched_pids matched_pid

  pid_file="$(service_pid_file "$service")"
  pid="$(read_pid_file "$pid_file")"
  debug_log "stop_by_pid_and_pattern($service): pid_file=$pid_file pid_from_file=${pid:-none}"
  if [[ -n "$pid" ]]; then
    stop_pid_gracefully "$service" "$pid"
  fi

  pattern="$(service_pattern "$service")"
  if [[ -n "$pattern" ]]; then
    matched_pids="$(pgrep -f "$pattern" 2>/dev/null || true)"
    debug_log "stop_by_pid_and_pattern($service): pattern=$pattern matched=${matched_pids//$'\n'/,}"
    for matched_pid in $matched_pids; do
      if [[ "$matched_pid" != "$$" ]]; then
        stop_pid_gracefully "$service" "$matched_pid"
      fi
    done
  fi

  remove_pid_file "$pid_file"
}

start_pg() {
  # Start PostgreSQL using the exact pg_ctl style requested by user.
  if ! should_manage_pg_service; then
    log "INFO" "Skip PostgreSQL (not managed in env=$ENV_NAME)."
    return 0
  fi

  if [[ -z "${PG_HOME:-}" || -z "${PGDATA:-}" ]]; then
    log "ERROR" "PG_HOME and PGDATA must be set for pg service."
    return 1
  fi

  local pg_ctl_bin="$PG_HOME/bin/pg_ctl"
  if [[ "$DRY_RUN" != "true" && ! -x "$pg_ctl_bin" ]]; then
    log "ERROR" "pg_ctl not executable: $pg_ctl_bin"
    return 1
  fi

  local pg_log_file pg_port pg_host cmd pid
  pg_log_file="${PG_START_LOG:-$PG_HOME/pg15.log}"
  pg_port="${PG_PORT:-5432}"
  pg_host="${PG_HOST:-127.0.0.1}"
  cmd="\"$pg_ctl_bin\" -D \"$PGDATA\" -l \"$pg_log_file\" -o \"-p $pg_port -h $pg_host\" start"

  log "INFO" "Starting pg."
  run_shell "$cmd" || return 1

  if [[ "$DRY_RUN" == "false" ]]; then
    # Refresh PID file from pg_ctl output so status is accurate.
    sleep 1
    pid="$(pg_pid_from_pgctl)"
    if [[ -n "$pid" ]]; then
      local pg_pid_file
      pg_pid_file="$(service_pid_file pg)"
      ensure_directory_for_file "$pg_pid_file"
      printf '%s\n' "$pid" >"$pg_pid_file"
    fi
  fi
}

stop_pg() {
  # Try pg_ctl stop first, then fallback kill strategy for stragglers.
  if ! should_manage_pg_service; then
    log "INFO" "Skip PostgreSQL stop (not managed in env=$ENV_NAME)."
    return 0
  fi

  local pg_ctl_bin="$PG_HOME/bin/pg_ctl"
  if [[ "$DRY_RUN" != "true" && ! -x "$pg_ctl_bin" ]]; then
    log "ERROR" "pg_ctl not executable: $pg_ctl_bin"
    return 1
  fi

  log "INFO" "Stopping pg."
  run_shell "\"$pg_ctl_bin\" -D \"$PGDATA\" stop" || true
  stop_by_pid_and_pattern pg
}

start_redis() {
  # Start Redis using redis-server <conf> and track process PID.
  if [[ -z "${REDIS_DIR:-}" ]]; then
    log "ERROR" "REDIS_DIR must be set for redis service."
    return 1
  fi

  local redis_server_bin="$REDIS_DIR/src/redis-server"
  local redis_conf_file="${REDIS_CONF:-$REDIS_DIR/redis.conf}"
  if [[ "$DRY_RUN" != "true" && ! -x "$redis_server_bin" ]]; then
    log "ERROR" "redis-server not executable: $redis_server_bin"
    return 1
  fi

  local pid_file log_file cmd
  pid_file="$(service_pid_file redis)"
  log_file="$(service_log_file redis)"
  ensure_directory_for_file "$pid_file"
  ensure_directory_for_file "$log_file"

  cmd="\"$redis_server_bin\" \"$redis_conf_file\" >> \"$log_file\" 2>&1 & echo \$! > \"$pid_file\""
  log "INFO" "Starting redis."
  run_shell "$cmd"
}

stop_redis() {
  # Preferred stop path uses redis-cli SHUTDOWN with optional auth.
  if [[ -z "${REDIS_DIR:-}" ]]; then
    log "ERROR" "REDIS_DIR must be set for redis service."
    return 1
  fi

  local redis_cli_bin="$REDIS_DIR/src/redis-cli"
  if [[ "$DRY_RUN" != "true" && ! -x "$redis_cli_bin" ]]; then
    log "ERROR" "redis-cli not executable: $redis_cli_bin"
    return 1
  fi

  local cmd
  if [[ -n "${REDIS_AUTH:-}" ]]; then
    cmd="REDISCLI_AUTH=\"$REDIS_AUTH\" \"$redis_cli_bin\" -h \"$REDIS_HOST\" -p \"$REDIS_PORT\" SHUTDOWN"
  else
    cmd="\"$redis_cli_bin\" -h \"$REDIS_HOST\" -p \"$REDIS_PORT\" SHUTDOWN"
  fi

  log "INFO" "Stopping redis."
  run_shell "$cmd" || true
  stop_by_pid_and_pattern redis
}

start_airflow_daemon() {
  # Generic starter for API server/scheduler/dag-processor/triggerer.
  local service="$1"
  local sub_cmd="$2"
  local pid_file log_file cmd

  pid_file="$(service_pid_file "$service")"
  log_file="$(service_log_file "$service")"
  ensure_directory_for_file "$pid_file"
  ensure_directory_for_file "$log_file"

  cmd="$sub_cmd >> \"$log_file\" 2>&1 & echo \$! > \"$pid_file\""
  log "INFO" "Starting $service."
  run_shell "$cmd"
}

start_worker() {
  # Start worker with the exact command style requested by operations.
  # Queue list always includes the dedicated worker queue plus "default".
  local pid_file log_file queue_name cmd
  pid_file="$(service_pid_file worker)"
  log_file="$(service_log_file worker)"
  queue_name="${WORKER_QUEUE_NAME:-${WORKER_QUEUE_PREFIX}${CELERY_WORKER}}"
  if [[ ",$queue_name," != *",default,"* ]]; then
    queue_name="${queue_name},default"
  fi

  ensure_directory_for_file "$pid_file"
  ensure_directory_for_file "$log_file"

  cmd="celery -A \"$WORKER_APP\" worker --hostname \"${CELERY_WORKER}@%h\" --loglevel INFO --concurrency \"$WORKER_NM\" --queues \"$queue_name\" --events --pidfile \"$pid_file\" --logfile \"$log_file\" >> \"$log_file\" 2>&1 &"
  log "INFO" "Starting worker (worker_name=$CELERY_WORKER queue=$queue_name)."
  run_shell "$cmd"
}

start_flower() {
  # SSL behavior: include --flower-conf only when explicitly enabled.
  local pid_file log_file cmd
  pid_file="$(service_pid_file flower)"
  log_file="$(service_log_file flower)"

  ensure_directory_for_file "$pid_file"
  ensure_directory_for_file "$log_file"

  cmd="airflow celery flower"
  if [[ "$FLOWER_ENABLE_SSL" == "true" ]]; then
    cmd+=" --flower-conf \"$FLOWER_CONF\""
  fi
  cmd+=" --hostname \"$HOSTNAME_OVERRIDE\" --port \"$FLOWER_PORT\" >> \"$log_file\" 2>&1 & echo \$! > \"$pid_file\""

  log "INFO" "Starting flower (ssl=$FLOWER_ENABLE_SSL)."
  run_shell "$cmd"
}

start_service() {
  # Idempotent start: skip when a running PID is already detected.
  local service="$1"
  local pid
  local start_rc=0

  pid="$(running_pid_for_service "$service")"
  if [[ -n "$pid" ]]; then
    log "INFO" "$service is already running (pid=$pid)."
    return 0
  fi

  case "$service" in
    pg) start_pg ;;
    redis) start_redis ;;
    api-server) start_airflow_daemon api-server "airflow api-server --port \"$API_SERVER_PORT\"" ;;
    scheduler) start_airflow_daemon scheduler "airflow scheduler" ;;
    dag-processor) start_airflow_daemon dag-processor "airflow dag-processor" ;;
    triggerer) start_airflow_daemon triggerer "airflow triggerer" ;;
    flower) start_flower ;;
    worker) start_worker ;;
    *)
      log "ERROR" "Unsupported start service: $service"
      return 1
      ;;
  esac

  start_rc=$?
  if [[ "$start_rc" -ne 0 ]]; then
    return "$start_rc"
  fi

  wait_for_service_start "$service"
}

stop_service() {
  # Service-specific stop entry point.
  local service="$1"
  case "$service" in
    pg) stop_pg ;;
    redis) stop_redis ;;
    api-server|scheduler|dag-processor|triggerer|flower|worker)
      log "INFO" "Stopping $service."
      stop_by_pid_and_pattern "$service"
      ;;
    *)
      log "ERROR" "Unsupported stop service: $service"
      return 1
      ;;
  esac
}

status_for_service() {
  # Return machine-friendly "status|pid" for table rendering.
  local service="$1"
  local pid status

  if [[ "$service" == "pg" ]] && ! should_manage_pg_service; then
    echo "disabled|-"
    return 0
  fi

  pid="$(running_pid_for_service "$service")"
  if [[ -n "$pid" ]]; then
    status="running"
  else
    status="stopped"
    pid="-"
  fi

  echo "$status|$pid"
}

print_status_table() {
  # Human-readable status report required by operations team.
  local services=("$@")
  local service status_line status pid

  print_row '%-8s %-15s %-10s %-10s' "env" "service" "status" "pid"
  print_row '%-8s %-15s %-10s %-10s' "--------" "---------------" "---------" "----------"

  for service in "${services[@]}"; do
    status_line="$(status_for_service "$service")"
    status="${status_line%%|*}"
    pid="${status_line#*|}"
    print_row '%-8s %-15s %-10s %-10s' "$ENV_NAME" "$service" "$status" "$pid"
  done
}

build_service_list() {
  # Service order matters:
  # - start/status: dependencies first (pg/redis before airflow daemons)
  # - stop: reverse order to reduce cascading failures
  local mode="$1"
  local -a services=()

  if [[ "$TARGET_SERVICE" != "all-services" ]]; then
    services=("$TARGET_SERVICE")
  else
    case "$mode" in
      start|status)
        if should_manage_pg_service; then
          services+=(pg)
        fi
        services+=("${BASE_SERVICES[@]}")
        ;;
      stop)
        services=(worker flower triggerer dag-processor scheduler api-server redis)
        if should_manage_pg_service; then
          services+=(pg)
        fi
        ;;
      *)
        services+=("${BASE_SERVICES[@]}")
        ;;
    esac
  fi

  debug_log "build_service_list(mode=$mode): ${services[*]}"
  printf '%s\n' "${services[@]}"
}

execute_action() {
  # Expand requested target into a concrete service list then execute.
  local current_action="$1"
  local services_output
  local -a services=()
  local service

  services_output="$(build_service_list "$current_action")"
  while IFS= read -r service; do
    if [[ -n "$service" ]]; then
      services+=("$service")
    fi
  done <<<"$services_output"

  case "$current_action" in
    status)
      print_status_table "${services[@]}"
      ;;
    start)
      for service in "${services[@]}"; do
        if ! start_service "$service"; then
          OVERALL_RC=1
        fi
      done
      ;;
    stop)
      for service in "${services[@]}"; do
        if ! stop_service "$service"; then
          OVERALL_RC=1
        fi
      done
      ;;
    *)
      log "ERROR" "Unsupported action in execute_action: $current_action"
      OVERALL_RC=1
      ;;
  esac
}

main() {
  # Main orchestration flow:
  # parse -> validate -> load config -> prepare runtime -> execute action.
  parse_args "$@"
  validate_basic_input
  load_config
  set_defaults
  validate_target_service
  setup_runtime_logging

  trap deactivate_conda_if_requested EXIT
  activate_conda_if_requested

  case "$ACTION" in
    start)
      execute_action start
      ;;
    stop)
      execute_action stop
      ;;
    restart)
      execute_action stop
      execute_action start
      ;;
    status)
      execute_action status
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
