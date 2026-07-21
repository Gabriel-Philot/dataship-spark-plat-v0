#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

set -a
source .env.example
source .env
set +a

COMPOSE=(
  docker compose
  --env-file .env
  -f build/docker-compose.yml
)
PLUGIN_CLASS="io.dataship.spark.observer.SparkDataShipPlugin"
SPARK_DRIVER_UI_PORT="${SPARK_DRIVER_UI_PORT:-24040}"
OBSERVER_ENABLED="${OBSERVER_ENABLED:-false}"
OBSERVER_SPARK_UI_ENABLED="${OBSERVER_SPARK_UI_ENABLED:-true}"
OBSERVER_PROBE_TIMEOUT_SECONDS="${OBSERVER_PROBE_TIMEOUT_SECONDS:-90}"
OBSERVER_UI_TIMEOUT_SECONDS="${OBSERVER_UI_TIMEOUT_SECONDS:-30}"
OBSERVER_HTTP_READ_INTERVAL_SECONDS="${OBSERVER_HTTP_READ_INTERVAL_SECONDS:-2}"
OBSERVER_QUEUE_CAPACITY="${OBSERVER_QUEUE_CAPACITY:-1024}"
OBSERVER_TEST_MODE="${OBSERVER_TEST_MODE:-false}"
OBSERVER_TEST_PROCESSING_DELAY_MS="${OBSERVER_TEST_PROCESSING_DELAY_MS:-0}"
OBSERVER_EXPECT_DROPS="${OBSERVER_EXPECT_DROPS:-false}"
OBSERVER_RUN_ID="$(
  printf 'observer-live-%(%Y%m%dT%H%M%SZ)T-%s-%s' -1 "$$" "$RANDOM"
)"
OBSERVER_TMP_DIR="/tmp/dataship-${OBSERVER_RUN_ID}"
SUBMIT_LOG="${OBSERVER_TMP_DIR}/spark-submit.log"
HOST_EXEC_PID=""
DRIVER_PID=""
OBSERVER_FINGERPRINT_INPUTS=(
  ".env.example"
  "Makefile"
  "build/docker-compose.yml"
  "build/images/spark/Dockerfile"
  "build/images/spark/requirements.txt"
  "build/scripts/assert-observer-response.py"
  "build/scripts/prepare-image-contexts.sh"
  "build/scripts/run-observer-live-probe.sh"
  "pyproject.toml"
  "spark-observer/build.sbt"
  "spark-observer/project/build.properties"
  "spark-observer/src/main/scala/io/dataship/spark/observer/BuildInfo.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipDriverPlugin.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipPlugin.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/api/CountersResponse.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/api/CountersServlet.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/api/HealthResponse.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/api/HealthServlet.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/api/JsonRenderer.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/events/BoundedEventQueue.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverCounters.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverEvent.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverListener.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverState.scala"
  "spark-observer/src/main/scala/org/apache/spark/dataship/v412/Spark412Bridge.scala"
  "spark-observer/src/test/scala/io/dataship/spark/observer/BuildInfoSpec.scala"
  "spark-observer/src/test/scala/io/dataship/spark/observer/ObserverConfigSpec.scala"
  "spark-observer/src/test/scala/io/dataship/spark/observer/PluginBootstrapSpec.scala"
  "spark-observer/src/test/scala/io/dataship/spark/observer/api/HealthResponseSpec.scala"
  "spark-observer/src/test/scala/io/dataship/spark/observer/api/CountersResponseSpec.scala"
  "spark-observer/src/test/scala/io/dataship/spark/observer/events/ObserverListenerSpec.scala"
  "src/apps/observer_live_probe.py"
  "tests/test_observer_live_probe.py"
  "tests/test_observer_response_assertions.py"
  "uv.lock"
)

observer_input_fingerprint() {
  local input_path

  for input_path in "${OBSERVER_FINGERPRINT_INPUTS[@]}"; do
    if [[ ! -f "$input_path" ]]; then
      echo "Observer fingerprint input is missing: $input_path" >&2
      return 1
    fi
  done

  {
    for input_path in "${OBSERVER_FINGERPRINT_INPUTS[@]}"; do
      sha256sum -- "$input_path"
    done
  } | sha256sum | awk '{ print $1 }'
}

active_container_image_id() {
  local service="$1"
  local container_id

  container_id="$("${COMPOSE[@]}" ps -q "$service")"
  if [[ -z "$container_id" ]]; then
    echo "No active container found for $service." >&2
    return 1
  fi
  docker inspect --format '{{.Image}}' "$container_id"
}

is_positive_number() {
  local value="$1"
  [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] &&
    [[ "$value" =~ [1-9] ]]
}

is_integer_in_range() {
  local value="$1"
  local minimum="$2"
  local maximum="$3"
  [[ "$value" =~ ^[0-9]+$ ]] &&
    ((10#$value >= minimum && 10#$value <= maximum))
}

missing_driver_exit_code() {
  local submit_exit="$1"
  if [[ "$submit_exit" -eq 0 ]]; then
    printf '1\n'
  else
    printf '%s\n' "$submit_exit"
  fi
}

case "$OBSERVER_ENABLED" in
  true|false) ;;
  *)
    echo "OBSERVER_ENABLED must be exactly true or false." >&2
    exit 2
    ;;
esac

case "$OBSERVER_SPARK_UI_ENABLED" in
  true|false) ;;
  *)
    echo "OBSERVER_SPARK_UI_ENABLED must be exactly true or false." >&2
    exit 2
    ;;
esac

case "$OBSERVER_TEST_MODE" in
  true|false) ;;
  *)
    echo "OBSERVER_TEST_MODE must be exactly true or false." >&2
    exit 2
    ;;
esac

case "$OBSERVER_EXPECT_DROPS" in
  true|false) ;;
  *)
    echo "OBSERVER_EXPECT_DROPS must be exactly true or false." >&2
    exit 2
    ;;
esac

if ! is_integer_in_range "$OBSERVER_QUEUE_CAPACITY" 1 65536; then
  echo "OBSERVER_QUEUE_CAPACITY must be an integer from 1 to 65536." >&2
  exit 2
fi

if ! is_integer_in_range "$OBSERVER_TEST_PROCESSING_DELAY_MS" 0 1000; then
  echo "OBSERVER_TEST_PROCESSING_DELAY_MS must be an integer from 0 to 1000." >&2
  exit 2
fi

if [[ "$OBSERVER_TEST_MODE" == "false" ]] &&
  [[ "$OBSERVER_TEST_PROCESSING_DELAY_MS" != "0" ]]; then
  echo "OBSERVER_TEST_PROCESSING_DELAY_MS requires OBSERVER_TEST_MODE=true." >&2
  exit 2
fi

if [[ "$OBSERVER_EXPECT_DROPS" == "true" ]] &&
  [[ "$OBSERVER_ENABLED" != "true" ]]; then
  echo "OBSERVER_EXPECT_DROPS=true requires OBSERVER_ENABLED=true." >&2
  exit 2
fi

for timeout_value in \
  "$OBSERVER_PROBE_TIMEOUT_SECONDS" \
  "$OBSERVER_UI_TIMEOUT_SECONDS" \
  "$OBSERVER_HTTP_READ_INTERVAL_SECONDS"; do
  if ! is_positive_number "$timeout_value"; then
    echo "Observer timeout values must be positive numbers." >&2
    exit 2
  fi
done

mkdir -p "$OBSERVER_TMP_DIR"

OBSERVER_GIT_HEAD="$(git rev-parse HEAD)"
OBSERVER_INPUT_FINGERPRINT_START="$(observer_input_fingerprint)"
printf 'observer_git_head=%s\n' "$OBSERVER_GIT_HEAD"
printf 'observer_fingerprint_inputs=%s\n' "$({
  IFS=,
  printf '%s' "${OBSERVER_FINGERPRINT_INPUTS[*]}"
})"
printf 'observer_input_fingerprint_start=%s\n' \
  "$OBSERVER_INPUT_FINGERPRINT_START"

find_observer_processes() {
  "${COMPOSE[@]}" exec -T spark-master \
    ps -eo pid=,ppid=,pgid=,comm=,args= |
    awk -v run_id="$OBSERVER_RUN_ID" '
      index($0, run_id) == 0 {
        next
      }
      $4 == "timeout" {
        print $1, $3, "wrapper"
        next
      }
      $4 == "java" &&
        index($0, "org.apache.spark.deploy.SparkSubmit") != 0 {
        print $1, $3, "driver"
      }
    '
}

find_driver_pids() {
  find_observer_processes | awk '$3 == "driver" { print $1 }'
}

driver_is_alive() {
  local pid="$1"
  [[ -n "$pid" ]] &&
    "${COMPOSE[@]}" exec -T spark-master \
      bash -c 'kill -0 "$1" 2>/dev/null' -- "$pid"
}

read_observer_health() {
  local read_number="$1"
  local read_label="$2"
  local deadline
  local validation_output=""
  local validation_error="${OBSERVER_TMP_DIR}/${read_label}.error"
  local expected_status="READY"
  local expected_listener_installed="$OBSERVER_ENABLED"

  if [[ "$OBSERVER_ENABLED" == "false" ]]; then
    expected_status="DISABLED"
  fi

  deadline="$(
    awk -v now="$(date +%s.%N)" -v timeout="$OBSERVER_UI_TIMEOUT_SECONDS" \
      'BEGIN { printf "%.6f", now + timeout }'
  )"
  while awk -v now="$(date +%s.%N)" -v read_deadline="$deadline" \
    'BEGIN { exit !(now < read_deadline) }'; do
    driver_is_alive "$DRIVER_PID" || return 1
    if validation_output="$(
      uv run python build/scripts/assert-observer-response.py \
        --url "http://127.0.0.1:${SPARK_DRIVER_UI_PORT}/dataship/api/v1/health" \
        --expected-status "$expected_status" \
        --expected-listener-installed "$expected_listener_installed" \
        --label "$read_label" \
        --timeout-seconds 2 \
        2>"$validation_error"
    )"; then
      printf '%s\n' "$validation_output"
      printf 'health_read_%s_pid=%s alive=true\n' \
        "$read_number" "$DRIVER_PID"
      return 0
    fi
    sleep 0.25
  done
  if [[ -s "$validation_error" ]]; then
    printf 'Last health validation error: %s\n' "$(<"$validation_error")" >&2
  fi
  return 1
}

read_observer_counters() {
  local read_number="$1"
  local read_label="$2"
  local greater_than="${3:-}"
  local deadline
  local validation_output=""
  local validation_error="${OBSERVER_TMP_DIR}/${read_label}.error"
  local -a threshold_args=()

  if [[ -n "$greater_than" ]]; then
    threshold_args+=(--listener-received-greater-than "$greater_than")
  fi
  if [[ "$OBSERVER_EXPECT_DROPS" == "true" ]] && [[ "$read_number" == "2" ]]; then
    threshold_args+=(--minimum-dropped-by-plugin 1)
  fi

  deadline="$(
    awk -v now="$(date +%s.%N)" -v timeout="$OBSERVER_UI_TIMEOUT_SECONDS" \
      'BEGIN { printf "%.6f", now + timeout }'
  )"
  while awk -v now="$(date +%s.%N)" -v read_deadline="$deadline" \
    'BEGIN { exit !(now < read_deadline) }'; do
    driver_is_alive "$DRIVER_PID" || return 1
    if validation_output="$(
      uv run python build/scripts/assert-observer-response.py \
        --response-kind counters \
        --url "http://127.0.0.1:${SPARK_DRIVER_UI_PORT}/dataship/api/v1/debug/counters" \
        --label "$read_label" \
        --timeout-seconds 2 \
        "${threshold_args[@]}" \
        2>"$validation_error"
    )"; then
      printf '%s\n' "$validation_output"
      COUNTERS_LAST_RECEIVED="$(
        awk -F'[= ]' -v label="${read_label}_listener_received" \
          '$1 == label { print $2; exit }' <<<"$validation_output"
      )"
      [[ "$COUNTERS_LAST_RECEIVED" =~ ^[0-9]+$ ]] || return 1
      printf 'counters_read_%s_pid=%s alive=true\n' \
        "$read_number" "$DRIVER_PID"
      return 0
    fi
    sleep 0.25
  done
  if [[ -s "$validation_error" ]]; then
    printf 'Last counters validation error: %s\n' \
      "$(<"$validation_error")" >&2
  fi
  return 1
}

inspect_native_route() {
  local label="$1"
  local path="$2"
  local headers_path="${OBSERVER_TMP_DIR}/${label}.headers"
  local final_result

  ROUTE_HTTP_STATUS="$(
    curl \
      --silent \
      --show-error \
      --dump-header "$headers_path" \
      --output /dev/null \
      --write-out '%{http_code}' \
      --connect-timeout 1 \
      --max-time 2 \
      "http://127.0.0.1:${SPARK_DRIVER_UI_PORT}${path}" \
      || true
  )"
  ROUTE_LOCATION="$(
    awk '
      tolower($1) == "location:" {
        sub(/\r$/, "", $2)
        print $2
        exit
      }
    ' "$headers_path"
  )"
  final_result="$(
    curl \
      --location \
      --silent \
      --show-error \
      --output /dev/null \
      --write-out '%{http_code}|%{url_effective}' \
      --connect-timeout 1 \
      --max-time 2 \
      "http://127.0.0.1:${SPARK_DRIVER_UI_PORT}${path}" \
      || true
  )"
  ROUTE_FINAL_STATUS="${final_result%%|*}"
  ROUTE_FINAL_URL="${final_result#*|}"
}

verify_native_route_absence() {
  local expected_jobs_url="http://127.0.0.1:${SPARK_DRIVER_UI_PORT}/jobs/"
  local deadline
  local label
  local path

  deadline="$(
    awk -v now="$(date +%s.%N)" -v timeout="$OBSERVER_UI_TIMEOUT_SECONDS" \
      'BEGIN { printf "%.6f", now + timeout }'
  )"
  while awk -v now="$(date +%s.%N)" -v route_deadline="$deadline" \
    'BEGIN { exit !(now < route_deadline) }'; do
    driver_is_alive "$DRIVER_PID" || return 1
    inspect_native_route native_dataship_route /dataship/
    if [[ "$ROUTE_HTTP_STATUS" == "302" ]] \
      && [[ "$ROUTE_LOCATION" == "$expected_jobs_url" ]] \
      && [[ "$ROUTE_FINAL_STATUS" == "200" ]] \
      && [[ "$ROUTE_FINAL_URL" == "$expected_jobs_url" ]]; then
      break
    fi
    sleep 0.25
  done

  [[ "$ROUTE_HTTP_STATUS" == "302" ]]
  [[ "$ROUTE_LOCATION" == "$expected_jobs_url" ]]
  [[ "$ROUTE_FINAL_STATUS" == "200" ]]
  [[ "$ROUTE_FINAL_URL" == "$expected_jobs_url" ]]
  printf '%s_http_status=%s location=%s final_status=%s final_url=%s\n' \
    native_dataship_route \
    "$ROUTE_HTTP_STATUS" \
    "$ROUTE_LOCATION" \
    "$ROUTE_FINAL_STATUS" \
    "$ROUTE_FINAL_URL"

  label="native_unknown_route"
  path="/observer-unknown-${OBSERVER_RUN_ID}/"
  inspect_native_route "$label" "$path"
  [[ "$ROUTE_HTTP_STATUS" == "302" ]]
  [[ "$ROUTE_LOCATION" == "$expected_jobs_url" ]]
  [[ "$ROUTE_FINAL_STATUS" == "200" ]]
  [[ "$ROUTE_FINAL_URL" == "$expected_jobs_url" ]]
  printf '%s_http_status=%s location=%s final_status=%s final_url=%s\n' \
    "$label" \
    "$ROUTE_HTTP_STATUS" \
    "$ROUTE_LOCATION" \
    "$ROUTE_FINAL_STATUS" \
    "$ROUTE_FINAL_URL"

  printf 'native_dataship_route_absent=true semantics=native-unknown-redirect\n'
}

signal_observer_processes() {
  local signal_name="$1"
  local records="$2"
  local pid
  local pgid
  local role
  local -a group_targets=()
  local -a direct_driver_pids=()
  local -A wrapper_groups=()

  while read -r pid pgid role; do
    [[ -n "$pid" ]] || continue
    if [[ "$role" == "wrapper" ]]; then
      wrapper_groups["$pgid"]=1
    fi
  done <<<"$records"

  for pgid in "${!wrapper_groups[@]}"; do
    group_targets+=("-$pgid")
  done

  while read -r pid pgid role; do
    [[ "$role" == "driver" ]] || continue
    if [[ -z "${wrapper_groups[$pgid]+present}" ]]; then
      direct_driver_pids+=("$pid")
    fi
  done <<<"$records"

  if [[ "${#group_targets[@]}" -gt 0 ]]; then
    "${COMPOSE[@]}" exec -T spark-master \
      kill "-${signal_name}" -- "${group_targets[@]}" 2>/dev/null || true
  fi
  if [[ "${#direct_driver_pids[@]}" -gt 0 ]]; then
    "${COMPOSE[@]}" exec -T spark-master \
      kill "-${signal_name}" -- "${direct_driver_pids[@]}" 2>/dev/null || true
  fi
}

cleanup_observer_processes() {
  local records
  local deadline

  records="$(find_observer_processes 2>/dev/null || true)"
  [[ -z "$records" ]] && return 0

  printf 'cleanup_observer_processes=%s\n' "$(
    awk '{ printf "%s%s:%s/pgid:%s", separator, $3, $1, $2; separator="," }' \
      <<<"$records"
  )"
  signal_observer_processes TERM "$records"

  deadline=$((SECONDS + 10))
  while [[ "$SECONDS" -lt "$deadline" ]]; do
    records="$(find_observer_processes 2>/dev/null || true)"
    if [[ -z "$records" ]]; then
      printf 'observer_wrapper_process_absent_after_cleanup=true\n'
      printf 'observer_driver_process_absent_after_cleanup=true\n'
      return 0
    fi
    sleep 0.25
  done

  signal_observer_processes KILL "$records"

  deadline=$((SECONDS + 2))
  while [[ "$SECONDS" -lt "$deadline" ]]; do
    records="$(find_observer_processes 2>/dev/null || true)"
    if [[ -z "$records" ]]; then
      printf 'observer_wrapper_process_absent_after_cleanup=true\n'
      printf 'observer_driver_process_absent_after_cleanup=true\n'
      return 0
    fi
    sleep 0.25
  done

  echo "Observer processes remain after cleanup: $records" >&2
  return 1
}

cleanup() {
  local exit_code=$?
  local cleanup_exit=0
  trap - EXIT TERM INT
  set +e
  cleanup_observer_processes
  cleanup_exit=$?
  if [[ -n "$HOST_EXEC_PID" ]] && kill -0 "$HOST_EXEC_PID" 2>/dev/null; then
    kill -TERM "$HOST_EXEC_PID" 2>/dev/null
    wait "$HOST_EXEC_PID" 2>/dev/null
  fi
  rm -rf "$OBSERVER_TMP_DIR"
  if [[ "$cleanup_exit" -ne 0 ]] && [[ "$exit_code" -eq 0 ]]; then
    exit_code=1
  fi
  printf 'observer_live_exit_code=%s\n' "$exit_code"
  exit "$exit_code"
}

trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

mapping="$("${COMPOSE[@]}" port spark-master 4040)"
if [[ "$mapping" != "127.0.0.1:${SPARK_DRIVER_UI_PORT}" ]]; then
  echo "Unexpected spark-master 4040 mapping: $mapping" >&2
  exit 1
fi

set +e
initial_http_status="$(
  curl \
    --silent \
    --output /dev/null \
    --write-out '%{http_code}' \
    --connect-timeout 1 \
    --max-time 2 \
    "http://127.0.0.1:${SPARK_DRIVER_UI_PORT}/"
)"
initial_http_exit=$?
set -e
if [[ "$initial_http_exit" -eq 0 ]]; then
  echo "Driver UI endpoint is already responding before the probe." >&2
  exit 1
fi

submit_args=(
  timeout
  --signal=TERM
  --kill-after=10
  "$OBSERVER_PROBE_TIMEOUT_SECONDS"
  /opt/spark/bin/spark-submit
  --master spark://spark-master:7077
  --deploy-mode client
  --name "dataship-observer-live-probe-${OBSERVER_RUN_ID}"
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src
  --conf spark.ui.port=4040
  --conf spark.port.maxRetries=0
  --conf "spark.ui.enabled=${OBSERVER_SPARK_UI_ENABLED}"
  --conf "spark.dataship.observer.runId=${OBSERVER_RUN_ID}"
  --conf "spark.dataship.observer.queue.capacity=${OBSERVER_QUEUE_CAPACITY}"
  --conf "spark.dataship.observer.testMode=${OBSERVER_TEST_MODE}"
  --conf "spark.dataship.observer.test.processingDelayMs=${OBSERVER_TEST_PROCESSING_DELAY_MS}"
  --conf "spark.plugins=${PLUGIN_CLASS}"
)
if [[ "$OBSERVER_ENABLED" == "true" ]]; then
  submit_args+=(--conf spark.dataship.observer.enabled=true)
else
  submit_args+=(--conf spark.dataship.observer.enabled=false)
fi
submit_args+=(
  /opt/spark/src/apps/observer_live_probe.py
  --observer-run-id "$OBSERVER_RUN_ID"
  "$@"
)

printf 'observer_run_id=%s\n' "$OBSERVER_RUN_ID"
printf 'observer_enabled=%s\n' "$OBSERVER_ENABLED"
printf 'observer_spark_ui_enabled=%s\n' "$OBSERVER_SPARK_UI_ENABLED"
printf 'observer_queue_capacity=%s\n' "$OBSERVER_QUEUE_CAPACITY"
printf 'observer_test_mode=%s\n' "$OBSERVER_TEST_MODE"
printf 'observer_test_processing_delay_ms=%s\n' \
  "$OBSERVER_TEST_PROCESSING_DELAY_MS"
printf 'observer_expect_drops=%s\n' "$OBSERVER_EXPECT_DROPS"
printf 'spark_master_4040_mapping=%s\n' "$mapping"
printf 'initial_driver_ui_http_exit=%s status=%s\n' \
  "$initial_http_exit" "$initial_http_status"

"${COMPOSE[@]}" exec -T spark-master \
  env \
  OBSERVER_RUN_ID="$OBSERVER_RUN_ID" \
  PYTHONPATH=/opt/spark/src \
  "${submit_args[@]}" \
  > >(tee "$SUBMIT_LOG") 2>&1 &
HOST_EXEC_PID=$!
printf 'host_compose_exec_pid=%s\n' "$HOST_EXEC_PID"

pid_deadline=$((SECONDS + 20))
while [[ "$SECONDS" -lt "$pid_deadline" ]]; do
  mapfile -t driver_pids < <(find_driver_pids)
  if [[ "${#driver_pids[@]}" -eq 1 ]]; then
    DRIVER_PID="${driver_pids[0]}"
    break
  fi
  if ! kill -0 "$HOST_EXEC_PID" 2>/dev/null; then
    break
  fi
  sleep 0.25
done
if [[ -z "$DRIVER_PID" ]]; then
  echo "Could not identify exactly one Spark driver PID for $OBSERVER_RUN_ID." >&2
  set +e
  wait "$HOST_EXEC_PID"
  submit_exit=$?
  set -e
  exit "$(missing_driver_exit_code "$submit_exit")"
fi
printf 'spark_driver_pid=%s\n' "$DRIVER_PID"
SPARK_MASTER_IMAGE_ID="$(active_container_image_id spark-master)"
SPARK_WORKER_IMAGE_ID="$(active_container_image_id spark-worker)"
printf 'spark_master_image_id=%s\n' "$SPARK_MASTER_IMAGE_ID"
printf 'spark_worker_image_id=%s\n' "$SPARK_WORKER_IMAGE_ID"

if [[ "$OBSERVER_SPARK_UI_ENABLED" == "true" ]]; then
  if [[ "$OBSERVER_ENABLED" == "true" ]]; then
    if ! read_observer_counters 1 "counters_read_1"; then
      echo "Counters read 1 did not return a valid response while the driver was alive." >&2
      set +e
      wait "$HOST_EXEC_PID"
      submit_exit=$?
      set -e
      [[ "$submit_exit" -ne 0 ]] && exit "$submit_exit"
      exit 1
    fi
    COUNTERS_T1_RECEIVED="$COUNTERS_LAST_RECEIVED"
  fi
  if ! read_observer_health 1 "health_read_1"; then
    echo "Health read 1 did not return a valid response while the driver was alive." >&2
    set +e
    wait "$HOST_EXEC_PID"
    submit_exit=$?
    set -e
    [[ "$submit_exit" -ne 0 ]] && exit "$submit_exit"
    exit 1
  fi
  sleep "$OBSERVER_HTTP_READ_INTERVAL_SECONDS"
  if ! read_observer_health 2 "health_read_2"; then
    echo "Health read 2 did not return a valid response while the driver was alive." >&2
    set +e
    wait "$HOST_EXEC_PID"
    submit_exit=$?
    set -e
    [[ "$submit_exit" -ne 0 ]] && exit "$submit_exit"
    exit 1
  fi
  if [[ "$OBSERVER_ENABLED" == "true" ]]; then
    if ! read_observer_counters \
      2 "counters_read_2" "$COUNTERS_T1_RECEIVED"; then
      echo "Counters read 2 did not prove live growth while the driver was alive." >&2
      set +e
      wait "$HOST_EXEC_PID"
      submit_exit=$?
      set -e
      [[ "$submit_exit" -ne 0 ]] && exit "$submit_exit"
      exit 1
    fi
    printf 'listener_received_growth=true t1=%s t2=%s\n' \
      "$COUNTERS_T1_RECEIVED" "$COUNTERS_LAST_RECEIVED"
  fi

  verify_native_route_absence
else
  printf 'health_endpoint_not_promised_without_spark_ui=true pid=%s alive=true\n' \
    "$DRIVER_PID"
fi

set +e
wait "$HOST_EXEC_PID"
submit_exit=$?
set -e
HOST_EXEC_PID=""
printf 'spark_submit_exit_code=%s\n' "$submit_exit"

if [[ "$OBSERVER_SPARK_UI_ENABLED" == "false" ]]; then
  if ! grep -Fq 'code=NO_SPARK_UI' "$SUBMIT_LOG"; then
    echo "The no-UI run did not log the stable NO_SPARK_UI code." >&2
    exit 1
  fi
  printf 'no_spark_ui_error_code=NO_SPARK_UI\n'
fi

cleanup_observer_processes

remaining_processes="$(find_observer_processes 2>/dev/null || true)"
if [[ -n "$remaining_processes" ]]; then
  echo "Observer processes remain after cleanup: $remaining_processes" >&2
  exit 1
fi
printf 'driver_process_absent_after_run=true\n'
printf 'wrapper_process_absent_after_run=true\n'

set +e
post_http_status="$(
  curl \
    --silent \
    --output /dev/null \
    --write-out '%{http_code}' \
    --connect-timeout 1 \
    --max-time 2 \
    "http://127.0.0.1:${SPARK_DRIVER_UI_PORT}/"
)"
post_http_exit=$?
set -e
if [[ "$post_http_exit" -eq 0 ]]; then
  echo "Driver UI endpoint still responds after the driver exited." >&2
  exit 1
fi
printf 'driver_ui_unavailable_after_run=true curl_exit=%s status=%s\n' \
  "$post_http_exit" "$post_http_status"

mapping_after="$("${COMPOSE[@]}" port spark-master 4040)"
[[ "$mapping_after" == "$mapping" ]]
printf 'spark_master_4040_mapping_persists=%s\n' "$mapping_after"

OBSERVER_INPUT_FINGERPRINT_END="$(observer_input_fingerprint)"
printf 'observer_input_fingerprint_end=%s\n' "$OBSERVER_INPUT_FINGERPRINT_END"
if [[ "$OBSERVER_INPUT_FINGERPRINT_END" != "$OBSERVER_INPUT_FINGERPRINT_START" ]]; then
  echo "Observer input fingerprint changed during the live run." >&2
  exit 1
fi
printf 'observer_input_fingerprints_match=true\n'

exit "$submit_exit"
