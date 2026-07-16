#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$ROOT_DIR"

COMPOSE=(
  docker compose
  --env-file .env
  -f build/docker-compose.yml
)
SPARK_DRIVER_UI_PORT="${SPARK_DRIVER_UI_PORT:-24040}"
log_path="/tmp/task-04-early-term-$$.log"
harness_pid=""
run_id=""
stopped=false

observer_records() {
  "${COMPOSE[@]}" exec -T spark-master \
    ps -eo pid=,ppid=,pgid=,comm=,args= |
    awk -v run_id="$run_id" '
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

fallback_cleanup() {
  local records
  local pid
  local pgid
  local role
  local -a targets=()

  set +e
  if [[ -n "$harness_pid" ]] && kill -0 "$harness_pid" 2>/dev/null; then
    kill -TERM "$harness_pid" 2>/dev/null
    [[ "$stopped" == true ]] && kill -CONT "$harness_pid" 2>/dev/null
    wait "$harness_pid" 2>/dev/null
  fi
  if [[ -n "$run_id" ]]; then
    records="$(observer_records 2>/dev/null || true)"
    while read -r pid pgid role; do
      [[ -n "$pid" ]] || continue
      if [[ "$role" == "wrapper" ]]; then
        targets+=("-$pgid")
      else
        targets+=("$pid")
      fi
    done <<<"$records"
    if [[ "${#targets[@]}" -gt 0 ]]; then
      "${COMPOSE[@]}" exec -T spark-master \
        kill -KILL -- "${targets[@]}" 2>/dev/null
    fi
  fi
  rm -f "$log_path"
}
trap fallback_cleanup EXIT

set +e
curl \
  --silent \
  --output /dev/null \
  --connect-timeout 1 \
  --max-time 2 \
  "http://127.0.0.1:${SPARK_DRIVER_UI_PORT}/"
initial_http_exit=$?
set -e
printf 'initial_driver_ui_unavailable=true curl_exit=%s\n' "$initial_http_exit"
[[ "$initial_http_exit" -ne 0 ]]

: > "$log_path"
OBSERVER_ENABLED=true \
  OBSERVER_PROBE_TIMEOUT_SECONDS=90 \
  build/scripts/run-observer-live-probe.sh \
    --hold-seconds 30 \
    >"$log_path" 2>&1 &
harness_pid=$!

for _ in $(seq 1 500); do
  run_id="$(sed -n 's/^observer_run_id=//p' "$log_path" | head -n 1)"
  [[ -n "$run_id" ]] && break
  kill -0 "$harness_pid" 2>/dev/null || break
  sleep 0.01
done
[[ -n "$run_id" ]]
printf 'early_term_run_id=%s harness_pid=%s\n' "$run_id" "$harness_pid"

for _ in $(seq 1 500); do
  docker_child="$(
    ps -o pid=,comm= --ppid "$harness_pid" |
      awk '$2 == "docker" { print $1; exit }'
  )"
  if [[ -n "$docker_child" ]]; then
    kill -STOP "$harness_pid"
    stopped=true
    printf 'harness_stopped_before_driver_discovery=true docker_child=%s\n' \
      "$docker_child"
    break
  fi
  kill -0 "$harness_pid" 2>/dev/null || break
  sleep 0.005
done
[[ "$stopped" == true ]]

records_before_term=""
for _ in $(seq 1 200); do
  records_before_term="$(observer_records)"
  if awk '$3 == "wrapper" { found=1 } END { exit !found }' \
    <<<"$records_before_term"; then
    break
  fi
  sleep 0.01
done
awk '$3 == "wrapper" { found=1 } END { exit !found }' <<<"$records_before_term"
printf 'records_before_term=%s\n' "$(
  tr '\n' ',' <<<"$records_before_term" | sed 's/,$//'
)"

driver_pid_recorded_count="$(
  rg -c '^spark_driver_pid=' "$log_path" 2>/dev/null || true
)"
driver_pid_recorded_count="${driver_pid_recorded_count:-0}"
printf 'driver_pid_recorded_before_term_count=%s\n' \
  "$driver_pid_recorded_count"
[[ "$driver_pid_recorded_count" -eq 0 ]]

kill -TERM "$harness_pid"
kill -CONT "$harness_pid"
stopped=false
set +e
wait "$harness_pid"
harness_exit=$?
set -e
harness_pid=""
printf 'early_term_harness_exit=%s\n' "$harness_exit"
[[ "$harness_exit" -eq 143 ]]

sed -n '1,160p' "$log_path"
rg -q '^observer_wrapper_process_absent_after_cleanup=true$' "$log_path"
rg -q '^observer_driver_process_absent_after_cleanup=true$' "$log_path"

remaining_records="$(observer_records)"
printf 'remaining_observer_process_records=%s\n' "$remaining_records"
[[ -z "$remaining_records" ]]

set +e
curl \
  --silent \
  --output /dev/null \
  --connect-timeout 1 \
  --max-time 2 \
  "http://127.0.0.1:${SPARK_DRIVER_UI_PORT}/"
post_http_exit=$?
set -e
printf 'driver_ui_unavailable_after_early_term=true curl_exit=%s\n' \
  "$post_http_exit"
[[ "$post_http_exit" -ne 0 ]]

mapping="$("${COMPOSE[@]}" port spark-master 4040)"
printf 'spark_master_4040_mapping_persists=%s\n' "$mapping"
[[ "$mapping" == "127.0.0.1:${SPARK_DRIVER_UI_PORT}" ]]

printf 'early_term_cleanup_proof=pass\n'
