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
JAR_PATH="spark-observer/target/scala-2.13/dataship-spark-observer_2.13-0.1.0-SNAPSHOT.jar"
CONTAINER_JAR_PATH="/opt/spark/jars/dataship-spark-observer_2.13-0.1.0-SNAPSHOT.jar"
PLUGIN_CLASS="io.dataship.spark.observer.SparkDataShipPlugin"
SPARK_DRIVER_UI_PORT="${SPARK_DRIVER_UI_PORT:-24040}"

TASK3_FILES=(
  "Makefile"
  "build/scripts/prepare-image-contexts.sh"
  "build/scripts/wait_spark_runtime_ready.py"
  "build/scripts/verify-observer-bootstrap.sh"
  "spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipDriverPlugin.scala"
  "spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipPlugin.scala"
  "spark-observer/src/test/scala/io/dataship/spark/observer/ObserverConfigSpec.scala"
  "spark-observer/src/test/scala/io/dataship/spark/observer/PluginBootstrapSpec.scala"
  "tests/test_observer_platform_contract.py"
  "tests/test_spark_runtime_readiness.py"
)

print_final_exit_code() {
  local exit_code=$?
  printf '\nobserver_bootstrap_verify_exit_code=%s\n' "$exit_code"
}

trap print_final_exit_code EXIT

compute_task3_fingerprint() {
  local file

  for file in "${TASK3_FILES[@]}"; do
    if [[ ! -f "$file" ]]; then
      echo "Missing Task 3 fingerprint input: $file" >&2
      return 1
    fi
  done

  {
    for file in "${TASK3_FILES[@]}"; do
      sha256sum "$file"
    done
  } | sha256sum | awk '{print $1}'
}

run_make() {
  local target="$1"
  printf '\n$ make %s\n' "$target"
  make "$target"
}

run_uv() {
  printf '\n$ uv run'
  printf ' %q' "$@"
  printf '\n'
  uv run "$@"
}

inspect_runtime_image() {
  local image="$1"
  printf '\n$ docker image inspect %s\n' "$image"
  docker image inspect "$image" \
    --format 'spark_runtime_image={{index .RepoTags 0}} id={{.Id}} repo_digests={{json .RepoDigests}}'
}

verify_jar_checksum() {
  local host_jar="$1"
  local container_jar="$2"
  local host_checksum
  local container_checksum

  printf '\n$ compare host and spark-master Observer JAR checksums\n'
  host_checksum="$(sha256sum "$host_jar" | awk '{print $1}')"
  container_checksum="$(
    "${COMPOSE[@]}" exec -T spark-master \
      sha256sum "$container_jar" |
      awk '{print $1}'
  )"
  printf 'host_jar_sha256=%s\n' "$host_checksum"
  printf 'container_jar_sha256=%s\n' "$container_checksum"
  [[ "$host_checksum" == "$container_checksum" ]]
}

run_plugin_on_sanity() {
  local plugin_class="$1"

  printf '\n$ plugin-on check_sanity.py\n'
  "${COMPOSE[@]}" exec -T spark-master \
    env PYTHONPATH=/opt/spark/src \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/src \
    --conf "spark.plugins=$plugin_class" \
    --conf spark.dataship.observer.enabled=true \
    /opt/spark/src/apps/sample_scripts/check_sanity.py
}

verify_absence() {
  local driver_port="$1"
  local mapping_output
  local mapping_code
  local route_code

  printf '\n$ verify Task 3 has no driver mapping or /dataship route\n'
  set +e
  mapping_output="$("${COMPOSE[@]}" port spark-master 4040 2>&1)"
  mapping_code=$?
  set -e
  printf '%s\n' "$mapping_output"
  printf 'spark_master_4040_mapping_absent_exit=%s\n' "$mapping_code"
  [[ "$mapping_code" -eq 1 ]]

  set +e
  curl \
    --fail \
    --silent \
    --show-error \
    --connect-timeout 3 \
    "http://127.0.0.1:${driver_port}/dataship/"
  route_code=$?
  set -e
  printf 'dataship_route_absent_exit=%s\n' "$route_code"
  [[ "$route_code" -eq 7 ]]
}

START_COMMIT="$(git rev-parse HEAD)"
START_FINGERPRINT="$(compute_task3_fingerprint)"
printf 'task3_commit_start=%s\n' "$START_COMMIT"
for file in "${TASK3_FILES[@]}"; do
  printf 'task3_fingerprint_file=%s\n' "$file"
done
printf 'task3_fingerprint_start=%s\n' "$START_FINGERPRINT"

run_uv pytest tests/test_spark_runtime_readiness.py -q
run_make observer-tests
run_make observer-runtime-refresh
inspect_runtime_image "$SPARK_RUNTIME_IMAGE"
verify_jar_checksum "$JAR_PATH" "$CONTAINER_JAR_PATH"
run_make compose
run_make smoke
run_plugin_on_sanity "$PLUGIN_CLASS"
verify_absence "$SPARK_DRIVER_UI_PORT"
run_make tests
run_make validate
run_make observer-ui-tests
run_make observer-verify

END_COMMIT="$(git rev-parse HEAD)"
END_FINGERPRINT="$(compute_task3_fingerprint)"
printf 'task3_commit_end=%s\n' "$END_COMMIT"
printf 'task3_fingerprint_end=%s\n' "$END_FINGERPRINT"
[[ "$END_COMMIT" == "$START_COMMIT" ]] || {
  echo "Task 3 verification changed Git HEAD." >&2
  exit 1
}
[[ "$END_FINGERPRINT" == "$START_FINGERPRINT" ]] || {
  echo "Task 3 fingerprint changed during verification." >&2
  exit 1
}

printf 'observer_bootstrap_verify_status=PASS\n'
