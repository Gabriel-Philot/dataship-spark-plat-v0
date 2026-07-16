#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

SBT_IMAGE="sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.10.11_2.13.16"
NODE_IMAGE="node:24.13.1-bookworm-slim"
JAR_PATH="spark-observer/target/scala-2.13/dataship-spark-observer_2.13-0.1.0-SNAPSHOT.jar"

print_final_exit_code() {
  local exit_code=$?
  printf '\nobserver_verify_exit_code=%s\n' "$exit_code"
}

trap print_final_exit_code EXIT

run_make() {
  local target="$1"
  printf '\n$ make SBT_IMAGE=%s NODE_IMAGE=%s %s\n' \
    "$SBT_IMAGE" \
    "$NODE_IMAGE" \
    "$target"
  make \
    SBT_IMAGE="$SBT_IMAGE" \
    NODE_IMAGE="$NODE_IMAGE" \
    "$target"
}

inspect_image() {
  local image="$1"
  printf '\n$ docker image inspect %s\n' "$image"
  docker image inspect "$image" \
    --format 'image={{index .RepoTags 0}} id={{.Id}} repo_digests={{json .RepoDigests}}'
}

printf 'commit=%s\n' "$(git rev-parse HEAD)"
inspect_image "$SBT_IMAGE"
inspect_image "$NODE_IMAGE"

run_make observer-tests
run_make observer-ui-tests
run_make observer-jar

printf '\n$ jar tf %s  # inside %s\n' "$JAR_PATH" "$SBT_IMAGE"
docker run --rm \
  --user "$(id -u):$(id -g)" \
  --volume "$ROOT_DIR:/workspace:ro" \
  --workdir /workspace \
  "$SBT_IMAGE" \
  bash -lc '
    set -euo pipefail
    jar tf spark-observer/target/scala-2.13/dataship-spark-observer_2.13-0.1.0-SNAPSHOT.jar |
      tee /tmp/dataship-observer-jar.txt
    if grep -Eq "^(org/apache/spark/|scala/)" /tmp/dataship-observer-jar.txt; then
      echo "FAIL: Spark or Scala classes were packaged" >&2
      exit 1
    fi
    echo "PASS: no org/apache/spark/ or scala/ entries"
  '

printf '\n$ wc -c %s\n' "$JAR_PATH"
wc -c "$JAR_PATH"

printf '\n$ sha256sum %s\n' "$JAR_PATH"
sha256sum "$JAR_PATH"

run_make tests
run_make validate
