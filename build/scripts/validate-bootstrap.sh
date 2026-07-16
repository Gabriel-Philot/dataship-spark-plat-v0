#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
JARS_DIR="$ROOT_DIR/build/config/spark/jars"
MANIFEST="$JARS_DIR/.bootstrap-manifest"
REQ_FILE="$ROOT_DIR/build/images/spark/requirements.txt"
WHEELS_DIR="$ROOT_DIR/build/cache/python-wheels"
WHEELS_MANIFEST="$WHEELS_DIR/.requirements.sha256"
SBT_CACHE_DIR="$ROOT_DIR/build/cache/sbt"
COURSIER_CACHE_DIR="$ROOT_DIR/build/cache/coursier"
OBSERVER_CACHE_MARKER="$SBT_CACHE_DIR/.observer-bootstrap.sha256"

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  echo "Missing .env. Run: make bootstrap" >&2
  exit 1
fi

set -a
source "$ROOT_DIR/.env.example"
source "$ROOT_DIR/.env"
set +a

observer_cache_fingerprint() {
  local build_sbt_hash
  local build_properties_hash
  build_sbt_hash="$(sha256sum "$ROOT_DIR/spark-observer/build.sbt" | awk '{print $1}')"
  build_properties_hash="$(sha256sum "$ROOT_DIR/spark-observer/project/build.properties" | awk '{print $1}')"
  printf '%s\n' \
    "SBT_IMAGE=$SBT_IMAGE" \
    "spark-observer/build.sbt=$build_sbt_hash" \
    "spark-observer/project/build.properties=$build_properties_hash" \
    | sha256sum \
    | awk '{print $1}'
}

validate_cache_subtree() {
  local cache_dir="$1"
  if [[ ! -d "$cache_dir" || -z "$(find "$cache_dir" -mindepth 1 -print -quit)" ]]; then
    echo "Missing or empty Observer build cache: $cache_dir" >&2
    echo "Run: make bootstrap" >&2
    exit 1
  fi
}

for image in "$SPARK_BASE_IMAGE" "$GO_BASE_IMAGE" "$MINIO_BASE_IMAGE" "$MINIO_MC_BASE_IMAGE" "$CLICKHOUSE_BASE_IMAGE" "$SBT_IMAGE" "$NODE_IMAGE"; do
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Missing bootstrapped base image: $image" >&2
    echo "Run: make bootstrap" >&2
    exit 1
  fi
done

if [[ ! -f "$OBSERVER_CACHE_MARKER" ]]; then
  echo "Missing Observer cache bootstrap marker. Run: make bootstrap" >&2
  exit 1
fi

OBSERVER_CACHE_FINGERPRINT="$(observer_cache_fingerprint)"
if [[ "$(cat "$OBSERVER_CACHE_MARKER")" != "$OBSERVER_CACHE_FINGERPRINT" ]]; then
  echo "Stale Observer build cache. Run: make bootstrap" >&2
  exit 1
fi

validate_cache_subtree "$SBT_CACHE_DIR/boot"
validate_cache_subtree "$COURSIER_CACHE_DIR/https"

if [[ ! -f "$MANIFEST" ]]; then
  echo "Missing jar bootstrap manifest. Run: make bootstrap" >&2
  exit 1
fi

while IFS= read -r jar_name; do
  [[ -z "$jar_name" ]] && continue
  if [[ ! -f "$JARS_DIR/$jar_name" ]]; then
    echo "Missing bootstrapped jar: $jar_name" >&2
    exit 1
  fi
done < "$MANIFEST"

requirements_hash="$(sha256sum "$REQ_FILE" | awk '{print $1}')"
if [[ ! -f "$WHEELS_MANIFEST" || "$(cat "$WHEELS_MANIFEST")" != "$requirements_hash" ]]; then
  echo "Missing or stale Python wheel cache. Run: make bootstrap" >&2
  exit 1
fi

if ! find "$WHEELS_DIR" -maxdepth 1 -type f \( -name '*.whl' -o -name '*.tar.gz' \) | grep -q .; then
  echo "Python wheel cache is empty. Run: make bootstrap" >&2
  exit 1
fi

if [[ ! -d "$ROOT_DIR/build/images/eventlog-loader/vendor" || ! -f "$ROOT_DIR/build/images/eventlog-loader/go.sum" ]]; then
  echo "Missing vendored Go dependencies. Run: make bootstrap" >&2
  exit 1
fi
