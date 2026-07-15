#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SPARK_CONTEXT="$ROOT_DIR/build/images/spark-listener-test/context"
JARS_DIR="$ROOT_DIR/build/config/spark/jars"
WHEELS_DIR="$ROOT_DIR/build/cache/python-wheels"
LISTENER_SRC="$ROOT_DIR/build/listener-test/src"

rm -rf "$SPARK_CONTEXT"
mkdir -p "$SPARK_CONTEXT/jars" "$SPARK_CONTEXT/python-wheels" "$SPARK_CONTEXT/conf" "$SPARK_CONTEXT/listener-src"

cp "$JARS_DIR"/*.jar "$SPARK_CONTEXT/jars/"
cp "$WHEELS_DIR"/* "$SPARK_CONTEXT/python-wheels/"
cat >"$ROOT_DIR/build/images/spark-listener-test/requirements.txt" <<'EOF'
pandas==2.3.3
pyarrow==21.0.0
PyYAML==6.0.2
EOF
cp "$ROOT_DIR/build/config/spark/log4j2.properties" "$SPARK_CONTEXT/conf/log4j2.properties"
cp -R "$LISTENER_SRC"/* "$SPARK_CONTEXT/listener-src/"

cp "$ROOT_DIR/build/config/spark/spark-defaults.conf" "$SPARK_CONTEXT/conf/spark-defaults.conf"
cat >>"$SPARK_CONTEXT/conf/spark-defaults.conf" <<'EOF'
spark.extraListeners com.dataship.spark.observability.SparkPlatformFirehoseListener
EOF

find "$SPARK_CONTEXT" -type f | sort
