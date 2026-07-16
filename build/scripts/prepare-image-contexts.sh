#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SPARK_CONTEXT="$ROOT_DIR/build/images/spark/context"
JARS_DIR="$ROOT_DIR/build/config/spark/jars"
WHEELS_DIR="$ROOT_DIR/build/cache/python-wheels"
OBSERVER_JAR="$ROOT_DIR/spark-observer/target/scala-2.13/dataship-spark-observer_2.13-0.1.0-SNAPSHOT.jar"

rm -rf "$SPARK_CONTEXT"
mkdir -p "$SPARK_CONTEXT/jars" "$SPARK_CONTEXT/python-wheels" "$SPARK_CONTEXT/conf"

cp "$JARS_DIR"/*.jar "$SPARK_CONTEXT/jars/"
cp "$OBSERVER_JAR" "$SPARK_CONTEXT/jars/"
cp "$WHEELS_DIR"/* "$SPARK_CONTEXT/python-wheels/"
cp "$ROOT_DIR/build/config/spark/spark-defaults.conf" "$SPARK_CONTEXT/conf/spark-defaults.conf"
cp "$ROOT_DIR/build/config/spark/log4j2.properties" "$SPARK_CONTEXT/conf/log4j2.properties"

find "$SPARK_CONTEXT" -type f | sort
