"""EXP: capture Spark Connect requests for SQL and DataFrame API cases.

This app uses the modern PySpark Connect client hook to persist the
ExecutePlanRequest sent by the client before Spark executes it. It compares a
literal SQL query with an equivalent DataFrame API pipeline.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

from google.protobuf.json_format import MessageToDict, MessageToJson
from pyspark.sql.connect import functions as F
from pyspark.sql.connect.session import SparkSession

from spark_platform.utils.logger import logger


APP_NAME = "apex-spark-connect-capture-exp"
REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
CAPTURE_DIR = Path(os.environ.get("EXP_CONNECT_CAPTURE_DIR", "/opt/spark/metrics/connect_capture_exp"))

SQL_LITERAL = """
SELECT
  id % 3 AS bucket_exp,
  COUNT(*) AS row_count_exp,
  SUM(id) AS id_sum_exp
FROM range(0, 12)
WHERE id >= 2
GROUP BY id % 3
ORDER BY bucket_exp
"""


def _which(message: Any, oneof_name: str) -> str:
    which = message.WhichOneof(oneof_name)
    return which or "<unset>"


def _relation_path(rel: Any) -> list[str]:
    path: list[str] = []
    current = rel
    for _ in range(20):
        rel_type = _which(current, "rel_type")
        path.append(rel_type)
        if rel_type == "sql":
            break
        if not hasattr(current, rel_type):
            break
        node = getattr(current, rel_type)
        if hasattr(node, "input") and node.HasField("input"):
            current = node.input
            continue
        break
    return path


def _extract_sql(rel: Any) -> str | None:
    current = rel
    for _ in range(20):
        rel_type = _which(current, "rel_type")
        if rel_type == "sql":
            return current.sql.query
        if not hasattr(current, rel_type):
            return None
        node = getattr(current, rel_type)
        if hasattr(node, "input") and node.HasField("input"):
            current = node.input
            continue
        return None
    return None


def _summarize_request(request: Any, case_name: str, correlation_id: str) -> dict[str, Any]:
    plan_type = _which(request.plan, "op_type")
    command_type = "<none>"
    root_type = "<none>"
    relation_path: list[str] = []
    sql_text = None
    if plan_type == "root":
        root_type = _which(request.plan.root, "rel_type")
        relation_path = _relation_path(request.plan.root)
        sql_text = _extract_sql(request.plan.root)
    elif plan_type == "command":
        command_type = _which(request.plan.command, "command_type")
        if command_type == "sql_command" and request.plan.command.sql_command.HasField("input"):
            sql_text = _extract_sql(request.plan.command.sql_command.input)
            root_type = _which(request.plan.command.sql_command.input, "rel_type")
            relation_path = _relation_path(request.plan.command.sql_command.input)

    return {
        "case_name": case_name,
        "command_type": command_type,
        "correlation_id": correlation_id,
        "session_id": request.session_id,
        "operation_id": request.operation_id,
        "user_id": request.user_context.user_id,
        "client_type": request.client_type,
        "tags": list(request.tags),
        "plan_type": plan_type,
        "root_type": root_type,
        "relation_path": relation_path,
        "sql_sha256_16": hashlib.sha256(sql_text.encode("utf-8")).hexdigest()[:16] if sql_text else None,
        "sql_text": sql_text,
    }


class CaptureHook(SparkSession.Hook):
    """Persist Connect ExecutePlanRequest before it reaches the server."""

    def on_execute_plan(self, request: Any) -> Any:
        case_name = os.environ.get("EXP_CONNECT_CASE_NAME", "unknown_case_exp")
        correlation_id = f"EXP_CONNECT_CORR case={case_name} id={uuid4()}"
        if not request.operation_id:
            request.operation_id = str(uuid4())
        request.tags.append(correlation_id)

        CAPTURE_DIR.mkdir(parents=True, exist_ok=True)
        summary = _summarize_request(request, case_name, correlation_id)
        stem = f"{summary['case_name']}__{summary['operation_id']}"

        (CAPTURE_DIR / f"{stem}.summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        (CAPTURE_DIR / f"{stem}.request.json").write_text(
            MessageToJson(request, preserving_proto_field_name=True, indent=2),
            encoding="utf-8",
        )
        (CAPTURE_DIR / f"{stem}.request_dict.json").write_text(
            json.dumps(MessageToDict(request, preserving_proto_field_name=True), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        logger.info(f"EXP connect_capture summary={summary}")
        return request


def _set_case(case_name: str) -> None:
    os.environ["EXP_CONNECT_CASE_NAME"] = case_name


def main() -> int:
    logger.set_level(os.environ.get("SPARK_PLAT_LOG_LEVEL", "INFO"))
    CAPTURE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"EXP connect remote={REMOTE}")
    logger.info(f"EXP connect capture_dir={CAPTURE_DIR}")

    spark = (
        SparkSession.builder.remote(REMOTE)
        .appName(APP_NAME)
        ._registerHook(lambda session: CaptureHook())
        .getOrCreate()
    )

    try:
        logger.info(f"EXP connect spark.version={spark.version}")

        _set_case("sql_literal_exp")
        sql_rows = spark.sql(SQL_LITERAL).collect()
        logger.info(f"EXP connect sql_literal rows={[row.asDict() for row in sql_rows]}")

        _set_case("dataframe_api_exp")
        df_rows = (
            spark.range(0, 12)
            .withColumn("bucket_exp", F.col("id") % F.lit(3))
            .where(F.col("id") >= F.lit(2))
            .groupBy("bucket_exp")
            .agg(F.count("*").alias("row_count_exp"), F.sum("id").alias("id_sum_exp"))
            .orderBy("bucket_exp")
            .collect()
        )
        logger.info(f"EXP connect dataframe_api rows={[row.asDict() for row in df_rows]}")

        _set_case("analysis_failure_exp")
        try:
            spark.sql("SELECT * FROM missing_table_for_connect_capture_exp").collect()
        except Exception as exc:  # noqa: BLE001 - expected experimental failure.
            logger.warning(f"EXP connect expected_failure type={type(exc).__name__} message={str(exc)[:320]}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
