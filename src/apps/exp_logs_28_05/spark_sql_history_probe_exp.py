"""EXP: generate controlled Spark SQL/DataFrame event-log cases.

This app is intentionally experimental. It exists to validate what Spark 4.x
persists in event logs and what the current platform can later expose through
Spark History and ClickHouse, without changing the stable sample applications.
"""

from __future__ import annotations

import hashlib

from pyspark.sql import functions as F

from spark_platform.config import load_config
from spark_platform.session import SparkSessionFactory
from spark_platform.utils.logger import logger


APP_NAME = "apex-spark-log-internals-exp"

SQL_LITERAL_NO_DESC = """
SELECT
  'sql_literal_no_desc_exp' AS case_name,
  id,
  id * 10 AS amount
FROM range(0, 6)
WHERE id IN (1, 3, 5)
ORDER BY id
"""

SQL_LITERAL_WITH_DESC = """
SELECT
  'sql_literal_with_desc_exp' AS case_name,
  id,
  id + 100 AS amount
FROM range(0, 4)
WHERE id >= 2
ORDER BY id
"""


def main() -> int:
    config = load_config()
    logger.set_level(config.get("app", {}).get("log_level", "INFO"))
    spark = SparkSessionFactory.get_or_create(config, app_name=APP_NAME)
    sc = spark.sparkContext

    logger.info(f"EXP spark.version={spark.version}")
    logger.info(f"EXP app.id={sc.applicationId}")
    logger.info(f"EXP eventLog.enabled={sc.getConf().get('spark.eventLog.enabled', '<unset>')}")
    logger.info(f"EXP eventLog.dir={sc.getConf().get('spark.eventLog.dir', '<unset>')}")

    try:
        rows = spark.sql(SQL_LITERAL_NO_DESC).collect()
        logger.info(f"EXP sql_literal_no_desc rows={len(rows)} first={rows[0].asDict() if rows else None}")

        query_hash = hashlib.sha256(SQL_LITERAL_WITH_DESC.encode("utf-8")).hexdigest()[:16]
        query_ref = f"EXP_QUERY_REF case=sql_literal_with_desc_exp sql_sha256_16={query_hash}"
        logger.info(f"EXP sql_literal_with_desc query_ref={query_ref}")
        sc.setJobDescription(query_ref)
        try:
            rows = spark.sql(SQL_LITERAL_WITH_DESC).collect()
            logger.info(f"EXP sql_literal_with_desc rows={len(rows)} first={rows[0].asDict() if rows else None}")
        finally:
            sc.setJobDescription(None)

        df_api = (
            spark.range(0, 12)
            .withColumn("bucket_exp", F.col("id") % F.lit(3))
            .where(F.col("id") >= F.lit(2))
            .groupBy("bucket_exp")
            .agg(F.count("*").alias("row_count_exp"), F.sum("id").alias("id_sum_exp"))
            .orderBy("bucket_exp")
        )
        rows = df_api.collect()
        logger.info(f"EXP dataframe_api rows={len(rows)} rows={[row.asDict() for row in rows]}")

        df_api.createOrReplaceTempView("apex_history_probe_exp_view")
        rows = spark.sql(
            """
            SELECT
              'temp_view_sql_exp' AS case_name,
              bucket_exp,
              row_count_exp,
              id_sum_exp
            FROM apex_history_probe_exp_view
            WHERE row_count_exp > 0
            ORDER BY bucket_exp
            """
        ).collect()
        logger.info(f"EXP temp_view_sql rows={len(rows)} rows={[row.asDict() for row in rows]}")

        try:
            spark.sql("SELECT * FROM missing_table_for_history_probe_exp").collect()
        except Exception as exc:  # noqa: BLE001 - this is an experiment probe.
            logger.warning(f"EXP expected_failure type={type(exc).__name__} message={str(exc)[:240]}")
    finally:
        SparkSessionFactory.stop_active()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
