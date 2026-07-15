"""EXP: validate a JVM Spark listener loaded through spark.extraListeners."""

from __future__ import annotations

from pyspark.sql import functions as F

from spark_platform.config import load_config
from spark_platform.session import SparkSessionFactory
from spark_platform.utils.logger import logger


APP_NAME = "apex-spark-listener-exp"


def main() -> int:
    config = load_config()
    logger.set_level(config.get("app", {}).get("log_level", "INFO"))
    spark = SparkSessionFactory.get_or_create(config, app_name=APP_NAME)
    sc = spark.sparkContext

    logger.info(f"EXP listener spark.version={spark.version}")
    logger.info(f"EXP listener app.id={sc.applicationId}")
    logger.info(f"EXP listener extraListeners={sc.getConf().get('spark.extraListeners', '<unset>')}")

    try:
        sc.setJobDescription("EXP_LISTENER_REF case=sql_literal")
        rows = spark.sql(
            """
            SELECT
              'listener_sql_literal_exp' AS case_name,
              id,
              id * 2 AS amount
            FROM range(0, 5)
            WHERE id >= 2
            ORDER BY id
            """
        ).collect()
        logger.info(f"EXP listener sql rows={len(rows)} first={rows[0].asDict() if rows else None}")

        sc.setJobDescription("EXP_LISTENER_REF case=dataframe_api")
        df = (
            spark.range(0, 10)
            .withColumn("bucket_exp", F.col("id") % F.lit(2))
            .groupBy("bucket_exp")
            .agg(F.count("*").alias("row_count_exp"))
            .orderBy("bucket_exp")
        )
        rows = df.collect()
        logger.info(f"EXP listener dataframe rows={len(rows)} rows={[row.asDict() for row in rows]}")
    finally:
        sc.setJobDescription(None)
        SparkSessionFactory.stop_active()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
