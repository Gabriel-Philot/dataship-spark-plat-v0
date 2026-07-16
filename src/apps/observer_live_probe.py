"""Deterministic PySpark workload for inspecting the native live driver UI."""

from __future__ import annotations

import argparse
import json
import os
import time


DEFAULT_ROWS = 40
DEFAULT_PARTITIONS = 4
DEFAULT_DELAY_MS = 75
DEFAULT_HOLD_SECONDS = 10


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a deterministic Spark workload and keep its driver UI live."
    )
    parser.add_argument("--rows", type=_non_negative_int, default=DEFAULT_ROWS)
    parser.add_argument(
        "--partitions",
        type=_non_negative_int,
        default=DEFAULT_PARTITIONS,
    )
    parser.add_argument(
        "--delay-ms",
        type=_non_negative_int,
        default=DEFAULT_DELAY_MS,
    )
    parser.add_argument(
        "--hold-seconds",
        type=_non_negative_int,
        default=DEFAULT_HOLD_SECONDS,
    )
    parser.add_argument(
        "--observer-run-id",
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)
    if args.partitions == 0:
        parser.error("--partitions must be greater than zero")
    return args


def expected_result(*, rows: int, partitions: int) -> dict[str, object]:
    if rows < 0:
        raise ValueError("rows must be non-negative")
    if partitions <= 0:
        raise ValueError("partitions must be greater than zero")

    bucket_totals = tuple(
        (
            bucket,
            sum(range(bucket, rows, partitions)),
        )
        for bucket in range(min(rows, partitions))
    )
    return {
        "rowCount": rows,
        "valueSum": sum(range(rows)),
        "bucketTotals": bucket_totals,
    }


def _delayed_value(value: int, delay_ms: int) -> int:
    if delay_ms:
        time.sleep(delay_ms / 1000)
    return value


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    run_id = getattr(
        args,
        "observer_run_id",
        os.environ.get("OBSERVER_RUN_ID", "observer-live-manual"),
    )
    app_name = f"dataship-observer-live-probe-{run_id}"
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    try:
        configured_run_id = spark.conf.get(
            "spark.dataship.observer.runId",
            run_id,
        )
        if configured_run_id != run_id:
            raise AssertionError(
                "spark.dataship.observer.runId does not match --observer-run-id"
            )

        delayed_values = spark.sparkContext.parallelize(
            range(args.rows),
            args.partitions,
        ).map(lambda value: _delayed_value(value, args.delay_ms))
        value_sum = int(delayed_values.sum())

        values = spark.range(
            start=0,
            end=args.rows,
            step=1,
            numPartitions=args.partitions,
        ).selectExpr(
            "CAST(id AS BIGINT) AS value",
            f"CAST(id % {args.partitions} AS INT) AS bucket",
        )
        values.createOrReplaceTempView("observer_live_values")
        bucket_rows = spark.sql(
            """
            SELECT bucket, SUM(value) AS total
            FROM observer_live_values
            GROUP BY bucket
            ORDER BY bucket
            """
        ).collect()

        actual = {
            "rowCount": args.rows,
            "valueSum": value_sum,
            "bucketTotals": tuple(
                (int(row["bucket"]), int(row["total"])) for row in bucket_rows
            ),
        }
        expected = expected_result(
            rows=args.rows,
            partitions=args.partitions,
        )
        if actual != expected:
            raise AssertionError(
                f"deterministic result mismatch: actual={actual!r} expected={expected!r}"
            )

        print(
            "OBSERVER_LIVE_RESULT "
            + json.dumps(
                {
                    "runId": run_id,
                    **actual,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            flush=True,
        )
        if args.hold_seconds:
            time.sleep(args.hold_seconds)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
