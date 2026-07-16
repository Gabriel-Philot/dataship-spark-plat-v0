#!/usr/bin/env python3
"""Wait for the Spark Master UI and an ALIVE worker."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
import time
from collections.abc import Callable
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen


DEFAULT_TIMEOUT_SECONDS = 120.0
DEFAULT_POLL_INTERVAL_SECONDS = 2.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class MasterStatus:
    http_status: int | None
    alive_workers: int
    detail: str

    @property
    def ready(self) -> bool:
        return self.http_status == 200 and self.alive_workers > 0


def fetch_master_status(
    master_url: str,
    *,
    opener: Callable[..., Any] = urlopen,
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
) -> MasterStatus:
    status_url = f"{master_url.rstrip('/')}/json/"

    try:
        with opener(status_url, timeout=request_timeout_seconds) as response:
            http_status = int(response.status)
            if http_status != 200:
                return MasterStatus(
                    http_status=http_status,
                    alive_workers=0,
                    detail=f"master returned HTTP {http_status}",
                )
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, TimeoutError, URLError):
        return MasterStatus(
            http_status=None,
            alive_workers=0,
            detail="master unavailable",
        )
    except (AttributeError, TypeError, ValueError, UnicodeDecodeError):
        return MasterStatus(
            http_status=200,
            alive_workers=0,
            detail="master returned an invalid readiness payload",
        )

    workers = payload.get("workers") if isinstance(payload, dict) else None
    if not isinstance(workers, list):
        return MasterStatus(
            http_status=200,
            alive_workers=0,
            detail="master returned an invalid readiness payload",
        )

    alive_workers = sum(
        1
        for worker in workers
        if isinstance(worker, dict) and worker.get("state") == "ALIVE"
    )
    worker_label = "worker" if alive_workers == 1 else "workers"
    detail = (
        f"master ready; {alive_workers} ALIVE {worker_label}"
        if alive_workers
        else "master ready; no ALIVE workers"
    )
    return MasterStatus(
        http_status=200,
        alive_workers=alive_workers,
        detail=detail,
    )


def wait_for_spark_runtime(
    master_url: str,
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    fetch_status: Callable[[str], MasterStatus] = fetch_master_status,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    emit: Callable[[str], None] = print,
) -> bool:
    started_at = monotonic()

    while True:
        status = fetch_status(master_url)
        elapsed = monotonic() - started_at
        if elapsed >= timeout_seconds:
            emit(
                f"Timed out after {timeout_seconds:g} seconds waiting for "
                f"Spark runtime; last observation: {status.detail}"
            )
            return False

        if status.ready:
            emit("Spark Master UI responded HTTP 200")
            emit(
                "Spark runtime ready: "
                f"{status.alive_workers} registered worker(s) ALIVE"
            )
            return True

        sleep(min(poll_interval_seconds, timeout_seconds - elapsed))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    default_port = os.environ.get("SPARK_MASTER_UI_PORT", "28081")
    parser = argparse.ArgumentParser(
        description="Wait for Spark Master HTTP 200 and an ALIVE worker."
    )
    parser.add_argument(
        "--master-url",
        default=f"http://127.0.0.1:{default_port}",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
    )
    args = parser.parse_args(argv)
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be greater than zero")
    if args.poll_interval_seconds <= 0:
        parser.error("--poll-interval-seconds must be greater than zero")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return (
        0
        if wait_for_spark_runtime(
            args.master_url,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
