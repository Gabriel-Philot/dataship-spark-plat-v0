import json
from urllib.error import URLError

from build.scripts.wait_spark_runtime_ready import (
    MasterStatus,
    fetch_master_status,
    wait_for_spark_runtime,
)


class FakeResponse:
    def __init__(self, status: int, payload: dict):
        self.status = status
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self) -> bytes:
        return self._body


def test_fetch_master_status_reports_unavailable_master():
    def unavailable(_url, *, timeout):
        assert timeout == 2.0
        raise URLError("connection refused")

    status = fetch_master_status(
        "http://127.0.0.1:28081",
        opener=unavailable,
    )

    assert status == MasterStatus(
        http_status=None,
        alive_workers=0,
        detail="master unavailable",
    )


def test_fetch_master_status_requires_an_alive_worker():
    def no_workers(url, *, timeout):
        assert url == "http://127.0.0.1:28081/json/"
        assert timeout == 2.0
        return FakeResponse(200, {"workers": []})

    status = fetch_master_status(
        "http://127.0.0.1:28081",
        opener=no_workers,
    )

    assert status == MasterStatus(
        http_status=200,
        alive_workers=0,
        detail="master ready; no ALIVE workers",
    )
    assert not status.ready


def test_fetch_master_status_accepts_registered_alive_worker():
    def alive_worker(_url, *, timeout):
        assert timeout == 2.0
        return FakeResponse(
            200,
            {
                "workers": [
                    {"id": "worker-dead", "state": "DEAD"},
                    {"id": "worker-alive", "state": "ALIVE"},
                ]
            },
        )

    status = fetch_master_status(
        "http://127.0.0.1:28081",
        opener=alive_worker,
    )

    assert status == MasterStatus(
        http_status=200,
        alive_workers=1,
        detail="master ready; 1 ALIVE worker",
    )
    assert status.ready


def test_wait_for_spark_runtime_times_out_without_real_sleep():
    now = [0.0]
    messages = []

    def advance(seconds):
        now[0] += seconds

    ready = wait_for_spark_runtime(
        "http://127.0.0.1:28081",
        timeout_seconds=4,
        poll_interval_seconds=2,
        fetch_status=lambda _url: MasterStatus(
            http_status=200,
            alive_workers=0,
            detail="master ready; no ALIVE workers",
        ),
        monotonic=lambda: now[0],
        sleep=advance,
        emit=messages.append,
    )

    assert not ready
    assert now[0] == 4
    assert messages[-1] == (
        "Timed out after 4 seconds waiting for Spark runtime; "
        "last observation: master ready; no ALIVE workers"
    )


def test_wait_for_spark_runtime_rejects_ready_response_after_deadline():
    now = [0.0]
    messages = []

    def ready_after_deadline(_url):
        now[0] = 5.0
        return MasterStatus(
            http_status=200,
            alive_workers=1,
            detail="master ready; 1 ALIVE worker",
        )

    ready = wait_for_spark_runtime(
        "http://127.0.0.1:28081",
        timeout_seconds=4,
        poll_interval_seconds=2,
        fetch_status=ready_after_deadline,
        monotonic=lambda: now[0],
        sleep=lambda _seconds: None,
        emit=messages.append,
    )

    assert not ready
    assert messages[-1] == (
        "Timed out after 4 seconds waiting for Spark runtime; "
        "last observation: master ready; 1 ALIVE worker"
    )
