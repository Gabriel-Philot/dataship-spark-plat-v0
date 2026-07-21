from __future__ import annotations

import importlib
import os
from pathlib import Path
import re
import signal
import subprocess
import time

import pytest
import yaml


ROOT_DIR = Path(__file__).resolve().parents[1]
HARNESS_PATH = ROOT_DIR / "build/scripts/run-observer-live-probe.sh"


def _load_probe():
    return importlib.import_module("apps.observer_live_probe")


def _write_fake_runtime(tmp_path: Path) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    state_dir = tmp_path / "state"
    bin_dir.mkdir()
    state_dir.mkdir()

    docker_path = bin_dir / "docker"
    docker_path.write_text(
        """#!/usr/bin/env bash
set -euo pipefail

state_dir="${FAKE_DOCKER_STATE_DIR:?}"
joined=" $* "

if [[ "$joined" == *" port spark-master 4040 "* ]]; then
  printf '127.0.0.1:%s\\n' "${SPARK_DRIVER_UI_PORT:-24040}"
  exit 0
fi

if [[ "$joined" == *" exec -T spark-master ps -eo "* ]]; then
  if [[ -f "$state_dir/wrapper.pid" ]]; then
    wrapper_pid="$(cat "$state_dir/wrapper.pid")"
    run_id="$(cat "$state_dir/run-id")"
    if kill -0 "$wrapper_pid" 2>/dev/null; then
      printf '999 1 999 bash bash -c inspect timeout org.apache.spark.deploy.SparkSubmit %s\\n' "$run_id"
      printf '%s 1 %s timeout timeout 90 /opt/spark/bin/spark-submit --name %s\\n' \
        "$wrapper_pid" "$wrapper_pid" "$run_id"
    fi
  fi
  exit 0
fi

if [[ "$joined" == *" exec -T spark-master kill "* ]]; then
  signal_name=""
  for argument in "$@"; do
    case "$argument" in
      -TERM|-KILL) signal_name="${argument#-}" ;;
    esac
  done
  wrapper_pid="$(cat "$state_dir/wrapper.pid")"
  printf '%s:%s\\n' "$signal_name" "-$wrapper_pid" >> "$state_dir/signals.log"
  /bin/kill "-$signal_name" "$wrapper_pid" 2>/dev/null || true
  exit 0
fi

if [[ "$joined" == *" exec -T spark-master bash -c "* ]]; then
  exit 0
fi

if [[ "$joined" == *" exec -T spark-master env "* ]]; then
  if [[ "${FAKE_SUBMIT_MODE:-exit}" == "wrapper" ]]; then
    run_id=""
    for argument in "$@"; do
      case "$argument" in
        OBSERVER_RUN_ID=*) run_id="${argument#OBSERVER_RUN_ID=}" ;;
      esac
    done
    printf '%s\\n' "$run_id" > "$state_dir/run-id"
    setsid sleep 300 >/dev/null 2>&1 &
    wrapper_pid=$!
    printf '%s\\n' "$wrapper_pid" > "$state_dir/wrapper.pid"
    trap 'exit 143' TERM INT
    wait "$wrapper_pid"
    exit $?
  fi
  exit "${FAKE_SUBMIT_EXIT:-0}"
fi

printf 'unexpected fake docker invocation: %s\\n' "$*" >&2
exit 98
""",
        encoding="utf-8",
    )
    docker_path.chmod(0o755)

    curl_path = bin_dir / "curl"
    curl_path.write_text(
        """#!/usr/bin/env bash
printf '000'
exit 7
""",
        encoding="utf-8",
    )
    curl_path.chmod(0o755)
    return bin_dir, state_dir


def _fake_runtime_env(bin_dir: Path, state_dir: Path, **updates: str) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "FAKE_DOCKER_STATE_DIR": str(state_dir),
            "SPARK_DRIVER_UI_PORT": "24040",
        }
    )
    env.update(updates)
    return env


def _run_fake_harness(tmp_path: Path, **env_updates: str) -> subprocess.CompletedProcess[str]:
    bin_dir, state_dir = _write_fake_runtime(tmp_path)
    return subprocess.run(
        [str(HARNESS_PATH)],
        cwd=ROOT_DIR,
        env=_fake_runtime_env(bin_dir, state_dir, **env_updates),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10,
        check=False,
    )


def _process_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_defaults_match_the_live_probe_contract():
    probe = _load_probe()

    args = probe.parse_args([])

    assert vars(args) == {
        "rows": 40,
        "partitions": 4,
        "delay_ms": 75,
        "hold_seconds": 10,
    }


@pytest.mark.parametrize(
    ("flag", "value"),
    (
        ("--rows", "-1"),
        ("--partitions", "-1"),
        ("--delay-ms", "-1"),
        ("--hold-seconds", "-1"),
    ),
)
def test_negative_values_are_rejected(flag, value):
    probe = _load_probe()

    with pytest.raises(SystemExit) as exc_info:
        probe.parse_args([flag, value])

    assert exc_info.value.code == 2


def test_expected_result_is_small_and_deterministic():
    probe = _load_probe()

    assert probe.expected_result(rows=40, partitions=4) == {
        "rowCount": 40,
        "valueSum": 780,
        "bucketTotals": (
            (0, 180),
            (1, 190),
            (2, 200),
            (3, 210),
        ),
    }


def test_pyspark_imports_are_confined_to_runtime_main():
    source = (ROOT_DIR / "src/apps/observer_live_probe.py").read_text(
        encoding="utf-8"
    )
    main_start = source.index("def main(")

    assert "pyspark" not in source[:main_start]
    assert "from pyspark" in source[main_start:]


def test_spark_master_publishes_only_the_local_driver_ui_port():
    compose = yaml.safe_load(
        (ROOT_DIR / "build/docker-compose.yml").read_text(encoding="utf-8")
    )

    assert (
        "127.0.0.1:${SPARK_DRIVER_UI_PORT:-24040}:4040"
        in compose["services"]["spark-master"]["ports"]
    )


def test_versioned_live_harness_is_the_primary_make_target():
    makefile = (ROOT_DIR / "Makefile").read_text(encoding="utf-8")
    harness_path = ROOT_DIR / "build/scripts/run-observer-live-probe.sh"

    assert harness_path.is_file()
    harness = harness_path.read_text(encoding="utf-8")

    assert re.search(r"^observer-live:\s*$", makefile, re.MULTILINE)
    assert "build/scripts/run-observer-live-probe.sh" in makefile
    assert ".superpowers/" not in harness
    assert "OBSERVER_ENABLED" in harness
    assert "OBSERVER_RUN_ID" in harness
    assert "/tmp/" in harness
    assert "trap cleanup EXIT" in harness
    assert "trap 'exit 143' TERM" in harness
    assert "trap 'exit 130' INT" in harness
    assert "timeout" in harness
    assert "--location" in harness
    assert "native_dataship_route_absent=true" in harness
    assert "native_unknown_route" in harness
    assert "%s_http_status=%s" in harness
    assert "%{url_effective}" in harness
    assert "spark.ui.port=4040" in harness
    assert "spark.port.maxRetries=0" in harness
    assert "ps -eo" in harness
    assert "spark-master" in harness
    assert "OBSERVER_QUEUE_CAPACITY" in harness
    assert "OBSERVER_TEST_MODE" in harness
    assert "OBSERVER_TEST_PROCESSING_DELAY_MS" in harness
    assert "OBSERVER_EXPECT_DROPS" in harness


@pytest.mark.parametrize(
    ("updates", "expected_message"),
    (
        ({"OBSERVER_QUEUE_CAPACITY": "0"}, "OBSERVER_QUEUE_CAPACITY"),
        ({"OBSERVER_QUEUE_CAPACITY": "65537"}, "OBSERVER_QUEUE_CAPACITY"),
        ({"OBSERVER_TEST_MODE": "maybe"}, "OBSERVER_TEST_MODE"),
        (
            {"OBSERVER_TEST_PROCESSING_DELAY_MS": "1001"},
            "OBSERVER_TEST_PROCESSING_DELAY_MS",
        ),
        (
            {"OBSERVER_TEST_PROCESSING_DELAY_MS": "1"},
            "requires OBSERVER_TEST_MODE=true",
        ),
        ({"OBSERVER_EXPECT_DROPS": "maybe"}, "OBSERVER_EXPECT_DROPS"),
    ),
)
def test_invalid_task_7_observer_controls_fail_before_startup(
    tmp_path,
    updates,
    expected_message,
):
    result = _run_fake_harness(tmp_path, **updates)

    assert result.returncode == 2, result.stdout
    assert expected_message in result.stdout


@pytest.mark.parametrize(
    ("submit_exit", "expected_harness_exit"),
    ((0, 1), (17, 17)),
)
def test_missing_driver_pid_normalizes_submit_success_only(
    tmp_path,
    submit_exit,
    expected_harness_exit,
):
    result = _run_fake_harness(
        tmp_path,
        FAKE_SUBMIT_EXIT=str(submit_exit),
    )

    assert result.returncode == expected_harness_exit, result.stdout
    assert "Could not identify exactly one Spark driver PID" in result.stdout


@pytest.mark.parametrize(
    "zero_spelling",
    ("0", "00", "0.0", "0.00", "000.000"),
)
def test_timeout_values_reject_every_numeric_zero_spelling(
    tmp_path,
    zero_spelling,
):
    result = _run_fake_harness(
        tmp_path,
        OBSERVER_PROBE_TIMEOUT_SECONDS=zero_spelling,
    )

    assert result.returncode == 2, result.stdout
    assert "Observer timeout values must be positive numbers." in result.stdout


def test_early_term_cleans_run_id_wrapper_before_driver_discovery(tmp_path):
    bin_dir, state_dir = _write_fake_runtime(tmp_path)
    output_path = tmp_path / "harness-output.txt"
    output_stream = output_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [str(HARNESS_PATH)],
        cwd=ROOT_DIR,
        env=_fake_runtime_env(
            bin_dir,
            state_dir,
            FAKE_SUBMIT_MODE="wrapper",
        ),
        text=True,
        stdout=output_stream,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    wrapper_pid = None
    try:
        deadline = time.monotonic() + 5
        wrapper_pid_path = state_dir / "wrapper.pid"
        while time.monotonic() < deadline and not wrapper_pid_path.exists():
            time.sleep(0.05)
        assert wrapper_pid_path.exists(), "fake timeout wrapper was not started"
        wrapper_pid = int(wrapper_pid_path.read_text(encoding="utf-8"))

        process.send_signal(signal.SIGTERM)
        process.wait(timeout=15)
        output_stream.close()
        output = output_path.read_text(encoding="utf-8")

        assert process.returncode == 143, output
        assert not _process_is_running(wrapper_pid), output
        assert "observer_wrapper_process_absent_after_cleanup=true" in output
        assert "observer_driver_process_absent_after_cleanup=true" in output
        assert (state_dir / "signals.log").read_text(encoding="utf-8").splitlines() == [
            f"TERM:-{wrapper_pid}"
        ]
    finally:
        if not output_stream.closed:
            output_stream.close()
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        if wrapper_pid is not None and _process_is_running(wrapper_pid):
            try:
                os.killpg(wrapper_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
