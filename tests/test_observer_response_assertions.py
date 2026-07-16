from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
from types import SimpleNamespace

import pytest


ROOT_DIR = Path(__file__).resolve().parents[1]
HELPER_PATH = ROOT_DIR / "build/scripts/assert-observer-response.py"
HARNESS_PATH = ROOT_DIR / "build/scripts/run-observer-live-probe.sh"


def _load_helper():
    if not HELPER_PATH.is_file():
        class MissingResponseValidationError(ValueError):
            pass

        def missing_validation(*_args, **_kwargs):
            raise MissingResponseValidationError(
                "Health response validation is not implemented."
            )

        return SimpleNamespace(
            ResponseValidationError=MissingResponseValidationError,
            validate_response=missing_validation,
        )

    spec = importlib.util.spec_from_file_location(
        "assert_observer_response", HELPER_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _valid_payload() -> dict[str, object]:
    return {
        "schemaVersion": "v1",
        "pluginVersion": "0.1.0-SNAPSHOT",
        "sparkVersion": "4.1.2",
        "appId": "app-health-test",
        "mode": "live",
        "capturedAt": "2026-07-16T12:34:56Z",
        "status": "READY",
        "uiAttached": True,
        "listenerInstalled": False,
        "supportedRuntime": True,
        "queueCapacity": 1024,
        "lastErrorCode": "",
    }


def test_live_harness_uses_the_versioned_health_response_validator():
    harness = HARNESS_PATH.read_text(encoding="utf-8")

    assert "assert-observer-response.py" in harness
    assert "health_read_1" in harness
    assert "health_read_2" in harness


def test_live_harness_fingerprints_explicit_non_secret_inputs_and_runtime_identity():
    harness = HARNESS_PATH.read_text(encoding="utf-8")
    allowlist_match = re.search(
        r"TASK5_FINGERPRINT_INPUTS=\(\n(?P<inputs>.*?)\n\)",
        harness,
        re.DOTALL,
    )

    assert allowlist_match is not None
    fingerprint_inputs = re.findall(r'"([^"]+)"', allowlist_match.group("inputs"))
    assert {
        "build/docker-compose.yml",
        "build/scripts/assert-observer-response.py",
        "build/scripts/run-observer-live-probe.sh",
        "spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala",
        "spark-observer/src/test/scala/io/dataship/spark/observer/api/HealthResponseSpec.scala",
        "src/apps/observer_live_probe.py",
        "tests/test_observer_response_assertions.py",
    }.issubset(fingerprint_inputs)
    assert len(fingerprint_inputs) == len(set(fingerprint_inputs))
    assert all("docs/spark-observer/evidence" not in path for path in fingerprint_inputs)
    assert all("execution-log" not in path for path in fingerprint_inputs)
    assert all(path != ".env" for path in fingerprint_inputs)
    assert all("target/" not in path for path in fingerprint_inputs)
    assert all("build/var/" not in path for path in fingerprint_inputs)
    assert all(
        not re.search(r"(?:secret|token|credential|password)", path, re.IGNORECASE)
        for path in fingerprint_inputs
    )
    for label in (
        "task5_git_head=",
        "spark_master_image_id=",
        "spark_worker_image_id=",
        "task5_input_fingerprint_start=",
        "task5_input_fingerprint_end=",
        "task5_input_fingerprints_match=true",
    ):
        assert label in harness


def test_accepts_the_exact_ready_health_contract():
    helper = _load_helper()
    payload = _valid_payload()

    result = helper.validate_response(
        status_code=200,
        content_type="application/json;charset=utf-8",
        body=json.dumps(payload),
        expected_status="READY",
    )

    assert result == payload


@pytest.mark.parametrize(
    ("status_code", "content_type", "body", "expected_message"),
    (
        (503, "application/json", json.dumps(_valid_payload()), "HTTP status"),
        (200, "text/html", json.dumps(_valid_payload()), "Content-Type"),
        (200, "application/json", "not-json", "JSON document"),
        (200, "application/json", "[]", "JSON object"),
    ),
)
def test_rejects_invalid_http_or_json_documents(
    status_code,
    content_type,
    body,
    expected_message,
):
    helper = _load_helper()

    with pytest.raises(helper.ResponseValidationError, match=expected_message):
        helper.validate_response(
            status_code=status_code,
            content_type=content_type,
            body=body,
            expected_status="READY",
        )


def test_rejects_an_incorrect_lifecycle_status():
    helper = _load_helper()
    payload = _valid_payload()
    payload["status"] = "STARTING"

    with pytest.raises(helper.ResponseValidationError, match="status"):
        helper.validate_response(
            status_code=200,
            content_type="application/json",
            body=json.dumps(payload),
            expected_status="READY",
        )


@pytest.mark.parametrize(
    "field",
    tuple(_valid_payload()),
)
def test_rejects_every_missing_required_field(field):
    helper = _load_helper()
    payload = _valid_payload()
    payload.pop(field)

    with pytest.raises(helper.ResponseValidationError, match="required fields"):
        helper.validate_response(
            status_code=200,
            content_type="application/json",
            body=json.dumps(payload),
            expected_status="READY",
        )


def test_rejects_fields_outside_the_health_allowlist():
    helper = _load_helper()
    payload = _valid_payload()
    payload["sparkConf"] = {"spark.secret": "must-not-appear"}

    with pytest.raises(helper.ResponseValidationError, match="unexpected fields"):
        helper.validate_response(
            status_code=200,
            content_type="application/json",
            body=json.dumps(payload),
            expected_status="READY",
        )


def test_rejects_an_invalid_captured_at_timestamp():
    helper = _load_helper()
    payload = _valid_payload()
    payload["capturedAt"] = "2026-07-16 12:34:56"

    with pytest.raises(helper.ResponseValidationError, match="capturedAt"):
        helper.validate_response(
            status_code=200,
            content_type="application/json",
            body=json.dumps(payload),
            expected_status="READY",
        )
