#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
from typing import Any
from urllib import error, request


HEALTH_FIELDS = frozenset(
    {
        "schemaVersion",
        "pluginVersion",
        "sparkVersion",
        "appId",
        "mode",
        "capturedAt",
        "status",
        "uiAttached",
        "listenerInstalled",
        "supportedRuntime",
        "queueCapacity",
        "lastErrorCode",
    }
)

COUNTER_CATEGORIES = frozenset(
    {"application", "job", "stage", "task", "sql", "executor", "other"}
)
COUNTER_FIELDS = frozenset(
    {
        "schemaVersion",
        "pluginVersion",
        "sparkVersion",
        "appId",
        "mode",
        "capturedAt",
        "receivedByCategory",
        "listenerReceived",
        "processed",
        "queued",
        "inFlight",
        "droppedByPlugin",
        "internalFailures",
        "depth",
        "capacity",
        "lastEventAt",
        "invariantHolds",
    }
)
SNAPSHOT_FIELDS = frozenset(
    {
        "schemaVersion",
        "pluginVersion",
        "sparkVersion",
        "appId",
        "mode",
        "capturedAt",
        "limit",
        "truncated",
        "application",
        "jobs",
        "stages",
    }
)
APPLICATION_FIELDS = frozenset(
    {"appId", "name", "status", "startedAt", "completedAt"}
)
JOB_FIELDS = frozenset(
    {"jobId", "status", "startedAt", "completedAt", "stageIds"}
)
STAGE_FIELDS = frozenset(
    {
        "stageId",
        "attemptId",
        "status",
        "startedAt",
        "completedAt",
        "tasks",
    }
)
TASK_AGGREGATE_FIELDS = frozenset(
    {"total", "active", "completed", "failed", "killed", "completedIndices"}
)


class ResponseValidationError(ValueError):
    pass


def validate_response(
    *,
    status_code: int,
    content_type: str,
    body: str,
    expected_status: str,
    expected_listener_installed: bool = False,
) -> dict[str, Any]:
    document = _parse_json_response(
        status_code=status_code,
        content_type=content_type,
        body=body,
        response_name="Health",
    )

    actual_fields = set(document)
    missing_fields = sorted(HEALTH_FIELDS - actual_fields)
    unexpected_fields = sorted(actual_fields - HEALTH_FIELDS)
    if missing_fields:
        raise ResponseValidationError(
            f"Health response is missing required fields: {', '.join(missing_fields)}."
        )
    if unexpected_fields:
        raise ResponseValidationError(
            f"Health response contains unexpected fields: {', '.join(unexpected_fields)}."
        )

    expected_constants = {
        "schemaVersion": "v1",
        "pluginVersion": "0.1.0-SNAPSHOT",
        "sparkVersion": "4.1.2",
        "mode": "live",
        "status": expected_status,
    }
    for field, expected_value in expected_constants.items():
        if document[field] != expected_value:
            raise ResponseValidationError(
                f"Health field {field} must be {expected_value!r}, "
                f"received {document[field]!r}."
            )

    if not isinstance(document["appId"], str) or not document["appId"].strip():
        raise ResponseValidationError("Health field appId must be a non-empty string.")
    _validate_utc_timestamp(document["capturedAt"])

    for field in ("uiAttached", "listenerInstalled", "supportedRuntime"):
        if not isinstance(document[field], bool):
            raise ResponseValidationError(f"Health field {field} must be boolean.")
    if document["listenerInstalled"] != expected_listener_installed:
        raise ResponseValidationError(
            "Health field listenerInstalled must be "
            f"{expected_listener_installed!r}."
        )
    if not document["uiAttached"]:
        raise ResponseValidationError(
            "Health field uiAttached must be true for an HTTP health response."
        )
    if not document["supportedRuntime"]:
        raise ResponseValidationError(
            "Health field supportedRuntime must be true for Spark 4.1.2."
        )

    queue_capacity = document["queueCapacity"]
    if (
        isinstance(queue_capacity, bool)
        or not isinstance(queue_capacity, int)
        or not 1 <= queue_capacity <= 65536
    ):
        raise ResponseValidationError(
            "Health field queueCapacity must be an integer from 1 to 65536."
        )
    if not isinstance(document["lastErrorCode"], str):
        raise ResponseValidationError("Health field lastErrorCode must be a string.")
    if expected_status in {"READY", "DISABLED"} and document["lastErrorCode"]:
        raise ResponseValidationError(
            f"Health field lastErrorCode must be empty for {expected_status}."
        )
    return document


def validate_counters_response(
    *,
    status_code: int,
    content_type: str,
    body: str,
    listener_received_greater_than: int | None,
    minimum_dropped_by_plugin: int,
) -> dict[str, Any]:
    document = _parse_json_response(
        status_code=status_code,
        content_type=content_type,
        body=body,
        response_name="Counters",
    )
    actual_fields = set(document)
    missing_fields = sorted(COUNTER_FIELDS - actual_fields)
    unexpected_fields = sorted(actual_fields - COUNTER_FIELDS)
    if missing_fields:
        raise ResponseValidationError(
            "Counters response is missing required fields: "
            + ", ".join(missing_fields)
            + "."
        )
    if unexpected_fields:
        raise ResponseValidationError(
            "Counters response contains unexpected fields: "
            + ", ".join(unexpected_fields)
            + "."
        )

    for field, expected_value in {
        "schemaVersion": "v1",
        "pluginVersion": "0.1.0-SNAPSHOT",
        "sparkVersion": "4.1.2",
        "mode": "live",
    }.items():
        if document[field] != expected_value:
            raise ResponseValidationError(
                f"Counters field {field} must be {expected_value!r}, "
                f"received {document[field]!r}."
            )
    if not isinstance(document["appId"], str) or not document["appId"].strip():
        raise ResponseValidationError("Counters field appId must be non-empty.")
    _validate_utc_timestamp(document["capturedAt"])
    _validate_utc_timestamp(document["lastEventAt"], field_name="lastEventAt")

    categories = document["receivedByCategory"]
    if not isinstance(categories, dict) or set(categories) != COUNTER_CATEGORIES:
        raise ResponseValidationError(
            "Counters receivedByCategory must contain the exact fixed categories."
        )
    for category, value in categories.items():
        _validate_non_negative_integer(value, f"receivedByCategory.{category}")

    numeric_fields = (
        "listenerReceived",
        "processed",
        "queued",
        "inFlight",
        "droppedByPlugin",
        "internalFailures",
        "depth",
    )
    for field in numeric_fields:
        _validate_non_negative_integer(document[field], field)
    capacity = document["capacity"]
    _validate_non_negative_integer(capacity, "capacity")
    if not 1 <= capacity <= 65536:
        raise ResponseValidationError(
            "Counters field capacity must be an integer from 1 to 65536."
        )

    listener_received = document["listenerReceived"]
    if sum(categories.values()) != listener_received:
        raise ResponseValidationError(
            "Counters category total must equal listenerReceived."
        )
    accounted = sum(
        document[field]
        for field in ("processed", "queued", "inFlight", "droppedByPlugin")
    )
    if listener_received != accounted:
        raise ResponseValidationError(
            "Counters accounting invariant does not close."
        )
    if document["depth"] != document["queued"]:
        raise ResponseValidationError("Counters depth must equal queued.")
    if document["invariantHolds"] is not True:
        raise ResponseValidationError("Counters invariantHolds must be true.")
    if (
        listener_received_greater_than is not None
        and listener_received <= listener_received_greater_than
    ):
        raise ResponseValidationError(
            "Counters listenerReceived must grow beyond "
            f"{listener_received_greater_than}, received {listener_received}."
        )
    if document["droppedByPlugin"] < minimum_dropped_by_plugin:
        raise ResponseValidationError(
            "Counters droppedByPlugin must be at least "
            f"{minimum_dropped_by_plugin}, received "
            f"{document['droppedByPlugin']}."
        )
    return document


def validate_snapshot_response(
    *,
    status_code: int,
    content_type: str,
    body: str,
    expected_limit: int,
    require_truncated: bool | None,
    require_running: bool,
) -> dict[str, Any]:
    document = _parse_json_response(
        status_code=status_code,
        content_type=content_type,
        body=body,
        response_name="Snapshot",
    )
    _validate_exact_fields(document, SNAPSHOT_FIELDS, "Snapshot")
    for field, expected_value in {
        "schemaVersion": "v1",
        "pluginVersion": "0.1.0-SNAPSHOT",
        "sparkVersion": "4.1.2",
        "mode": "live",
        "limit": expected_limit,
    }.items():
        if document[field] != expected_value:
            raise ResponseValidationError(
                f"Snapshot field {field} must be {expected_value!r}, "
                f"received {document[field]!r}."
            )
    if not isinstance(document["appId"], str) or not document["appId"].strip():
        raise ResponseValidationError("Snapshot field appId must be non-empty.")
    _validate_utc_timestamp(document["capturedAt"])
    if (
        isinstance(expected_limit, bool)
        or not isinstance(expected_limit, int)
        or not 1 <= expected_limit <= 200
    ):
        raise ResponseValidationError("Snapshot expected limit must be from 1 to 200.")
    if not isinstance(document["truncated"], bool):
        raise ResponseValidationError("Snapshot field truncated must be boolean.")
    if (
        require_truncated is not None
        and document["truncated"] is not require_truncated
    ):
        raise ResponseValidationError(
            f"Snapshot field truncated must be {require_truncated!r}."
        )

    application = document["application"]
    if not isinstance(application, dict):
        raise ResponseValidationError("Snapshot application must be an object.")
    _validate_exact_fields(application, APPLICATION_FIELDS, "Application")
    if application["appId"] != document["appId"]:
        raise ResponseValidationError(
            "Snapshot application appId must match the envelope appId."
        )
    if not isinstance(application["name"], str):
        raise ResponseValidationError("Application name must be a string.")
    if application["status"] not in {"RUNNING", "COMPLETED"}:
        raise ResponseValidationError("Application status is not allowlisted.")
    _validate_utc_timestamp(application["startedAt"], field_name="startedAt")
    _validate_optional_utc_timestamp(
        application["completedAt"], field_name="completedAt"
    )
    if application["status"] == "RUNNING" and application["completedAt"] is not None:
        raise ResponseValidationError(
            "A running application must not expose a completion timestamp."
        )

    jobs = document["jobs"]
    stages = document["stages"]
    if not isinstance(jobs, list) or not isinstance(stages, list):
        raise ResponseValidationError("Snapshot jobs and stages must be arrays.")
    if len(jobs) > expected_limit or len(stages) > expected_limit:
        raise ResponseValidationError(
            "Snapshot collections must not exceed the requested limit."
        )
    for job in jobs:
        _validate_job(job)
    for stage in stages:
        _validate_stage(stage)
    job_ids = [job["jobId"] for job in jobs]
    stage_ids = [(stage["stageId"], stage["attemptId"]) for stage in stages]
    if job_ids != sorted(job_ids, reverse=True):
        raise ResponseValidationError("Snapshot jobs must be newest first.")
    if stage_ids != sorted(stage_ids, reverse=True):
        raise ResponseValidationError("Snapshot stages must be newest first.")
    if require_running and not any(job["status"] == "RUNNING" for job in jobs):
        raise ResponseValidationError("Snapshot must include a running job.")
    if require_running and not any(stage["status"] == "RUNNING" for stage in stages):
        raise ResponseValidationError("Snapshot must include a running stage.")
    return document


def validate_snapshot_transition(
    previous: dict[str, Any],
    current: dict[str, Any],
) -> dict[str, int]:
    if previous.get("appId") != current.get("appId"):
        raise ResponseValidationError(
            "Snapshot transition must come from the same application."
        )
    current_jobs = {job["jobId"]: job for job in current.get("jobs", [])}
    current_stages = {
        (stage["stageId"], stage["attemptId"]): stage
        for stage in current.get("stages", [])
    }
    transitioned_jobs = [
        job
        for job in previous.get("jobs", [])
        if job.get("status") == "RUNNING"
        and current_jobs.get(job.get("jobId"), {}).get("status") == "SUCCEEDED"
    ]
    transitioned_stages = [
        stage
        for stage in previous.get("stages", [])
        if stage.get("status") == "RUNNING"
        and current_stages.get(
            (stage.get("stageId"), stage.get("attemptId")), {}
        ).get("status")
        == "SUCCEEDED"
    ]
    for job in transitioned_jobs:
        stage_ids = set(job.get("stageIds", []))
        matching_stage = next(
            (stage for stage in transitioned_stages if stage.get("stageId") in stage_ids),
            None,
        )
        if matching_stage is not None:
            return {
                "jobId": job["jobId"],
                "stageId": matching_stage["stageId"],
                "stageAttemptId": matching_stage["attemptId"],
            }
    raise ResponseValidationError(
        "Snapshot transition must show the same jobId and stage attempt "
        "moving from RUNNING to SUCCEEDED."
    )


def _validate_job(job: Any) -> None:
    if not isinstance(job, dict):
        raise ResponseValidationError("Snapshot job must be an object.")
    _validate_exact_fields(job, JOB_FIELDS, "Job")
    _validate_non_negative_integer(job["jobId"], "jobId", response_name="Snapshot")
    if job["status"] not in {"RUNNING", "SUCCEEDED", "FAILED", "UNKNOWN"}:
        raise ResponseValidationError("Job status is not allowlisted.")
    _validate_optional_utc_timestamp(job["startedAt"], field_name="startedAt")
    _validate_optional_utc_timestamp(job["completedAt"], field_name="completedAt")
    if not isinstance(job["stageIds"], list):
        raise ResponseValidationError("Job stageIds must be an array.")
    for stage_id in job["stageIds"]:
        _validate_non_negative_integer(
            stage_id, "stageIds", response_name="Snapshot"
        )


def _validate_stage(stage: Any) -> None:
    if not isinstance(stage, dict):
        raise ResponseValidationError("Snapshot stage must be an object.")
    _validate_exact_fields(stage, STAGE_FIELDS, "Stage")
    _validate_non_negative_integer(
        stage["stageId"], "stageId", response_name="Snapshot"
    )
    _validate_non_negative_integer(
        stage["attemptId"], "attemptId", response_name="Snapshot"
    )
    if stage["status"] not in {
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
        "PENDING",
        "SKIPPED",
    }:
        raise ResponseValidationError("Stage status is not allowlisted.")
    _validate_optional_utc_timestamp(stage["startedAt"], field_name="startedAt")
    _validate_optional_utc_timestamp(stage["completedAt"], field_name="completedAt")
    tasks = stage["tasks"]
    if not isinstance(tasks, dict):
        raise ResponseValidationError("Stage tasks must be an aggregate object.")
    _validate_exact_fields(tasks, TASK_AGGREGATE_FIELDS, "Task aggregate")
    for field in TASK_AGGREGATE_FIELDS:
        _validate_non_negative_integer(
            tasks[field], f"tasks.{field}", response_name="Snapshot"
        )
    if tasks["completedIndices"] > tasks["total"]:
        raise ResponseValidationError(
            "Stage tasks completedIndices must not exceed total."
        )


def _validate_exact_fields(
    document: dict[str, Any],
    expected_fields: frozenset[str],
    response_name: str,
) -> None:
    actual_fields = set(document)
    missing_fields = sorted(expected_fields - actual_fields)
    unexpected_fields = sorted(actual_fields - expected_fields)
    if missing_fields:
        raise ResponseValidationError(
            f"{response_name} response is missing required fields: "
            + ", ".join(missing_fields)
            + "."
        )
    if unexpected_fields:
        raise ResponseValidationError(
            f"{response_name} response contains unexpected fields: "
            + ", ".join(unexpected_fields)
            + "."
        )


def _parse_json_response(
    *,
    status_code: int,
    content_type: str,
    body: str,
    response_name: str,
) -> dict[str, Any]:
    if status_code != 200:
        raise ResponseValidationError(
            f"Expected HTTP status 200, received {status_code}."
        )
    media_type = content_type.split(";", maxsplit=1)[0].strip().lower()
    if media_type != "application/json":
        raise ResponseValidationError(
            f"Expected Content-Type application/json, received {content_type!r}."
        )
    try:
        document = json.loads(body)
    except json.JSONDecodeError as error_value:
        raise ResponseValidationError("Response is not a valid JSON document.") from error_value
    if not isinstance(document, dict):
        raise ResponseValidationError(f"{response_name} response must be a JSON object.")
    return document


def _validate_non_negative_integer(
    value: Any,
    field_name: str,
    response_name: str = "Counters",
) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ResponseValidationError(
            f"{response_name} field {field_name} must be a non-negative integer."
        )


def _validate_utc_timestamp(value: Any, field_name: str = "capturedAt") -> None:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ResponseValidationError(
            f"Field {field_name} must be an ISO-8601 UTC timestamp."
        )
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error_value:
        raise ResponseValidationError(
            f"Field {field_name} must be an ISO-8601 UTC timestamp."
        ) from error_value
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ResponseValidationError(
            f"Field {field_name} must be an ISO-8601 UTC timestamp."
        )


def _validate_optional_utc_timestamp(value: Any, field_name: str) -> None:
    if value is not None:
        _validate_utc_timestamp(value, field_name=field_name)


def fetch_response(url: str, timeout_seconds: float) -> tuple[int, str, str]:
    http_request = request.Request(url, method="GET")
    try:
        with request.urlopen(http_request, timeout=timeout_seconds) as response:
            return (
                response.status,
                response.headers.get("Content-Type", ""),
                response.read().decode("utf-8"),
            )
    except error.HTTPError as error_value:
        return (
            error_value.code,
            error_value.headers.get("Content-Type", ""),
            error_value.read().decode("utf-8", errors="replace"),
        )
    except (error.URLError, TimeoutError) as error_value:
        raise ResponseValidationError(
            f"Health request did not complete: {error_value}."
        ) from error_value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate one DataShip Spark Observer health response."
    )
    parser.add_argument("--url", required=True)
    parser.add_argument(
        "--response-kind",
        choices=("health", "counters", "snapshot"),
        default="health",
    )
    parser.add_argument(
        "--expected-status",
        choices=("READY", "DISABLED"),
        default="READY",
    )
    parser.add_argument(
        "--expected-listener-installed",
        choices=("true", "false"),
        default="false",
    )
    parser.add_argument("--listener-received-greater-than", type=int)
    parser.add_argument("--minimum-dropped-by-plugin", type=int, default=0)
    parser.add_argument("--expected-limit", type=int)
    parser.add_argument("--require-truncated", choices=("true", "false"))
    parser.add_argument("--require-running", action="store_true")
    parser.add_argument("--previous-snapshot", type=Path)
    parser.add_argument("--write-json", type=Path)
    parser.add_argument("--label", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=2.0)
    args = parser.parse_args(argv)
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    if (
        args.listener_received_greater_than is not None
        and args.listener_received_greater_than < 0
    ):
        parser.error("--listener-received-greater-than must be non-negative")
    if args.minimum_dropped_by_plugin < 0:
        parser.error("--minimum-dropped-by-plugin must be non-negative")
    if args.response_kind == "snapshot" and args.expected_limit is None:
        parser.error("--expected-limit is required for snapshot responses")
    if args.expected_limit is not None and not 1 <= args.expected_limit <= 200:
        parser.error("--expected-limit must be from 1 to 200")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        status_code, content_type, body = fetch_response(
            args.url, args.timeout_seconds
        )
        transition = None
        if args.response_kind == "health":
            document = validate_response(
                status_code=status_code,
                content_type=content_type,
                body=body,
                expected_status=args.expected_status,
                expected_listener_installed=(
                    args.expected_listener_installed == "true"
                ),
            )
        elif args.response_kind == "counters":
            document = validate_counters_response(
                status_code=status_code,
                content_type=content_type,
                body=body,
                listener_received_greater_than=(
                    args.listener_received_greater_than
                ),
                minimum_dropped_by_plugin=args.minimum_dropped_by_plugin,
            )
        else:
            document = validate_snapshot_response(
                status_code=status_code,
                content_type=content_type,
                body=body,
                expected_limit=args.expected_limit,
                require_truncated=(
                    None
                    if args.require_truncated is None
                    else args.require_truncated == "true"
                ),
                require_running=args.require_running,
            )
            if args.previous_snapshot is not None:
                previous = json.loads(
                    args.previous_snapshot.read_text(encoding="utf-8")
                )
                transition = validate_snapshot_transition(previous, document)
            if args.write_json is not None:
                args.write_json.write_text(
                    json.dumps(document, separators=(",", ":"), sort_keys=True),
                    encoding="utf-8",
                )
    except ResponseValidationError as error_value:
        response_label = args.response_kind.capitalize()
        print(
            f"{response_label} response validation failed: {error_value}",
            file=sys.stderr,
        )
        return 1

    print(
        f"{args.label}_http_status={status_code} "
        f"content_type={content_type.split(';', maxsplit=1)[0].strip().lower()}"
    )
    if args.response_kind == "counters":
        print(
            f"{args.label}_listener_received={document['listenerReceived']} "
            f"dropped_by_plugin={document['droppedByPlugin']}"
        )
    if args.response_kind == "snapshot":
        print(
            f"{args.label}_jobs={len(document['jobs'])} "
            f"stages={len(document['stages'])} "
            f"limit={document['limit']} truncated={str(document['truncated']).lower()}"
        )
        if transition is not None:
            print(
                f"{args.label}_transition_job_id={transition['jobId']} "
                f"stage_id={transition['stageId']} "
                f"stage_attempt_id={transition['stageAttemptId']}"
            )
    print(
        f"{args.label}_json="
        + json.dumps(document, separators=(",", ":"), sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
