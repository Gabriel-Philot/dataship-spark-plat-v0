#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
import json
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


class ResponseValidationError(ValueError):
    pass


def validate_response(
    *,
    status_code: int,
    content_type: str,
    body: str,
    expected_status: str,
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
        raise ResponseValidationError("Health response must be a JSON object.")

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
    if document["listenerInstalled"]:
        raise ResponseValidationError(
            "Health field listenerInstalled must remain false in Task 5."
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


def _validate_utc_timestamp(value: Any) -> None:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ResponseValidationError(
            "Health field capturedAt must be an ISO-8601 UTC timestamp."
        )
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error_value:
        raise ResponseValidationError(
            "Health field capturedAt must be an ISO-8601 UTC timestamp."
        ) from error_value
    if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
        raise ResponseValidationError(
            "Health field capturedAt must be an ISO-8601 UTC timestamp."
        )


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
        "--expected-status",
        required=True,
        choices=("READY", "DISABLED"),
    )
    parser.add_argument("--label", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=2.0)
    args = parser.parse_args(argv)
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        status_code, content_type, body = fetch_response(
            args.url, args.timeout_seconds
        )
        document = validate_response(
            status_code=status_code,
            content_type=content_type,
            body=body,
            expected_status=args.expected_status,
        )
    except ResponseValidationError as error_value:
        print(f"Health response validation failed: {error_value}", file=sys.stderr)
        return 1

    print(
        f"{args.label}_http_status={status_code} "
        f"content_type={content_type.split(';', maxsplit=1)[0].strip().lower()}"
    )
    print(
        f"{args.label}_json="
        + json.dumps(document, separators=(",", ":"), sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
