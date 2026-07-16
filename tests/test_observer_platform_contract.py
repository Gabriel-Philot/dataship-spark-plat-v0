from pathlib import Path
import re
import shlex

import yaml


ROOT_DIR = Path(__file__).resolve().parents[1]


def _read_env_example() -> dict[str, str]:
    entries = {}
    for line in (ROOT_DIR / ".env.example").read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            entries[key] = value
    return entries


def _read_spark_defaults(path: Path | None = None) -> dict[str, str]:
    entries = {}
    defaults_path = path or ROOT_DIR / "build/config/spark/spark-defaults.conf"
    for line in defaults_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split(maxsplit=1)
        entries[key] = value
    return entries


def _assert_single_system_property(
    options: str, property_name: str, expected_value: str
) -> None:
    property_prefix = f"-D{property_name}="
    expected_token = f"{property_prefix}{expected_value}"
    matching_tokens = [
        token for token in shlex.split(options) if token.startswith(property_prefix)
    ]

    assert matching_tokens == [expected_token]


def test_observer_preserves_native_spark_event_log_contract():
    env = _read_env_example()
    spark_defaults = _read_spark_defaults()
    compose = yaml.safe_load(
        (ROOT_DIR / "build/docker-compose.yml").read_text(encoding="utf-8")
    )
    loader_env = compose["services"]["eventlog-loader"]["environment"]
    history_opts = compose["services"]["spark-history"]["environment"][
        "SPARK_HISTORY_OPTS"
    ]

    assert env["MINIO_LOG_BUCKET"] == "spark-logs"
    assert loader_env["MINIO_BUCKET"] == "${MINIO_LOG_BUCKET:-spark-logs}"
    assert loader_env["MINIO_PREFIX"] == "events/"
    assert spark_defaults["spark.eventLog.enabled"] == "true"
    assert spark_defaults["spark.eventLog.dir"] == "s3a://spark-logs/events"
    _assert_single_system_property(
        history_opts,
        "spark.history.fs.logDirectory",
        "s3a://${MINIO_LOG_BUCKET:-spark-logs}/events",
    )
    assert (
        spark_defaults["spark.history.fs.logDirectory"]
        == "s3a://spark-logs/events"
    )
    assert (
        spark_defaults["spark.history.provider"]
        == "org.apache.spark.deploy.history.FsHistoryProvider"
    )
    assert spark_defaults["spark.eventLog.logStageExecutorMetrics"] == "true"


def test_spark_defaults_parser_ignores_blank_and_comment_lines(tmp_path):
    defaults_path = tmp_path / "spark-defaults.conf"
    defaults_path.write_text(
        """
# Native event-log settings
spark.eventLog.enabled true

spark.eventLog.dir s3a://spark-logs/events
""",
        encoding="utf-8",
    )

    assert _read_spark_defaults(defaults_path) == {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "s3a://spark-logs/events",
    }


def test_history_option_contract_rejects_path_drift_and_duplicates():
    property_name = "spark.history.fs.logDirectory"
    expected = "s3a://${MINIO_LOG_BUCKET:-spark-logs}/events"

    _assert_single_system_property(
        f"-Dspark.history.ui.port=18080 -D{property_name}={expected}",
        property_name,
        expected,
    )

    invalid_options = (
        f"-D{property_name}=s3a://spark-logs/drifted",
        f"-D{property_name}={expected} -D{property_name}={expected}",
    )
    for options in invalid_options:
        try:
            _assert_single_system_property(options, property_name, expected)
        except AssertionError:
            continue
        raise AssertionError(f"invalid History options were accepted: {options}")


def test_scala_dependencies_align_with_spark_runtime():
    build_definition = (ROOT_DIR / "spark-observer/build.sbt").read_text(
        encoding="utf-8"
    )

    assert re.search(
        r'scalaVersion\s*:=\s*"2\.13\.17"',
        build_definition,
    )
    assert re.search(r"autoScalaLibrary\s*:=\s*false", build_definition)
    assert re.search(
        r'"org\.scala-lang"\s*%\s*"scala-library"\s*%\s*"2\.13\.17"'
        r"\s*%\s*Provided",
        build_definition,
    )
    for artifact in ("scala-library", "scala-reflect"):
        assert re.search(
            rf'dependencyOverrides\s*\+=\s*"org\.scala-lang"\s*%'
            rf'\s*"{artifact}"\s*%\s*"2\.13\.17"',
            build_definition,
        )


def test_observer_cache_marker_is_input_bound_and_strictly_validated():
    bootstrap = (ROOT_DIR / "build/scripts/bootstrap.sh").read_text(
        encoding="utf-8"
    )
    validator = (
        ROOT_DIR / "build/scripts/validate-bootstrap.sh"
    ).read_text(encoding="utf-8")

    marker_assignment = (
        'OBSERVER_CACHE_MARKER="$SBT_CACHE_DIR/.observer-bootstrap.sha256"'
    )
    for script in (bootstrap, validator):
        assert marker_assignment in script
        assert "observer_cache_fingerprint()" in script
        assert '"SBT_IMAGE=$SBT_IMAGE"' in script
        assert "spark-observer/build.sbt" in script
        assert "spark-observer/project/build.properties" in script

    assert 'rm -f "$OBSERVER_CACHE_MARKER"' in bootstrap
    assert bootstrap.index("sbt update") < bootstrap.index(
        '> "$OBSERVER_CACHE_MARKER"'
    )
    assert '[[ ! -f "$OBSERVER_CACHE_MARKER" ]]' in validator
    assert (
        '"$(cat "$OBSERVER_CACHE_MARKER")" '
        '!= "$OBSERVER_CACHE_FINGERPRINT"'
    ) in validator
    assert 'validate_cache_subtree "$SBT_CACHE_DIR/boot"' in validator
    assert 'validate_cache_subtree "$COURSIER_CACHE_DIR/https"' in validator
