# Task 4 — deterministic live workload and harness

Status: `READY — awaiting user acceptance`.

No commit, stage, push or Task 5 implementation was performed.

## Delivered behavior

- Pure-Python defaults are exactly `rows=40`, `partitions=4`,
  `delay-ms=75` and `hold-seconds=10`.
- Negative CLI values are rejected; zero partitions are also rejected because
  Spark requires at least one partition.
- PySpark is imported only inside `main`, so the focused host tests do not
  require host PySpark.
- The workload performs an explicit delayed RDD `sum` action and an explicit
  SQL `collect` action. Native Spark evidence shows job `0` for `sum`, jobs
  `1`–`4` for the SQL action, shuffle stages and multiple result/map stages.
- The asserted deterministic result is:

```json
{"bucketTotals":[[0,180],[1,190],[2,200],[3,210]],"rowCount":40,"valueSum":780}
```

- `spark-master` owns the persistent localhost-only mapping
  `127.0.0.1:24040:4040`.
- `observer-live` invokes the versioned harness.
- Each harness run creates a unique `OBSERVER_RUN_ID`, includes it in the
  Spark application name, Spark configuration and probe arguments, and
  classifies the exact in-container `timeout` wrapper and Java `SparkSubmit`
  from a self-safe `ps` snapshot by that identifier.
- The harness forces `spark.ui.port=4040` and `spark.port.maxRetries=0`, uses
  explicit deadlines and an in-container `timeout`, installs `EXIT`, `TERM`
  and `INT` cleanup traps, and keeps temporary files under `/tmp`.
- Plugin-disabled and plugin-enabled runs both finish with result and exit
  code `0`; only the enabled transcript contains the plugin initialization.
- A deliberate timeout returns `124` from the harness and still proves PID
  absence, HTTP unavailability and persistent Docker mapping.
- A pre-discovery TERM returns `143` and removes the run-ID wrapper process
  group even when `DRIVER_PID` was never set.

## TDD evidence

| Phase | Command/evidence | Result |
| --- | --- | --- |
| Initial RED | `uv run pytest tests/test_observer_live_probe.py -q` | `9` failures caused by the absent module, absent mapping and absent harness/target |
| Compose RED | filtered `docker compose ... config` lookup | exit `1`; mapping absent |
| Redirect RED | focused contract requiring redirect following | `8 passed, 1 failed` |
| Native route RED | focused contract requiring honest native-route absence evidence | `8 passed, 1 failed` |
| Final GREEN | `uv run pytest tests/test_observer_live_probe.py -q` | `9 passed` |

Raw files:

- `task-04-python-red.txt`
- `task-04-compose-red.txt`
- `task-04-root-redirect-red.txt`
- `task-04-native-route-red.txt`
- `task-04-python-green.txt`
- `task-04-compose-green.txt`

## Live timeline

### Plugin disabled

Source: `task-04-plugin-disabled.txt`.

| Observation | Evidence |
| --- | --- |
| Run/PID | `observer-live-20260716T194351Z-272105-13791`, Java PID `3528` |
| First live read | final HTTP `200`, PID `3528` alive |
| Second live read | final HTTP `200`, PID `3528` alive |
| Workload | job `0` plus SQL jobs `1`–`4`, shuffle and multiple stages |
| Result | row count `40`, value sum `780`, four deterministic buckets |
| Submit | exit `0` |
| Cleanup | PID absent; driver UI unavailable; mapping remains `127.0.0.1:24040` |

### Plugin enabled

Source: `task-04-plugin-enabled.txt`.

| Observation | Evidence |
| --- | --- |
| Run/PID | `observer-live-20260716T194435Z-273727-1865`, Java PID `3903` |
| Plugin | `DriverPluginContainer` initialized `SparkDataShipPlugin` |
| First/second reads | final HTTP `200` twice with PID `3903` alive |
| Workload/result | same deterministic jobs, shuffle, SQL and result as disabled mode |
| Submit/cleanup | exit `0`; PID absent; UI unavailable; mapping persists |

`task-04-mode-comparison.txt` proves plugin initialization is absent in the
disabled transcript and present in the enabled transcript.

### Deliberate failure and mapping reuse

- `task-04-direct-timeout-status.txt` is a direct versioned harness command.
  It returns `124`, prints `spark_submit_exit_code=124`, then proves:
  `driver_process_absent_after_run=true`,
  `driver_ui_unavailable_after_run=true` and
  `spark_master_4040_mapping_persists=127.0.0.1:24040`.
- GNU Make normalizes a failed recipe to its own exit `2`; the separate
  `task-04-deliberate-timeout.txt` therefore records Make's expected failure
  while the harness output inside it remains `124`.
- `task-04-mapping-reuse.txt` runs successfully after that failure without
  recreating `spark-master`: submit exit `0`, cleanup passes and the same
  mapping remains available for the next driver.

## Native `/dataship/` discrepancy

The implementation plan expected a native `404`. Spark `4.1.2` does not
return `404` for unknown UI paths after the full driver UI is attached.

`task-04-native-route-diagnostic.txt`, captured while Java driver PID `3137`
was alive, proves that all three paths behave identically:

- `/dataship/`
- `/dataship/api/v1/health`
- `/observer-random-unknown/`

Each returns `302 Location: http://127.0.0.1:24040/jobs/`, then final HTTP
`200` at the native `/jobs/` page. The harness therefore makes the smallest
honest absence assertion: both DataShip paths must have exactly the same
initial status, Location, final status and final URL as a unique random
unknown path. No DataShip endpoint, listener, servlet or UI was added.

`task-04-root-redirect-diagnostic.txt` separately proves why root reads must
follow redirects: native `/` returns `302` after UI attachment and final
`200` at `/jobs/`.

## Independent-review lifecycle fixes

The focused post-review RED is
`task-04-review-fixes-red.txt` (command exit `1`). Its five behavioral
failures demonstrated:

- a submit that exited `0` before driver discovery incorrectly made the
  harness exit `0`;
- `00`, `0.00` and `000.000` were accepted as positive timeout values;
- TERM before `DRIVER_PID` discovery left the run-ID timeout wrapper alive.

The minimal fix:

- maps missing-driver submit `0` to harness failure `1` while preserving any
  nonzero submit status;
- treats a numeric timeout as positive only when the validated spelling
  contains at least one nonzero digit;
- obtains `ps pid/ppid/pgid/comm/args` without putting the run ID in the
  inspection process;
- selects only exact `comm=timeout` wrappers and exact `comm=java` processes
  whose arguments contain both the unique run ID and
  `org.apache.spark.deploy.SparkSubmit`;
- signals the wrapper process group, falls back to direct Java signaling when
  no wrapper exists, and verifies both roles are absent after TERM/KILL.

`task-04-review-fixes-green.txt` records the focused suite and `bash -n`
passing with command exit `0` (`17` focused tests).

The real early-interruption command is versioned as
`task-04-early-term-command.sh`; its authoritative transcript is
`task-04-early-term-cleanup.txt`. Run
`observer-live-20260716T202125Z-313416-24704` was frozen during discovery
before `spark_driver_pid=` was recorded. The container showed wrapper `6620`
and Java sharing PGID `6620`; TERM cleanup returned `143`, removed both,
left no run-ID process record, kept the UI unavailable and preserved
`127.0.0.1:24040`.

Steady-state rechecks:

- `task-04-review-fixes-plugin-enabled.txt`: plugin-enabled submit/harness
  exit `0`, wrapper and driver absent after the run;
- `task-04-review-fixes-direct-timeout.txt`: exact submit/harness exit `124`,
  wrapper and driver absent after the run;
- `task-04-review-fixes-regressions.txt`: Scala `9/9`, Node `1/1`, Python
  `41/41`, JAR exclusion valid, platform validation PASS, verifier exit `0`.

The existing screenshots were not regenerated because the visual surface,
workload and screenshot correlation did not change.

## Controller verification and final re-review

The controller independently repeated the early-TERM scenario in
`task-04-controller-early-term.txt`. The harness was stopped before
`spark_driver_pid=` existed; wrapper and Java shared PGID `7409`; TERM
returned `143`; both processes were absent afterward; the driver UI was
unavailable and the persistent mapping remained `127.0.0.1:24040`.

The controller also ran the final full verifier in
`task-04-controller-final-regressions.txt`:

- Scala: `9/9`
- Node: `1/1`
- Python: `41/41`
- Observer JAR: no bundled Spark/Scala runtime classes
- platform validation: `Validation passed`
- verifier and transcript exit: `0`

The final independent re-review reported zero Critical, Important or Minor
findings. It accepted the native unknown-route equivalence as an honest
Spark 4.1.2-specific proof that no DataShip route is installed.

## Visual evidence

`task-04-playwright-capture.txt` records Playwright `1.58.2` captures and
proves Java PID `5390` remained alive after all screenshots. It also records
driver Jobs HTTP `200`, Master HTTP `200` and the three image SHA-256 values.

- `spark-driver-jobs-live.png`: native driver UI on `24040`, five completed
  jobs and their stage/task counts.
- `spark-driver-sql-live.png`: native SQL/DataFrame UI on `24040`, the SQL
  execution linked to jobs `1`–`4`.
- `spark-master-live.png`: distinct Master UI on `28081`, one ALIVE worker
  and the same uniquely named application in `RUNNING`.

The correlated run is in `task-04-screenshot-correlation-run.txt`; it ends
with submit exit `0`, PID absence, endpoint unavailability and mapping
persistence.

## Regression gate

Direct capture:

```bash
script -qefc 'make observer-verify' \
  docs/spark-observer/evidence/task-04/task-04-regressions.txt
```

Results:

- Scala: `9/9`
- Node: `1/1`
- Python: historical capture `33/33`; post-review capture `41/41`
- Observer JAR: no bundled Spark/Scala classes
- platform validation: `Validation passed`
- verifier exit: `0`

## Scope and remaining concern

- No protected durable-path file changed.
- No Spark Observer JVM source changed.
- No endpoint, listener, custom tab or Task 5 behavior was added.
- The only concern is the documented plan/runtime discrepancy: native Spark
  unknown routes redirect to `/jobs/` instead of returning literal `404`.
  Absence is proven by equivalence with a unique random unknown path.
- State remains `READY — awaiting user acceptance`.
