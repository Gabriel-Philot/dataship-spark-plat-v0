# Task 5 evidence report

## Result

Task 5 is `READY WITH REQUIRED WAIVER — review fixes verified`. User acceptance
must explicitly waive the missing historical Task-5-specific live RED capture.
The Spark 4.1.2 driver plugin exposes `/dataship/api/v1/health` from the
existing Spark UI, reports a strict allowlisted lifecycle document, and leaves
listener, snapshot, and tab work unimplemented for Task 6.

No commit was created.

## Files

Task-owned implementation and test changes:

- `docs/spark-observer/2026-07-15-dataship-spark-observer-implementation-plan.md`
- `docs/spark-observer/execution-log.md`
- `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipDriverPlugin.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/api/HealthResponse.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/api/HealthServlet.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/api/JsonRenderer.scala`
- `spark-observer/src/main/scala/org/apache/spark/dataship/v412/Spark412Bridge.scala`
- `spark-observer/src/test/scala/io/dataship/spark/observer/ObserverConfigSpec.scala`
- `spark-observer/src/test/scala/io/dataship/spark/observer/api/HealthResponseSpec.scala`
- `build/scripts/assert-observer-response.py`
- `build/scripts/run-observer-live-probe.sh`
- `tests/test_observer_response_assertions.py`
- `docs/spark-observer/evidence/task-05/`

## Red phase

- `task-05-scala-red.txt`: `make observer-tests` exited `2` with 25 expected
  compilation errors because queue capacity, lifecycle state, the health DTO,
  installer seam, and current-health access did not exist.
- `task-05-python-red.txt`: the focused Python command exited `1`; the 17
  existing live-harness tests passed and all 21 new contract tests failed
  because the validator and health reads did not exist.
- The pre-change native route behavior was already captured from the same
  starting implementation in
  `../task-04/task-04-plugin-enabled.txt`: `/dataship/api/v1/health` matched a
  random unknown route with Spark's native `302` redirect to `/jobs/`. This is
  the honest pre-implementation endpoint-absence observation.

## Green phase

- `task-05-final-scala.txt`: `make observer-tests`, exit `0`, 18/18 Scala
  tests passed.
- `task-05-final-focused-python.txt`: the focused live-harness and response
  contract modules, exit `0`, 38/38 tests passed.
- Configuration defaults to queue capacity 1024 and rejects values outside
  1 through 65536.
- `init` only validates configuration and retains the `SparkContext` and
  runtime. `registerMetrics` performs the single endpoint installation after
  receiving the application ID. `shutdown` is idempotent and reports
  `STOPPING` before closing the endpoint resource once.
- The JSON renderer gives Spark-provided Jackson only a task-owned
  `LinkedHashMap` containing the 12 allowlisted primitive fields.
- `listenerInstalled` remains `false` in every state.

## Runtime refresh and artifact identity

- `task-05-runtime-refresh.txt`: `make observer-runtime-refresh` completed
  successfully and the refreshed master reported one `ALIVE` worker.
- `task-05-jar-checksum.txt`: host artifact, staged image-context artifact,
  and live `spark-master` artifact all had SHA-256
  `019ae046ca12277e0f8682b839521f11be4ce5f39cda34250391d6ec9a433b4a`.

## Live acceptance

The first READY run is `task-05-live-ready-first-run.txt`:

- run ID `observer-live-20260716T235508Z-412359-30528`;
- internal Java driver PID `136` was alive for both health reads;
- both responses were HTTP `200`, `application/json`, with the same non-empty
  application ID `app-20260716225513-0000` and `status=READY`;
- both documents contained exactly the stable 12-field contract, including
  `schemaVersion=v1`, `pluginVersion=0.1.0-SNAPSHOT`,
  `sparkVersion=4.1.2`, `mode=live`, `uiAttached=true`,
  `listenerInstalled=false`, and `supportedRuntime=true`;
- after shutdown the submit exited `0`, driver and wrapper were absent, the
  endpoint was unavailable, and the mapping remained
  `127.0.0.1:24040`.

The real first health document was:

```json
{"appId":"app-20260716225513-0000","capturedAt":"2026-07-16T22:55:16.856800548Z","lastErrorCode":"","listenerInstalled":false,"mode":"live","pluginVersion":"0.1.0-SNAPSHOT","queueCapacity":1024,"schemaVersion":"v1","sparkVersion":"4.1.2","status":"READY","supportedRuntime":true,"uiAttached":true}
```

The second run is `task-05-live-ready-mapping-reuse.txt`:

- run ID `observer-live-20260716T235551Z-414255-28680`;
- internal Java driver PID `517` and application ID
  `app-20260716225556-0001`;
- two further READY responses, submit and harness exit `0`, complete process
  cleanup, endpoint removal, and reuse of the same persistent mapping.

Mode isolation is covered separately:

- `task-05-live-disabled.txt`: plugin present with observer disabled returned
  two `DISABLED` health documents while installing no listener; the workload
  and cleanup both succeeded.
- `task-05-live-no-spark-ui.txt`: `spark.ui.enabled=false` installed no
  handler, logged stable code `NO_SPARK_UI`, completed the workload with exit
  `0`, removed both processes, and retained the mapping.
- All UI-enabled runs retained Task 4's native `/dataship/` and random
  unknown-route equivalence while only the exact health path returned JSON.

## Visual acceptance

`spark-observer-health-ready.png` is a Playwright Chromium capture of the
live health endpoint. It visibly contains `status=READY`, application ID
`app-20260716225916-0004`, Spark version `4.1.2`, plugin version
`0.1.0-SNAPSHOT`, and only the allowlisted fields.

`task-05-playwright-capture.txt` records the capture and screenshot SHA-256
`bb777ab4b69f89009cb1211a54c8a7f2a476cde157b95675f8438d94f836903c`
while internal Java driver PID `1596` was alive.
`task-05-playwright-live-run.txt` correlates that PID, app ID, two READY
responses, successful workload, exit `0`, process cleanup, endpoint removal,
and persistent mapping reuse.

## Regression and guards

- `task-05-final-python-regression.txt`: `make tests`, exit `0`, 62/62 tests
  passed.
- `python3 -m py_compile` passed for the validator and its test module.
- `bash -n build/scripts/run-observer-live-probe.sh` passed.
- `git diff --check` passed.
- The protected durable-path diff from
  `89202730dfd19d35d52c35d61b739dad4fcca345` was empty.
- The final changed-path audit contains only the Task 5 allowlist, its
  execution log and evidence, plus the requested ignored controller report.

## Remaining risk

The Spark 4.1.2 bridge invokes the exact, version-pinned `attachHandler` and
`detachHandler` methods reflectively. Direct compilation against those public
methods is blocked by Spark core's published Scala metadata referring to an
unshaded Jetty type that the Spark POM does not expose. `javap` inspection and
the real live runs verify the exact runtime signatures. The bridge is isolated
under `org.apache.spark.dataship.v412`, so a future Spark upgrade must replace
or re-verify this adapter.

The runtime also emits an SLF4J `StaticLoggerBinder` warning after executor
startup. It does not affect the endpoint, workload, exit code, or cleanup and
is not introduced by the health implementation.

## Post-review fixes

The review findings were reproduced in
`task-05-review-fixes-red.txt`. Scala exited `2` because application-ID
registration did not return validity and the package-private runtime seam was
absent. Python exited `1` because the harness did not define the explicit
input-fingerprint contract. `task-05-review-fixes-red-sandbox-attempt.txt`
records an earlier Docker and `uv` sandbox failure and is setup evidence only,
not behavioral RED.

The corrected registration path now behaves as follows:

- blank and null IDs do not create or invoke the endpoint installer;
- a later valid ID clears only transient `EMPTY_APP_ID`, installs exactly
  once, and yields READY with a non-empty application ID;
- unrelated runtime failures remain degraded;
- installer-factory exceptions fail open as `HEALTH_INSTALL_FAILED`, and
  repeated shutdown remains safe;
- an unsupported runtime does not invoke the installer factory or claim
  support; and
- the runtime-construction test seam remains package-private.

`task-05-review-fixes-green.txt` records Scala 22/22 and response assertions
22/22. `task-05-post-review-regressions.txt` records Scala 22/22, the focused
Python pair 39/39, full Python 63/63, Python compilation, and shell syntax with
overall exit `0`.

The rebuilt artifact identity is recorded in
`task-05-post-review-jar-checksum.txt`: host, staged, and live JARs all have
SHA-256
`a3f83d6b9df46caf34a337ca71a87f18468d08f690363a1933d0ebd0cf483c76`.
All post-review live harness runs record starting `HEAD`
`43b4541b284ba054a8a507fe73dc970a21d28c15`, active master and worker image ID
`sha256:e0631eb6ff27614641e88a741dd8b5baa7f8b4eb84c8602d8a763d4962fe2bc9`,
and matching start/end fingerprint
`2e7d50ed3e6931e61286af4bfdbe57305d46bd98e47ff1c2cd3c944e6c018857`.
The SHA-256 fingerprint uses an ordered explicit Task 5 input allowlist and
excludes `.env`, evidence, generated outputs, caches, and secret-prone inputs.

Post-review live evidence:

- `task-05-post-review-live-ready-first-run.txt`: run ID
  `observer-live-20260717T002443Z-440234-16867`, PID `160`, application ID
  `app-20260716232447-0000`, two READY documents, exit `0`, cleanup, endpoint
  removal, and retained mapping;
- `task-05-post-review-live-ready-mapping-reuse.txt`: run ID
  `observer-live-20260717T002521Z-442139-7685`, PID `528`, application ID
  `app-20260716232526-0001`, the same proofs on the reused mapping;
- `task-05-post-review-live-disabled.txt`: PID `908`, two DISABLED documents,
  successful workload and cleanup;
- `task-05-post-review-live-no-spark-ui.txt`: PID `1279`, stable
  `NO_SPARK_UI`, successful workload and cleanup, and no endpoint promise; and
- `task-05-post-review-visual-live-run.txt` plus
  `task-05-post-review-visual-capture.txt`: READY application
  `app-20260716232730-0004` was captured while PID `1584` was alive.

The new visual artifact is `spark-observer-health-ready-post-review.png`. It
visibly contains READY, the application ID, both versions, and only the
allowlisted fields. Its SHA-256 is
`dfc0833b162b1956771a71ae561d0003765fb1236e363c59d2344031c8bb1775`.

`task-05-post-review-spark-4.1.2-javap.txt` records independent Spark 4.1.2
inspection of public `SparkContext.ui`, `WebUI.attachHandler`, and
`WebUI.detachHandler`. That inspection supports but does not replace the live
runtime proof.

`task-05-post-review-guards.txt` records passing final checks for Python
compilation, shell syntax, `git diff --check`, protected durable paths, the
Task 5 changed-path allowlist, secret-pattern filenames, English-only changed
content, and source trailing whitespace.

## Final rereview lifecycle fix

The final rereview exposed an ordering defect in invalid application-ID
handling: blank or null callbacks overwrote unrelated errors and degraded an
already READY runtime.

Two focused tests were written first:

- `preserves an unrelated failure across invalid and valid application IDs`
  covers `INVALID_CONFIG -> blank -> null -> valid -> UI attached` and requires
  `INVALID_CONFIG` to remain degraded; and
- `ignores invalid application IDs after reaching ready` covers
  `READY -> blank -> null` and requires the established application ID, READY
  status, and empty error to remain unchanged.

`task-05-final-rereview-red.txt` records `make observer-tests` exiting `2`.
Of 23 tests, 21 passed and the two new tests failed behaviorally: the first
reported READY instead of DEGRADED, and the second reported DEGRADED instead
of READY. This proves both sides of the defect without relying on compilation
or environment failure.

The minimal fix changes only the invalid-ID branch. It creates
`EMPTY_APP_ID` only when the runtime has neither an application ID nor a prior
error. An invalid callback otherwise returns `false` without mutating identity,
status, UI attachment, or error state. A valid callback continues to clear only
a genuinely transient `EMPTY_APP_ID`.

`task-05-final-rereview-green.txt` records Scala 23/23 with exit `0`.
`task-05-final-rereview-regressions.txt` records Scala 23/23, focused Python
39/39, full Python 63/63, Python compilation, and shell syntax with overall
exit `0`.

The runtime was rebuilt because production code changed.
`task-05-final-rereview-runtime-refresh.txt` records exit `0` and one ALIVE
worker. `task-05-final-rereview-jar-checksum.txt` proves matching host, staged,
and live JAR SHA-256
`5334786124080256cf83e959f90776c575e612a56d20f6462533dc7042bf3544`.

All final-rereview live runs record starting `HEAD`
`43b4541b284ba054a8a507fe73dc970a21d28c15`, master and worker image ID
`sha256:2a79ca1478cf0e54e0156e8d06ddce2d835b3f6b4c00b6b8f3a765039b58e728`,
and identical start/end fingerprint
`b43cf8f7399029ae4a32a69a558fa29e0fe108c94e605ab0d0eadc0f72a722be`.

Final-rereview live evidence:

- `task-05-final-rereview-live-ready-first-run.txt`: run ID
  `observer-live-20260717T004427Z-463635-23244`, PID `131`, application
  `app-20260716234432-0000`, two READY documents, successful workload, exit
  `0`, cleanup, endpoint removal, and persistent mapping retention;
- `task-05-final-rereview-live-ready-mapping-reuse.txt`: run ID
  `observer-live-20260717T004506Z-465527-19659`, PID `473`, application
  `app-20260716234511-0001`, and the same proofs on the reused mapping;
- `task-05-final-rereview-live-disabled.txt`: run ID
  `observer-live-20260717T004544Z-467489-20305`, PID `856`, application
  `app-20260716234549-0002`, two DISABLED documents, exit `0`, and cleanup;
- `task-05-final-rereview-live-no-spark-ui.txt`: run ID
  `observer-live-20260717T004621Z-469319-15473`, PID `1236`, application
  `app-20260716234626-0003`, stable `NO_SPARK_UI`, exit `0`, cleanup, and no
  endpoint promise; and
- `task-05-final-rereview-visual-live-run.txt` plus
  `task-05-final-rereview-visual-capture.txt`: run ID
  `observer-live-20260717T004705Z-470643-22770`, PID `1547`, and application
  `app-20260716234710-0004`.

The final screenshot is `spark-observer-health-ready-final-rereview.png`. It
was visually inspected and visibly contains READY, the non-empty application
ID, both versions, and only the allowlisted fields. PID `1547` was alive before
and after capture. The screenshot SHA-256 is
`792503de400010cdcd1f4f56b08f28e847d8cea02a7340aa81f3e3f29ad1e268`.

`task-05-final-rereview-guards.txt` records unchanged starting `HEAD`, an empty
index, `git diff --check`, Python compilation, shell syntax, protected-path and
changed-path allowlists, secret-pattern filename checks, English-only changed
content, and source trailing-whitespace checks all passing.

## Required historical-evidence waiver

A Task-5-specific live harness RED was not captured before the original
implementation. The Task 4 transcript truthfully shows the exact starting
implementation treating the future health path like a random unknown route,
but using it here is substituted historical evidence rather than the required
Task 5 live RED. This gap cannot be reconstructed after implementation without
rewriting history. Task 5 must not be described as fully compliant or accepted
unless the user explicitly waives this evidence requirement.

## Next slice

Task 6 — implement bounded event capture.
