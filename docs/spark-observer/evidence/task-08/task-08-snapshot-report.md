# Task 8 — live application, job, and stage snapshots

**Status:** `PASS — AWAITING USER ACCEPTANCE`
**Date:** 2026-07-21
**Starting HEAD:** `3b4f7c0629bd755f96fb0cf0b6cc0404512b6b7a`

## Implemented behavior

- `GET /dataship/api/v1/snapshot?limit=N` returns an allowlisted v1 JSON DTO
  for the application, recent jobs, recent stages, and task aggregates.
- `limit` accepts `1..200`, defaults to `50`, and `truncated` is derived from
  a one-record sentinel.
- The Spark 4.1.2 adapter reads `JobDataWrapper` and `StageDataWrapper`
  directly from the native `KVStore` with
  `.reverse().max(limit + 1).closeableIterator()`.
- The servlet returns `200`, `400`, `409`, or an isolated `500`; it is not
  installed when the Observer is disabled.
- No SQL snapshot, DataShip UI tab, durable-path feature, or Task 9 behavior
  was added.

## RED evidence

| Gate | Evidence | Result |
| --- | --- | --- |
| Scala contract | `task-08-focused-scala-red.txt` | Exit `2`; 44 compile errors because the Task 8 API did not exist |
| Python contract/harness | `task-08-focused-python-red.txt` | Exit `1`; 13 focused failures for the absent snapshot validator and harness behavior |
| Rebuilt live runtime before bridge wiring | `task-08-live-endpoint-red.txt` | Workload produced `40/780` and Spark exited normally, but the harness exited `1` because no valid snapshot route appeared |

The pre-implementation host, staged, and driver JARs all had SHA-256
`267774b7886c43f46ec089b6e5d29b747328039841a27481afec35f548758ca1`,
so the live RED did not use a stale artifact.

## GREEN evidence

| Gate | Evidence | Result |
| --- | --- | --- |
| Scala | `task-08-focused-scala-green-after-review.txt` | 48/48 passed |
| Focused Python review | `task-08-review-green.txt` | 63/63 passed |
| Full Python regression | `task-08-regression-python-tests-final.txt` | 87/87 passed |
| Node 24 UI regression | `task-08-regression-ui-tests-final.txt` | 1/1 passed |
| Repository validation | `task-08-regression-validate-final.txt` | `Validation passed` |

A final read-only review found no remaining Critical or Important issue after
the visual provenance, validator diagnostic, and disabled-route corrections.

The canonical host, staged, and driver JARs all had SHA-256
`110c558a10648ca5b552baeb9b1db1ed6f63324ce49aba2f92d0301ce6b04d63`.

## Canonical live proof

Evidence: `task-08-live-green.txt`.

- Run ID: `observer-live-20260721T155451Z-29960-5309`
- Spark application: `app-20260721145456-0000`
- Driver PID: `132`, alive for every validated HTTP read
- `t1`: job `0` and stage `0/0` were `RUNNING`
- `t2`: the same job `0` and stage `0/0` were `SUCCEEDED`
- Stage task aggregates changed from `0/4` completed to `4/4` completed
- Listener events grew from `1` to `6`
- `limit=1` returned one job and one stage with `truncated=true`
- Workload result remained 40 rows, value sum 780, bucket totals
  `180/190/200/210`
- Spark submit and harness both exited `0`; the driver process and endpoint
  were absent after shutdown while the Docker port mapping persisted

The opt-out run in `task-08-live-disabled.txt` used run
`observer-live-20260721T161133Z-54952-23755`, application
`app-20260721151137-0005`, and driver PID `2641`. While that PID was alive, the
snapshot path returned the Spark UI's native unknown-route behavior (`302` to
`/jobs/`, then `200`) instead of an Observer response. The same workload still
produced `40/780`, and Spark/harness both exited `0`.

## Real Spark UI evidence

The screenshots below were captured by direct Playwright commands against the
real driver UI during one run; none was generated from a template:

- Run ID: `observer-live-20260721T161421Z-59245-19538`
- Spark application: `app-20260721151425-0007`
- Driver PID: `3360`
- `task-08-spark-ui-jobs-active.png`: `Active Jobs (1)` shows job `0`, the
  real `sum` operation, and task progress `2/4 (2 running)`.
- `task-08-spark-ui-stages-active.png`: `Active Stages (1)` shows stage `0`
  and the same `2/4 (2 running)` progress.
- `task-08-spark-ui-jobs-completed.png`: `Completed Jobs (5)` shows every
  workload job completed, including the original job `0` at `4/4` tasks.
- `task-08-spark-ui-stages-completed.png`: `Completed Stages (5)` shows task
  completion and real shuffle read/write values.

`task-08-playwright-provenance-live.txt` ties the full run ID, application ID,
PID, snapshot transition, workload result, and clean shutdown together. The
four `task-08-playwright-*.txt` capture transcripts record each direct
Playwright command, URL, selector, output path, timestamp, and exit code. Image
hashes are recorded in `task-08-playwright-sha256.txt`.

The native Spark UI does not expose listener-bus queue names. The dedicated
`dataship-observer` queue is proven by the Scala listener test and live counter
growth; its own visual surface belongs to the DataShip UI task.

## Infrastructure handoff

`task-08-infra-down.txt` records `make down` completing successfully, and
`task-08-infra-down-verification.txt` records an empty Compose service list.
Built images were preserved.

## Remaining risk

Jobs and stages are read through separate bounded native-store queries, so the
endpoint does not claim a cross-list atomic snapshot. Spark internal APIs are
still version-specific and remain isolated under the `v412` adapter.

## Next task title only

**Task 9 — add safe live SQL execution snapshots.** It is not authorized or
started by this Task 8 result.
