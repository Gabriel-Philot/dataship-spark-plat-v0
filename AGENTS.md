# DataShip Spark Platform Agent Instructions

## Scope

These instructions apply to the entire `spark-plat-v0` repository. A more specific `AGENTS.md` in a subdirectory may add or override rules for that subtree.

Keep volatile task status out of this file. Use the relevant implementation plan, execution log, and evidence directory for task-specific state.

Repository-wide sections define lightweight working agreements. Sections explicitly named for the Spark Observer apply only to Spark Observer tasks.

## Canonical project context

Read the smallest relevant set of documents before changing the repository:

- `README.md` for the local platform workflow.
- `docs/dev-design/architecture.md` for platform architecture.
- `docs/dev-design/operations.md` for operational constraints.
- `docs/spark-observer/2026-07-15-dataship-spark-observer-live-driver-design.md` for the Spark Observer design.
- `docs/spark-observer/2026-07-15-dataship-spark-observer-implementation-plan.md` for ordered Observer tasks and gates.
- `docs/spark-observer/execution-log.md` for executed commands, results, and user acceptance.

Do not treat this file as a replacement for those documents.

## Repository-wide working protocol

- Before editing, inspect the branch, `HEAD`, working tree, relevant files, available tools, and repository-specific instructions.
- Preserve user-made changes and unrelated dirty-worktree content.
- Prefer minimal, targeted diffs. Do not introduce unrelated refactors.
- If a requirement or external review comment is technically questionable, verify it against the repository and runtime before implementing it.
- Use verification proportional to the change and its risk.
- Outside Spark Observer tasks, documentation-only or unrelated changes do not require an artificial RED, visual evidence, an execution log, or task-by-task acceptance unless the user explicitly requests them.
- A request to commit or push authorizes only the current reviewed scope; it does not authorize additional implementation.

## Repository-wide commit and push rules

- Do not stage, commit, push, open a pull request, or switch branches unless the user explicitly requests that action.
- Stage explicit paths. Never sweep unrelated files into a commit.
- After a commit, verify the local `HEAD` and working tree.
- After a push, additionally verify that the upstream commit matches the local `HEAD`.

## Spark Observer task protocol

- Work on one approved Observer task or observable hypothesis at a time.
- Do not start the next Observer task until the current task has passed its documented gates and the user has explicitly accepted it.
- At the end of each Observer task, report the behavior implemented, exact files, RED, GREEN, observable evidence, regressions, remaining risk, and only the title of the next task.
- Observer task commits must include the corresponding execution-log update and evidence directory when the plan requires them.

### Test-driven Observer changes

- For production behavior, write the focused test first and observe a valid RED.
- A valid RED must fail because the requested behavior is absent, not because a target, tool, dependency, or runner is missing.
- Implement the smallest change that makes the focused test GREEN.
- Run the focused test, the applicable regression gates, and the task-specific verifier.
- Do not recreate historical RED evidence by removing working code. If a raw historical transcript was not preserved, state that explicitly.

## Spark Observer reproducible evidence

- Persist proportional human-reviewable evidence under `docs/`, normally `docs/spark-observer/evidence/task-XX/`.
- Important verification commands must be implemented as versioned scripts or Make targets.
- Do not use ignored, temporary, or local-only wrappers as the primary acceptance command.
- Capture critical terminal evidence with a direct versioned command, for example:

```bash
script -qefc 'make observer-verify' \
  docs/spark-observer/evidence/task-02/task-02-reproducible-verification.txt
```

- Preserve the raw transcript alongside any rendered screenshot.
- The transcript must show the direct command, current commit, relevant image identifiers, actual test output, and final exit code.
- Screenshots are supporting evidence; the raw transcript and reproducible command are authoritative.
- Never expose credentials, tokens, `.env` contents, or secret-prone configuration in evidence.

## Spark Observer fresh-checkout prerequisites

The repository is intentionally containerized. The host should not need Java, Scala, sbt, or Node for Spark Observer work.

On a fresh checkout, prepare the repository before running the Observer verifier:

```bash
make bootstrap
make build
make observer-verify
```

- `make bootstrap` prepares `.env`, dependencies, pinned base images, and local caches.
- `make build` creates the local runtime images required by `make validate`.
- `make observer-verify` assumes both steps already succeeded; it does not replace them.
- `make compose` is not a prerequisite for the Task 2 toolchain verifier.

Pinned Task 2 toolchain images:

```text
SBT_IMAGE=sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.10.11_2.13.16
NODE_IMAGE=node:24.13.1-bookworm-slim
```

Do not silently substitute other images in acceptance evidence.

## Cursor integration check

These instructions guide Cursor Agent/Chat and Inline Edit. They do not replace repository tests or verifiers and do not apply to Cursor Tab autocomplete.

To verify instruction activation, open a new Cursor Chat in this repository and send:

```text
Without changing files, list the applicable AGENTS.md files and canonical
documents, then state whether you are authorized to start Task 3 merely
because it is documented.
```

The response must identify the root `AGENTS.md`, list the relevant canonical documents, and state that documentation alone does not authorize starting Task 3.

## Spark Observer boundaries

- Keep the plugin opt-in.
- Keep the v0 plugin driver-only; do not add an executor plugin unless a later approved task requires it.
- Keep Spark `4.1.2`, Scala binary `2.13`, Java `17`, and the exact tested runtime adapter explicit.
- Listener callbacks must remain lightweight, bounded, non-blocking, and free of external I/O.
- Use Spark's native stores as the source of truth for live jobs, stages, and SQL state.
- Isolate Spark internal APIs under the version-specific adapter namespace.
- Observability failures must be fail-open after successful plugin class loading and must not change the Spark job result.

## Frozen durable-path scope

Spark Observer feature tasks must not redesign or silently modify the durable path:

```text
Spark event log -> MinIO -> History Server -> Go loader -> ClickHouse
```

Treat these paths as protected unless the user approves an architecture change:

- `build/clickhouse/`
- `build/images/eventlog-loader/`
- `build/images/minio/init-buckets.sh`
- `build/config/spark/spark-defaults.conf`

The loader, ClickHouse schemas, MinIO buckets/prefixes, and native event-log path are regression scope, not Observer feature scope.

## Observer sequencing rule

Documentation or completion of a later Observer task does not authorize its implementation. Each next Observer task requires a new explicit user request.
