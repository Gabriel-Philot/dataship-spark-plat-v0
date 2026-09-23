# Operations

## Commands

```bash
make bootstrap    # create local config, pull base images, download jars, wheels, and Go dependencies
make build        # build all project images from local dependencies
make validate     # validate tools, jars, images, and ports
make compose      # start MinIO, ClickHouse, Spark master, worker, and History
make smoke        # run landing ingestion, bronze Delta job, sanity check, then verify History and MinIO outputs
make spark-logs   # load Spark event logs from MinIO into ClickHouse
make services     # print URLs, credentials, and UI setup steps
make tests        # run fast Python unit tests through uv
make test         # alias for make tests
make down         # stop the stack without deleting local data
make clean-data   # delete local MinIO and ClickHouse data
make removeimage  # remove only local project images; keep jars and dependency caches
```

## Local Credentials

These credentials are local-only defaults and must not be reused outside the local lab.

- MinIO user: `spv0minio`
- MinIO password: `spv0minio123`
- ClickHouse user: `spv0`
- ClickHouse password: `spv0clickhouse123`
- ClickHouse database: `spark_observability`

## Service URLs

Run `make services` for the current values. The default URLs are:

- MinIO Console: `http://127.0.0.1:29001`
- Spark History: `http://127.0.0.1:28080`
- Spark Master: `http://127.0.0.1:28081`
- ClickHouse HTTP: `http://127.0.0.1:28123`
- ClickHouse native: `127.0.0.1:29002`

## ClickStack Source Setup

ClickStack is not part of this Compose stack in v0. If a ClickStack or ClickHouse UI is already running, configure it as follows:

1. Open the ClickStack UI.
2. Click Data Source or Schema.
3. Choose database `spark_observability`.
4. Choose table `spark_raw_events`.
5. Choose timestamp column `ingested_at`.
6. Set Default Select to:

```sql
ingested_at AS Timestamp, raw AS Body
```

7. Click `Save New Source`.

Useful optional tables are `spark_sql_executions`, `spark_stages`, and `spark_tasks`.


## Smoke Validation

`make smoke` runs three `spark-submit` calls: landing ingestion, the bronze job, and the sanity check. After the job finishes, `build/scripts/validate-smoke.sh` waits for Spark History to index the completed application and uses the project-local MinIO client image to verify Delta data and event logs. The MinIO checks run with `docker compose run --rm --no-deps` so they do not start extra service dependencies.

## Dependency Caches

`make bootstrap` keeps reusable dependencies outside Docker images:

- Spark jars: `build/config/spark/jars`
- Spark Python wheels: `build/cache/python-wheels`
- Spark image build context: `build/images/spark/context`
- Go loader vendor tree: `build/images/eventlog-loader/vendor`

These caches are intentionally preserved by `make removeimage`.

## Data Cleanup

`make down` stops containers and keeps local data. `make removeimage` removes local project images and keeps jars and dependency caches.

Use `make clean-data` only when you explicitly want to delete local MinIO and ClickHouse data.


## Permission Risk Note

During the May 24, 2026 E2E cleanup, local bind-mounted data under `build/var` contained files owned by container users/root. A normal host-side `rm -rf` failed with `Permission denied` for MinIO object metadata and ClickHouse data/log files.

This is an expected risk when stateful containers write directly to project bind mounts. It is most visible in local development because the files are easy to inspect from the IDE, but their ownership may not match the host user.

Current mitigation:

- `make clean-data` first tries normal host cleanup.
- If that fails, it falls back to a temporary Docker container running as root and deletes only the mounted `build/var` state directories.
- The fallback uses `--pull=never` and chooses the project Spark runtime image when present, otherwise the bootstrapped official Spark base image. It does not download anything during cleanup.

Implemented Spark mitigation: Spark runtime and Spark History now return to the upstream `spark` user after image build steps. The official Apache Spark base image defaults to `uid=185(spark)`, and the project images are validated with container `id` checks after rebuild.

Do not rush the same change for MinIO or ClickHouse. Those official images have their own data-dir and entrypoint permission assumptions. Forcing them to run as the host UID may reduce root-owned files but can break startup or persistence behavior. If file ownership keeps hurting local development, evaluate Docker named volumes as an alternative to bind mounts, with the tradeoff that data becomes less convenient to inspect directly from the IDE.

## Local (Non-S3) Job Output

`spark-master` and `spark-worker` are separate containers with independent container filesystems. In client mode the driver runs on the master and commits `FileOutputCommitter` output by renaming files from `_temporary/` into the final directory, but that rename only works if the driver and the executor that wrote the files can see the same filesystem.

Do not point a job's output at an arbitrary local path (for example a bare `/tmp/...` path). The write will report success (`_SUCCESS` is created by the driver) but the actual part-files stay stranded under `_temporary/` on whichever worker container ran the task, and are never visible from the master or from the host.

Two supported options:

- **Preferred:** use the MinIO-backed lakehouse target from `src/config/lakehouse.yaml`. Its default resolves to `s3a://lakehouse/...`; `s3a://${MINIO_LAKEHOUSE_BUCKET:-lakehouse}/...` is the configuration template and must not be copied literally into job code.
- **Local filesystem output:** if a job must write to local disk (for example ad-hoc debugging), use the shared bind mount `./var/local-output`, mounted at `/opt/spark/local-output` on both `spark-master` and `spark-worker` in `build/docker-compose.yml`. Any other local path is not shared between the two containers and will reproduce this failure.

Reference: `ISSUES.md`/apex-workspace issue #1 documents a reproduction of this failure using an unshared `/tmp/apex_output` path.

## Python Unit Tests

Run fast Python tests from the project root with:

```bash
make tests
```

The IO unit tests use fake Spark reader/writer objects. They validate the same fluent API calls used by PySpark without starting Compose or requiring a Spark cluster. The singular `make test` target is kept as an alias for `make tests`.

## Bootstrap Python Environment

Bootstrap also validates `uv`. If `uv` is missing, `build/scripts/bootstrap.sh` installs it with the official Astral installer and then runs `uv sync` from the project root.
