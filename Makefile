SHELL := /usr/bin/env bash
ROOT_DIR := $(CURDIR)
ENV_FILE := $(ROOT_DIR)/.env
COMPOSE_FILE := $(ROOT_DIR)/build/docker-compose.yml
COMPOSE := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)
SBT_CACHE_DIR := $(ROOT_DIR)/build/cache/sbt
COURSIER_CACHE_DIR := $(ROOT_DIR)/build/cache/coursier

include .env.example
-include .env

.PHONY: bootstrap build validate compose ingest-landing bronze sanity smoke spark-logs services test tests observer-tests observer-jar observer-ui-tests observer-verify down removeimage clean-data

SPARK_SUBMIT := $(COMPOSE) exec -T spark-master env PYTHONPATH=/opt/spark/src /opt/spark/bin/spark-submit \
	--master spark://spark-master:7077 \
	--deploy-mode client \
	--conf spark.executorEnv.PYTHONPATH=/opt/spark/src

OBSERVER_SBT := docker run --rm \
	--user "$$(id -u):$$(id -g)" \
	--env HOME=/tmp \
	--env COURSIER_CACHE=/cache/coursier \
	--env SBT_OPTS="-Dsbt.global.base=/cache/sbt/global -Dsbt.boot.directory=/cache/sbt/boot -Dsbt.ivy.home=/cache/sbt/ivy -Dsbt.supershell=false" \
	--volume "$(ROOT_DIR):/workspace" \
	--volume "$(SBT_CACHE_DIR):/cache/sbt" \
	--volume "$(COURSIER_CACHE_DIR):/cache/coursier" \
	--workdir /workspace/spark-observer \
	"$(SBT_IMAGE)" \
	sbt

bootstrap:
	@build/scripts/bootstrap.sh

build:
	@build/scripts/validate-bootstrap.sh
	@build/scripts/prepare-image-contexts.sh >/dev/null
	@docker build \
		--build-arg MINIO_BASE_IMAGE=$(MINIO_BASE_IMAGE) \
		-f build/images/minio/Dockerfile.server \
		-t $(MINIO_IMAGE) \
		build/images/minio
	@docker build \
		--build-arg MINIO_MC_BASE_IMAGE=$(MINIO_MC_BASE_IMAGE) \
		-f build/images/minio/Dockerfile.client \
		-t $(MINIO_MC_IMAGE) \
		build/images/minio
	@docker build \
		--build-arg CLICKHOUSE_BASE_IMAGE=$(CLICKHOUSE_BASE_IMAGE) \
		-f build/images/clickhouse/Dockerfile \
		-t $(CLICKHOUSE_IMAGE) \
		build/images/clickhouse
	@docker build \
		--build-arg SPARK_BASE_IMAGE=$(SPARK_BASE_IMAGE) \
		-f build/images/spark/Dockerfile \
		-t $(SPARK_RUNTIME_IMAGE) \
		build/images/spark
	@docker build \
		--build-arg SPARK_RUNTIME_IMAGE=$(SPARK_RUNTIME_IMAGE) \
		-f build/images/spark-history/Dockerfile \
		-t $(SPARK_HISTORY_IMAGE) \
		build/images/spark-history
	@docker build \
		--build-arg GO_BASE_IMAGE=$(GO_BASE_IMAGE) \
		-f build/images/eventlog-loader/Dockerfile \
		-t $(LOADER_IMAGE) \
		build/images/eventlog-loader

validate:
	@build/scripts/validate.sh

compose: validate
	@$(COMPOSE) up -d minio minio-init clickhouse spark-master spark-worker spark-history
	@build/scripts/wait-ready.sh

ingest-landing:
	@$(SPARK_SUBMIT) /opt/spark/src/apps/sample_scripts/simple_persist_customers_landing.py

bronze:
	@$(SPARK_SUBMIT) /opt/spark/src/apps/sample_scripts/smoke_job_plat_minio.py

sanity:
	@$(SPARK_SUBMIT) /opt/spark/src/apps/sample_scripts/check_sanity.py

smoke: ingest-landing bronze sanity
	@build/scripts/validate-smoke.sh

spark-logs:
	@$(COMPOSE) run --rm eventlog-loader
	@build/scripts/validate-clickhouse.sh

services:
	@build/scripts/services.sh

test: tests

tests:
	@uv run pytest

observer-tests:
	@mkdir -p "$(SBT_CACHE_DIR)" "$(COURSIER_CACHE_DIR)"
	@$(OBSERVER_SBT) test

observer-jar:
	@mkdir -p "$(SBT_CACHE_DIR)" "$(COURSIER_CACHE_DIR)"
	@$(OBSERVER_SBT) package

observer-ui-tests:
	@docker run --rm \
		--user "$$(id -u):$$(id -g)" \
		--volume "$(ROOT_DIR):/workspace:ro" \
		--workdir /workspace \
		"$(NODE_IMAGE)" \
		node --test spark-observer/src/test/js/toolchain.test.mjs

observer-verify:
	@build/scripts/verify-observer-toolchain.sh

down:
	@$(COMPOSE) down

removeimage:
	@docker rmi $(SPARK_RUNTIME_IMAGE) $(SPARK_HISTORY_IMAGE) $(LOADER_IMAGE) $(MINIO_IMAGE) $(MINIO_MC_IMAGE) $(CLICKHOUSE_IMAGE) || true

clean-data:
	@$(COMPOSE) down -v --remove-orphans || true
	@mkdir -p build/var
	@if ! rm -rf build/var/minio-data build/var/clickhouse-data build/var/clickhouse-logs build/var/metrics 2>/dev/null; then \
		echo "Local data contains root-owned files; cleaning through Docker."; \
		cleanup_image="$(SPARK_RUNTIME_IMAGE)"; \
		if ! docker image inspect "$$cleanup_image" >/dev/null 2>&1; then cleanup_image="$(SPARK_BASE_IMAGE)"; fi; \
		docker run --rm --pull=never --user 0 \
			-v "$(ROOT_DIR)/build/var:/target" \
			--entrypoint /bin/sh \
			"$$cleanup_image" \
			-c 'rm -rf /target/minio-data /target/clickhouse-data /target/clickhouse-logs /target/metrics'; \
	fi
	@mkdir -p build/var/minio-data build/var/clickhouse-data build/var/clickhouse-logs build/var/metrics
