# DataShip Spark Observer v0 Implementation Plan

**Revisão:** 2026-07-16 — feedback técnico incorporado antes da Task 1.

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:test-driven-development` and `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do not use subagents unless the user explicitly authorizes them.

**Goal:** construir e provar um plugin JVM opt-in no driver Spark 4.1.2 que exponha estado live, endpoints versionados e uma aba mínima na Spark UI sem interferir no job nem alterar o caminho durável existente.

**Architecture:** o plugin terá uma entrada pública pequena em `io.dataship.spark.observer` e todo acesso a APIs internas do Spark ficará isolado em `org.apache.spark.dataship.v412`. Jobs, stages e SQL serão lidos dos stores nativos; apenas contadores e uma janela limitada de transições pertencerão ao plugin. Build e testes JVM usarão um container sbt/Java 17, enquanto o JAR será executado exclusivamente pelo driver no container Spark.

**Tech Stack:** Spark `4.1.2`, Scala `2.13.16`/binary `2.13`, Java `17`, sbt `1.10.11`, ScalaTest `3.2.19`, PySpark, Bash, Python `3.10+`, Node `24.13.1` exclusivamente em container para testes do JavaScript estático, Docker Compose.

**Design aprovado:** `docs/spark-observer/2026-07-15-dataship-spark-observer-live-driver-design.md`

## Global Constraints

- O host não deve receber Java, Scala, sbt ou Node; ele precisa apenas das ferramentas já usadas pelo repositório, especialmente Docker e Make.
- A imagem de build JVM será `sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.10.11_2.13.16`.
- A imagem de testes da UI será `node:24.13.1-bookworm-slim` e todo teste JavaScript passará pelo target conteinerizado `observer-ui-tests`.
- O runtime continuará sendo `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu`.
- Spark e Scala serão dependências `provided`; o JAR não incluirá runtime Scala nem classes Spark.
- O artefato será `dataship-spark-observer_2.13-0.1.0-SNAPSHOT.jar`.
- A ativação exigirá `spark.plugins=io.dataship.spark.observer.SparkDataShipPlugin` e `spark.dataship.observer.enabled=true`.
- `executorPlugin()` sempre retornará `null`.
- Apenas o adapter `org.apache.spark.dataship.v412` poderá importar APIs `private[spark]` ou internas.
- O plugin não fará I/O externo e não escreverá em MinIO, Kafka, ClickHouse ou qualquer outro serviço.
- Schemas ClickHouse, loader Go, buckets/prefixos MinIO e event log nativo estão congelados para features.
- A UI do driver será publicada somente em `127.0.0.1:${SPARK_DRIVER_UI_PORT:-24040}:4040`.
- O teste aceitará um único driver por vez e configurará `spark.ui.port=4040` e `spark.port.maxRetries=0`.
- A publicação `24040:4040` pertence ao container persistente `spark-master`; depois do driver, o gate exige endpoint indisponível e ausência do processo, não remoção do mapeamento Docker.
- Toda task que alterar o JAR deverá executar `make observer-runtime-refresh` antes de qualquer prova live; esse target recompila, faz staging, reconstrói a imagem Spark e recria `spark-master` e `spark-worker`.
- Nenhuma resposta poderá expor `SparkConf` completo, ambiente, credenciais, fonte do usuário ou descrição SQL bruta.
- Nenhuma task poderá alterar código de tasks posteriores para “adiantar” trabalho.

---

## Correções incorporadas antes da Task 1

| # | Decisão executável |
| --- | --- |
| 1 | `execution-log.md` participa de todos os checkpoints de commit. |
| 2 | Toda prova live usa `observer-runtime-refresh` e valida o checksum do JAR no container. |
| 3 | O gate distingue mapping persistente, endpoint temporário e processo interno do driver. |
| 4 | Snapshot SQL não promete o literal de `spark.sql(...)`; expõe metadados e descrição identificada. |
| 5 | Descrição fica desabilitada por default e, quando opt-in, é redigida antes do truncamento e testada contra valores sentinela. |
| 6 | Node 24 é executado apenas por imagem fixada e target Make conteinerizado. |
| 7 | A baseline começa com `bootstrap` e `build` antes de `validate`. |
| 8 | Guards usam a baseline fixa, caminhos imutáveis e teste semântico do contrato MinIO/event log/History. |
| 9 | Jobs e stages usam consulta `KVStore.max(limit + 1)`, sem materialização total via helpers de lista. |
| 10 | Spark UI desabilitada e instalação idempotente possuem gates próprios. |
| 11 | Runtime não suportado não promete `/health`; a guarda impede criar o adapter quando possível. |
| 12 | `429` foi removido do v0 por YAGNI; eventual rate limit exigirá novo design e teste determinístico. |
| 13 | O cleanup identifica e verifica o processo real dentro de `spark-master`. |
| 14 | Vermelho válido falha no comportamento testado, nunca por target, runner ou ferramenta ausente. |

---

## Frame obrigatório de execução

Cada task é uma unidade de revisão e um commit. O executor deve seguir esta sequência:

1. Confirmar branch, baseline fixa `89202730dfd19d35d52c35d61b739dad4fcca345`, árvore limpa e allowlist exata de arquivos da task.
2. Registrar no `docs/spark-observer/execution-log.md`:

   ```text
   Task:
   Hipótese:
   Mudança mínima:
   Teste vermelho:
   Retorno observável esperado:
   Regressão:
   Status: RUNNING
   ```

3. Escrever primeiro o teste estreito.
4. Executar o teste e confirmar falha pela razão prevista.
5. Fazer somente a implementação mínima da task.
6. Executar teste verde, regressão e prova real descritos na task.
7. Atualizar o registro para `PASS`, `FAIL` ou `BLOCKED`, anexando comandos e resultados sem segredos.
8. Apresentar ao usuário:

   | Campo | Evidência obrigatória |
   | --- | --- |
   | Fatia | comportamento único implementado |
   | Arquivos | lista exata alterada |
   | Teste vermelho | comando, exit code e motivo |
   | Teste verde | comando, exit code e contagem |
   | Prova live | requisição e campos vistos com o processo vivo |
   | Regressão | comandos e resultados |
   | Risco restante | o que ainda não foi provado |
   | Próxima fatia | apenas o título, sem iniciá-la |

9. Parar sem commit e aguardar o usuário responder `ACEITO`.
10. Depois do aceite, executar somente o checkpoint de commit da task, sempre incluindo `docs/spark-observer/execution-log.md`, e mostrar hash/status.
11. Parar novamente. A próxima task exige um novo pedido explícito.

Se qualquer gate falhar, não commitar e não iniciar outra task. Aplicar `superpowers:systematic-debugging`, registrar `FAIL` ou `BLOCKED` e pedir direção quando necessário.

A partir da Task 2, toda task executará `make tests` antes do user gate para manter ativo `tests/test_observer_platform_contract.py`. Se a task alterar o JAR e possuir prova live, `make observer-runtime-refresh` é pré-condição obrigatória dessa prova.

Antes de cada commit, a lista de arquivos alterados deve ser subconjunto de `Files` da task mais `docs/spark-observer/execution-log.md`. Também deve continuar sem saída:

```bash
git diff --name-only 89202730dfd19d35d52c35d61b739dad4fcca345 -- \
  build/clickhouse \
  build/images/eventlog-loader \
  build/images/minio/init-buckets.sh \
  build/config/spark/spark-defaults.conf
```

Uma saída não vazia bloqueia o commit e exige decisão de arquitetura do usuário. Como `.env.example` e `build/docker-compose.yml` recebem mudanças permitidas, seus valores do lado direito serão protegidos por testes semânticos fixados na baseline: bucket `spark-logs`, prefixo `events/`, `spark.eventLog.enabled=true` e diretórios `s3a://spark-logs/events` do event log e do History Server.

---

## Contratos fixados

### Configuração

| Chave | Default | Regra |
| --- | --- | --- |
| `spark.dataship.observer.enabled` | `false` | `true` habilita listener, snapshots e UI |
| `spark.dataship.observer.queue.capacity` | `1024` | inteiro entre `1` e `65536` |
| `spark.dataship.observer.transitions.capacity` | `128` | inteiro entre `1` e `1024` |
| `spark.dataship.observer.snapshot.limit` | `50` | inteiro entre `1` e `200` |
| `spark.dataship.observer.sql.maxLength` | `1024` | inteiro entre `64` e `8192` |
| `spark.dataship.observer.sql.description.enabled` | `false` | opt-in explícito para expor a descrição fornecida pelo Spark após redação |
| `spark.dataship.observer.testMode` | `false` | permite controles determinísticos somente nos testes |
| `spark.dataship.observer.test.processingDelayMs` | `0` | deve ser `0` quando `testMode=false`; máximo `1000` |

O runtime suportado é exatamente Spark `4.1.2`. Em runtime suportado, configuração inválida pode produzir `/health` com `DEGRADED`. Em versão não suportada, a guarda deve impedir a criação do adapter `v412`, registrar `UNSUPPORTED_SPARK_VERSION` e deixar o job continuar quando o classloading público permitir; não há promessa de endpoint HTTP porque o próprio handler depende das internals da UI suportada. Incompatibilidade binária ou `LinkageError` anterior à guarda continua sendo risco coberto apenas pelo build e smoke contra a imagem exata.

### Estado e rotas

- Estados: `STARTING`, `READY`, `DEGRADED`, `DISABLED`, `STOPPING`.
- Fila do listener bus: `dataship-observer`.
- `GET /dataship/api/v1/health`
- `GET /dataship/api/v1/debug/counters`
- `GET /dataship/api/v1/snapshot?limit=N`
- `GET /dataship/`
- Assets: `/dataship/static/app.css` e `/dataship/static/app.js`.
- O v0 não implementará limitador HTTP próprio nem responderá `429`; essa feature só será reaberta mediante evidência de necessidade.

Toda resposta JSON bem-sucedida terá `schemaVersion`, `pluginVersion`, `sparkVersion`, `appId`, `mode` e `capturedAt`.

### Interfaces internas

```scala
trait SparkSnapshotSource {
  def application(): ApplicationView
  def jobs(limit: Int): Seq[JobView]
  def stages(limit: Int): Seq[StageView]
  def sqlExecutions(limit: Int): Seq[SqlExecutionView]
}

trait Spark412Bridge {
  def installListener(listener: ObserverListener): Unit
  def installHttp(runtime: ObserverRuntime): Unit
  def installUi(runtime: ObserverRuntime): Unit
  def snapshotSource: SparkSnapshotSource
}
```

`LiveSnapshotService` consumirá apenas `SparkSnapshotSource`; nenhum DTO HTTP importará classes internas do Spark.

---

## Mapa de arquivos planejado

### Módulo JVM

- `spark-observer/build.sbt`: versões, dependências `provided`, testes e nome do JAR.
- `spark-observer/project/build.properties`: sbt `1.10.11`.
- `spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipPlugin.scala`: entrada `SparkPlugin`.
- `spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipDriverPlugin.scala`: lifecycle e fail-open.
- `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala`: parsing e validação das flags.
- `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala`: ownership e shutdown dos componentes.
- `spark-observer/src/main/scala/io/dataship/spark/observer/events/`: fila, listener, estado e contadores.
- `spark-observer/src/main/scala/io/dataship/spark/observer/api/`: DTOs, JSON, servlets e snapshot service.
- `spark-observer/src/main/scala/org/apache/spark/dataship/v412/`: único adapter de internals Spark 4.1.2.
- `spark-observer/src/main/resources/io/dataship/spark/observer/ui/`: HTML/CSS/JavaScript próprios.
- `spark-observer/src/test/scala/`: testes unitários JVM.
- `spark-observer/src/test/js/`: testes `node:test` executados exclusivamente pelo target conteinerizado.

### Integração local

- `src/apps/observer_live_probe.py`: workload determinístico live.
- `tests/test_observer_live_probe.py`: contrato puro do workload.
- `build/scripts/run-observer-live-probe.sh`: lifecycle do processo e cleanup.
- `build/scripts/assert-observer-response.py`: validações HTTP/JSON sem `jq`.
- `tests/test_observer_response_assertions.py`: testes do validador.
- `build/scripts/validate-observer-e2e.sh`: gate completo do Observer.
- `Makefile`, `.env.example`, `.gitignore`, Compose e scripts de build: targets, imagem sbt, porta e empacotamento.
- `docs/spark-observer/execution-log.md`: evidência incremental de cada task.

---

## Task 1: congelar a baseline e o protocolo de evidência

**Hipótese:** a plataforma pode ser preparada a partir de checkout novo, passa seus gates sem o plugin, não publica `24040` na baseline e mantém o lado direito funcional antes de qualquer feature.

**Files:**

- Create: `docs/spark-observer/execution-log.md`
- No production code changes.

**Test first:**

- [ ] Registrar branch, `HEAD`, status e nomes dos serviços.
- [ ] Executar `make bootstrap`, que cria/atualiza `.env`, baixa dependências e aquece os caches ignorados pelo Git.
- [ ] Executar `make build`, `make validate` e `make tests` nessa ordem.
- [ ] Executar `make down` para remover containers antigos sem apagar volumes.
- [ ] Antes de `make compose`, confirmar que nenhum listener do host ocupa `127.0.0.1:24040`.
- [ ] Executar `make compose` e `make smoke`.
- [ ] Executar `docker compose --env-file .env -f build/docker-compose.yml port spark-master 4040` e confirmar falha porque a baseline ainda não publica a porta.
- [ ] Executar `curl --fail --silent --show-error http://127.0.0.1:24040/dataship/api/v1/health`.
- [ ] Confirmar que o `curl` falha com conexão recusada porque a porta do driver ainda não existe.
- [ ] Executar `make spark-logs` e registrar apenas contagens, nunca credenciais.

**Acceptance criteria:**

- `make bootstrap`, `make build`, `make validate`, `make tests`, `make smoke` e `make spark-logs` retornam exit code `0`.
- A porta `24040` não aceita conexão.
- O execution log registra o commit `89202730dfd19d35d52c35d61b739dad4fcca345`.
- Nenhum arquivo de ClickHouse, loader Go, MinIO ou event log é alterado.

**User gate:** apresentar a baseline completa e parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add docs/spark-observer/execution-log.md
git commit -m "test: record Spark Observer baseline"
```

---

## Task 2: adicionar o toolchain JVM totalmente conteinerizado

**Hipótese:** o módulo Scala e os testes JavaScript compilam/testam sem Java, Scala, sbt ou Node instalados no host.

**Files:**

- Create: `spark-observer/build.sbt`
- Create: `spark-observer/project/build.properties`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/BuildInfo.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/BuildInfoSpec.scala`
- Modify: `.env.example`
- Modify: `.gitignore`
- Modify: `Makefile`
- Modify: `build/scripts/bootstrap.sh`
- Modify: `build/scripts/validate-bootstrap.sh`
- Create: `spark-observer/src/test/js/toolchain.test.mjs`
- Create: `tests/test_observer_platform_contract.py`

**Interfaces:**

- `BuildInfo.PluginVersion = "0.1.0-SNAPSHOT"`
- `BuildInfo.SupportedSparkVersion = "4.1.2"`
- `BuildInfo.ScalaBinaryVersion = "2.13"`
- Make targets: `observer-tests`, `observer-jar` e `observer-ui-tests`.
- `SBT_IMAGE=sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.10.11_2.13.16`.
- `NODE_IMAGE=node:24.13.1-bookworm-slim`.

**Toolchain preparation:**

- [ ] Criar o projeto sbt, os targets Docker `observer-tests`/`observer-jar` e o target Docker `observer-ui-tests` antes do teste de comportamento.
- [ ] Fazer `bootstrap.sh` puxar as imagens sbt e Node e aquecer caches em `build/cache/sbt` e `build/cache/coursier`.
- [ ] Criar `toolchain.test.mjs` verificando major `24` e executar `make observer-ui-tests`; esperar `PASS` dentro da imagem fixada.
- [ ] Criar `tests/test_observer_platform_contract.py` fixando bucket `spark-logs`, prefixo `events/`, event log habilitado e os dois caminhos `s3a://spark-logs/events`; executar com `make tests` e esperar `PASS` antes de alterar `.env.example`.

**Red phase:**

- [ ] Criar `BuildInfoSpec` verificando as três constantes e o nome do artefato.
- [ ] Executar `make observer-tests`.
- [ ] Confirmar falha de compilação por `BuildInfo` inexistente; ausência do Make target, Docker ou runner não é um vermelho válido.

**Green phase:**

- [ ] Adicionar `BuildInfo` e configurar Scala `2.13.16`, Spark Core/SQL `4.1.2` como `provided` e ScalaTest `3.2.19` em `Test`.
- [ ] Fazer os targets sbt rodarem em Docker com usuário do host e volumes de cache, sem chamar `java` ou `sbt` no host.
- [ ] Executar `make observer-tests`; esperar todos os testes verdes.
- [ ] Executar `make observer-jar`; esperar `spark-observer/target/scala-2.13/dataship-spark-observer_2.13-0.1.0-SNAPSHOT.jar`.
- [ ] Listar o JAR dentro do container e confirmar ausência de `org/apache/spark/` e `scala/`.

**Regression:**

```bash
make validate
make tests
```

**Acceptance criteria:**

- `command -v java`, `command -v scalac`, `command -v sbt` e `command -v node` podem continuar falhando no host.
- Build e testes usam somente Docker.
- Spark/Scala não são empacotados.
- O teste de contrato do lado direito passa e será regressão obrigatória das tasks seguintes.
- `make validate` e `make tests` continuam verdes.

**User gate:** mostrar comandos, imagem usada, conteúdo relevante do JAR e parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add .env.example .gitignore Makefile build/scripts/bootstrap.sh build/scripts/validate-bootstrap.sh spark-observer tests/test_observer_platform_contract.py docs/spark-observer/execution-log.md
git commit -m "build: add containerized Spark Observer toolchain"
```

---

## Task 3: produzir o JAR mínimo carregável e opt-in

**Hipótese:** Spark carrega o plugin somente com as duas flags opt-in, cria apenas o `DriverPlugin`, não cria plugin de executor e conclui o workload existente.

**Files:**

- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipPlugin.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipDriverPlugin.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/PluginBootstrapSpec.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/ObserverConfigSpec.scala`
- Modify: `Makefile`
- Modify: `build/scripts/prepare-image-contexts.sh`

**Interfaces:**

- Public class: `io.dataship.spark.observer.SparkDataShipPlugin`.
- `driverPlugin(): DriverPlugin` returns a new `SparkDataShipDriverPlugin`.
- `executorPlugin(): ExecutorPlugin` returns `null`.
- `ObserverConfig.from(sc.getConf)` lê `spark.dataship.observer.enabled`, com default `false`.
- O JAR gerado é copiado para `/opt/spark/jars/` durante `make build`.
- O target `observer-runtime-refresh` executa, nesta ordem: `observer-jar`, staging pelo `prepare-image-contexts.sh`, rebuild da imagem `SPARK_RUNTIME_IMAGE`, remoção dos containers Spark antigos, preflight da porta host quando o Compose publicar 4040 e recriação de `spark-master` e `spark-worker`.

**Red phase:**

- [ ] Criar testes que instanciam `SparkDataShipPlugin`, verificam o tipo do driver, `executorPlugin() == null` e os estados enabled/disabled.
- [ ] Executar `make observer-tests`.
- [ ] Confirmar falha de compilação porque as classes ainda não existem.

**Green phase:**

- [ ] Implementar apenas as classes de bootstrap e o parsing da flag enabled; `init` retorna mapa vazio e não faz I/O.
- [ ] Integrar o JAR produzido ao contexto da imagem Spark sem colocá-lo no bootstrap manifest de dependências externas.
- [ ] Executar `make observer-tests` e `make observer-runtime-refresh`.
- [ ] Verificar dentro de `spark-master` que o checksum do JAR corresponde ao artefato recém-produzido no host.
- [ ] Executar `make compose` e `make smoke` sem flags do plugin.
- [ ] Executar `check_sanity.py` com `spark.plugins` e `spark.dataship.observer.enabled=true`.
- [ ] Confirmar exit code `0` nos dois modos e nenhuma rota `/dataship/`.

**Acceptance criteria:**

- O JAR está em `/opt/spark/jars/`.
- O checksum prova que a imagem/container usa o JAR recém-produzido, não um artefato anterior.
- Estar no classpath não ativa o plugin.
- O plugin habilitado é carregado no driver e não nos executors.
- Nenhuma thread, listener, endpoint ou aba customizada existe ainda.
- O resultado do workload é idêntico com plugin ligado e desligado.

**User gate:** mostrar testes unitários, duas execuções Spark e parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add Makefile build/scripts/prepare-image-contexts.sh spark-observer docs/spark-observer/execution-log.md
git commit -m "feat: add minimal driver-only Spark plugin"
```

---

## Task 4: criar workload e harness determinísticos para prova live

**Hipótese:** um `spark-submit` identificado permanece vivo tempo suficiente para duas leituras, executa dois jobs, um shuffle e SQL, e o cleanup sempre remove o driver sem confundir processo, endpoint e mapeamento Docker.

**Files:**

- Create: `src/apps/observer_live_probe.py`
- Create: `tests/test_observer_live_probe.py`
- Create: `build/scripts/run-observer-live-probe.sh`
- Modify: `.env.example`
- Modify: `build/docker-compose.yml`
- Modify: `Makefile`

**Interfaces:**

- `observer_live_probe.py --rows 40 --partitions 4 --delay-ms 75 --hold-seconds 2`.
- Make target `observer-live`.
- Porta `127.0.0.1:${SPARK_DRIVER_UI_PORT:-24040}:4040`.
- O harness aceita `OBSERVER_ENABLED=true|false`, cria um `OBSERVER_RUN_ID` único, inclui esse ID no nome/configuração do submit e grava apenas artefatos temporários em `/tmp`.

**Red phase:**

- [ ] Criar testes puros para defaults, rejeição de valores negativos e resultado determinístico esperado.
- [ ] Executar `uv run pytest tests/test_observer_live_probe.py -q`.
- [ ] Confirmar falha porque o módulo não existe.
- [ ] Criar uma asserção de Compose que procura a porta 4040.
- [ ] Executar `docker compose --env-file .env -f build/docker-compose.yml config` e confirmar ausência da publicação.

**Green phase:**

- [ ] Implementar o workload com imports PySpark dentro de `main`, dois jobs, um shuffle, uma consulta `spark.sql(...)` e atraso configurável dentro do trabalho distribuído.
- [ ] Fazer `observer-runtime-refresh` remover os containers Spark antigos e, antes de recriá-los, verificar que `127.0.0.1:24040` pode ser publicada; o harness não repetirá esse preflight enquanto o container persistente possuir o mapping.
- [ ] Implementar harness com `trap` para TERM/INT/EXIT, timeout explícito, propagação do exit code e identificação do PID real do driver dentro de `spark-master` pelo `OBSERVER_RUN_ID`.
- [ ] Publicar a porta apenas em `127.0.0.1`.
- [ ] Forçar `spark.ui.port=4040` e `spark.port.maxRetries=0`.
- [ ] Executar `make observer-runtime-refresh` para aplicar a nova configuração do Compose e verificar `docker compose ... port spark-master 4040`.
- [ ] Executar o workload com `OBSERVER_ENABLED=false` e `true`.
- [ ] Durante ambas as execuções, consultar a raiz da Spark UI e provar HTTP `200` com o processo vivo.
- [ ] Confirmar que `/dataship/` ainda retorna `404`.
- [ ] Depois de cada run, confirmar que o endpoint 24040 deixa de responder, que não há processo com o `OBSERVER_RUN_ID` dentro de `spark-master` e que o mapping Docker continua publicado.
- [ ] Executar uma segunda vez reutilizando o mesmo container/mapping e obter exit code `0`.

**Acceptance criteria:**

- O processo está vivo durante pelo menos duas leituras.
- O workload termina sozinho com exit code `0` nos dois modos.
- Há dois jobs, shuffle, múltiplos stages e uma execução SQL.
- Falha induzida no harness não deixa processo do probe nem endpoint respondendo; o mapping do container permanece, como esperado.
- Master UI em `28081` e driver UI em `24040` são demonstradas como interfaces distintas.
- Uma segunda execução reutiliza `24040:4040` com sucesso.

**User gate:** apresentar timeline do PID, respostas HTTP e cleanup; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add .env.example Makefile build/docker-compose.yml build/scripts/run-observer-live-probe.sh src/apps/observer_live_probe.py tests/test_observer_live_probe.py docs/spark-observer/execution-log.md
git commit -m "test: add deterministic live observer workload"
```

---

## Task 5: implementar lifecycle e endpoint de health

**Hipótese:** `registerMetrics` anexa `/health` depois que `appId` existe, e a resposta fica `READY` enquanto o submit está vivo.

**Files:**

- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/HealthResponse.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/JsonRenderer.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/HealthServlet.scala`
- Create: `spark-observer/src/main/scala/org/apache/spark/dataship/v412/Spark412Bridge.scala`
- Modify: `spark-observer/src/test/scala/io/dataship/spark/observer/ObserverConfigSpec.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/api/HealthResponseSpec.scala`
- Create: `build/scripts/assert-observer-response.py`
- Create: `tests/test_observer_response_assertions.py`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipDriverPlugin.scala`
- Modify: `build/scripts/run-observer-live-probe.sh`

**Interfaces:**

- `init(sc, context)` valida configuração, guarda referências e não instala HTTP.
- `registerMetrics(appId, context)` instala o handler no máximo uma vez quando a Spark UI suportada existe.
- `shutdown()` muda para `STOPPING` e fecha recursos idempotentemente.
- JSON health contém o envelope e `status`, `uiAttached`, `listenerInstalled`, `supportedRuntime`, `queueCapacity`, `lastErrorCode`.
- Sem Spark UI, o plugin registra o código estável `NO_SPARK_UI`, não instala handlers e deixa o job terminar; não há endpoint HTTP a consultar.

**Red phase:**

- [ ] Criar testes de config, envelope, estados e serialização sem campos extras.
- [ ] Criar testes do installer chamado duas vezes e do caminho `context.ui.isEmpty`.
- [ ] Criar testes Python que rejeitam status, content type, JSON ou campos incorretos.
- [ ] Executar `make observer-tests` e os dois testes Python; confirmar falhas esperadas.
- [ ] Executar o harness esperando `/health`; confirmar `404`.

**Green phase:**

- [ ] Implementar config imutável e DTOs próprios.
- [ ] Usar Jackson fornecido pelo Spark para serializar somente maps/lists/primitivos allowlisted.
- [ ] Implementar o adapter v4.1.2 sob `org.apache.spark.dataship.v412`.
- [ ] Anexar o servlet à mesma Spark UI, sem iniciar servidor adicional.
- [ ] Tornar a instalação idempotente e tratar Spark UI ausente como degradação isolada.
- [ ] Implementar espera com timeout no harness e validação via Python, sem `jq`.
- [ ] Executar `make observer-runtime-refresh` e confirmar por checksum que o container recebeu o JAR desta task antes do teste live.
- [ ] Executar teste live e coletar duas respostas health com o submit vivo.
- [ ] Após shutdown, provar endpoint indisponível, ausência do processo identificado dentro do container e sucesso de uma segunda execução no mesmo mapping.
- [ ] Executar o workload com `spark.ui.enabled=false`; exigir exit code `0` e `NO_SPARK_UI` no log redigido, sem prometer `/health`.

**Acceptance criteria:**

- `/health` responde `200` e `application/json`.
- `schemaVersion=v1`, `pluginVersion=0.1.0-SNAPSHOT`, `sparkVersion=4.1.2`, `mode=live`.
- `appId` não é vazio, `status=READY`, `uiAttached=true`, `supportedRuntime=true`.
- `capturedAt` é ISO-8601 UTC e não é comparado como literal.
- `init` não faz I/O nem scan de store.
- Duas chamadas de instalação não duplicam handler nem recurso.
- Com `spark.ui.enabled=false`, o job conclui e nenhum handler é instalado.
- Com `spark.plugins` presente e `enabled=false`, apenas `/health` responde `DISABLED`; listener, snapshot e aba não são instalados.
- Duas execuções consecutivas não encontram thread ou processo preso e reutilizam o mapping persistente.

**User gate:** apresentar o JSON real allowlisted, processo interno vivo e segunda execução; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/assert-observer-response.py build/scripts/run-observer-live-probe.sh tests/test_observer_response_assertions.py docs/spark-observer/execution-log.md
git commit -m "feat: expose observer lifecycle health"
```

---

## Task 6: adicionar fila interna limitada e estado consistente

**Hipótese:** o handoff nunca bloqueia, contabiliza recusa deterministicamente e preserva o invariante sob concorrência e shutdown.

**Files:**

- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverEvent.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverCounters.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverState.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/events/BoundedEventQueue.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/events/BoundedEventQueueSpec.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/events/ObserverStateSpec.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala`

**Interfaces:**

- `offer(event: ObserverEvent): Boolean` nunca espera por espaço.
- Um único worker daemon executa `queued -> inFlight -> processed`.
- `close()` é idempotente e tem timeout fixo de cinco segundos.
- Snapshot atômico respeita `listenerReceived = processed + queued + inFlight + droppedByPlugin`.
- A janela recente usa eviction FIFO e capacidade fixa.

**Red phase:**

- [ ] Criar teste com capacidade `1`, worker bloqueado por latch e três offers.
- [ ] Criar teste concorrente do invariante, eviction e dois `close()`.
- [ ] Executar somente os dois specs; confirmar falha de compilação.

**Green phase:**

- [ ] Implementar `ArrayBlockingQueue`, `offer` e worker daemon único.
- [ ] Atualizar contadores dentro de uma seção crítica única para snapshots coerentes.
- [ ] Interromper e aguardar o worker no shutdown sem chamar funções Spark.
- [ ] Executar os specs repetidamente cinquenta vezes para detectar flakiness.
- [ ] Executar `make observer-tests` e `make tests`.
- [ ] Executar `make observer-runtime-refresh` antes da regressão live e confirmar que `/health` da Task 5 continua funcionando com o JAR atual.

**Acceptance criteria:**

- Teste cheio gera exatamente os descartes esperados sem bloquear.
- Invariante permanece verdadeiro em todos os snapshots de teste.
- Janela nunca excede a capacidade.
- Shutdown repetido não lança exceção nem deixa thread viva.
- Nenhum código desta task importa Spark internals.

**User gate:** apresentar tempos, contadores e repetição sem flakiness; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer/src/main/scala/io/dataship/spark/observer/events spark-observer/src/test/scala/io/dataship/spark/observer/events spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala docs/spark-observer/execution-log.md
git commit -m "feat: add bounded observer event handoff"
```

---

## Task 7: instalar listener dedicado e endpoint de contadores

**Hipótese:** eventos reais entram pela fila `dataship-observer`, os contadores crescem entre `t1` e `t2`, e backpressure do observador não altera o resultado do job.

**Files:**

- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverListener.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/CountersResponse.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/CountersServlet.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/events/ObserverListenerSpec.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/api/CountersResponseSpec.scala`
- Modify: `spark-observer/src/main/scala/org/apache/spark/dataship/v412/Spark412Bridge.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/api/HealthResponse.scala`
- Modify: `build/scripts/run-observer-live-probe.sh`

**Interfaces:**

- Categorias: `application`, `job`, `stage`, `task`, `sql`, `executor`, `other`.
- `onOtherEvent` classifica eventos SQL sem modificar planos.
- `/debug/counters` inclui recebidos por categoria, processed, queued, inFlight, droppedByPlugin, internalFailures, depth, capacity, lastEventAt e invariantHolds.

**Red phase:**

- [ ] Criar specs de classificação e JSON.
- [ ] Acrescentar ao harness duas leituras `t1`/`t2` exigindo crescimento.
- [ ] Executar specs e harness; confirmar classes ausentes e `404`.

**Green phase:**

- [ ] Implementar callback O(1): classificar, contar e chamar `offer`.
- [ ] Instalar o listener com `sc.listenerBus.addToQueue(listener, "dataship-observer")` somente no adapter v4.1.2.
- [ ] Instalar endpoint de contadores e atualizar health para `listenerInstalled=true`.
- [ ] Executar `make observer-runtime-refresh` e validar o checksum antes dos cenários live.
- [ ] Executar live normal; exigir `listenerReceived(t2) > listenerReceived(t1)`.
- [ ] Executar live com capacidade `1`, `testMode=true` e delay controlado; exigir `droppedByPlugin > 0`.
- [ ] Confirmar resultado e exit code idênticos ao run sem delay.

**Acceptance criteria:**

- Listener usa a fila nomeada correta.
- Contadores crescem enquanto o processo está vivo.
- `invariantHolds=true` nas duas leituras.
- Descarte do plugin é separado de qualquer descarte do Spark.
- Drop induzido não falha nem muda o resultado do workload.
- Callback não faz HTTP, snapshot, I/O ou retenção ilimitada.

**User gate:** apresentar `t1`, `t2`, cenário de drop e resultados; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/run-observer-live-probe.sh docs/spark-observer/execution-log.md
git commit -m "feat: expose live listener counters"
```

---

## Task 8: expor snapshot live de aplicação, jobs e stages

**Hipótese:** o endpoint lê os stores nativos e observa uma transição real sem manter uma segunda cópia de jobs, stages ou tasks.

**Files:**

- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/SnapshotModels.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/SparkSnapshotSource.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/LiveSnapshotService.scala`
- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/SnapshotServlet.scala`
- Create: `spark-observer/src/main/scala/org/apache/spark/dataship/v412/Spark412SnapshotSource.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/api/LiveSnapshotServiceSpec.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/api/SnapshotServletSpec.scala`
- Modify: `spark-observer/src/main/scala/org/apache/spark/dataship/v412/Spark412Bridge.scala`
- Modify: `build/scripts/run-observer-live-probe.sh`

**Interfaces:**

- `ApplicationView`, `JobView`, `StageView`, `SqlExecutionView` são DTOs próprios.
- Tasks aparecem somente como agregados dentro de `StageView`.
- Parâmetro `limit` aceita `1..200`; ausência usa config `50`.
- Resposta informa `truncated=true|false`.
- Jobs e stages são lidos diretamente do `KVStore` com `.max(limit + 1)`; `AppStatusStore.jobsList` e `stageList` não podem ser usados porque materializam toda a retenção antes do corte.

**Red phase:**

- [ ] Criar fake `SparkSnapshotSource` com job/stage ativo e completo.
- [ ] Testar limit, ordenação recente, agregados e resposta `400` fora da faixa.
- [ ] Executar specs e confirmar falha de compilação.
- [ ] Acrescentar ao harness polling de `/snapshot`; confirmar `404`.

**Green phase:**

- [ ] Implementar service contra a interface fake antes do adapter real.
- [ ] Implementar adapter read-only sobre `AppStatusStore` dentro do namespace v4.1.2.
- [ ] Implementar as consultas de jobs/stages no `KVStore` com limite real `limit + 1`, sem `jobsList`, `stageList`, `taskList` ilimitado ou map de tasks.
- [ ] Instalar servlet com `200`, `400`, `409` e `500` isolado.
- [ ] Executar `make observer-runtime-refresh` e validar o checksum antes do polling live.
- [ ] Executar live e capturar um job/stage ativo em `t1` e finalizado em `t2`.
- [ ] Executar com `limit=1` e provar truncamento.

**Acceptance criteria:**

- Uma transição `RUNNING -> SUCCEEDED` é vista com submit vivo.
- Contagens de tasks por stage são coerentes e não negativas.
- Coleções nunca excedem o limite solicitado.
- O adapter busca no máximo `limit + 1` registros para decidir `truncated`; o custo residual é limitado também pelas retenções configuradas da Spark UI.
- Nenhum objeto Spark é serializado diretamente.
- Falta temporária da store retorna `409`, não falha o driver.

**User gate:** apresentar snapshots `t1`/`t2`, limite e transição; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/run-observer-live-probe.sh docs/spark-observer/execution-log.md
git commit -m "feat: expose live job and stage snapshots"
```

---

## Task 9: acrescentar snapshot de execução SQL com descrição opt-in e sem fonte inventada

**Hipótese:** metadados de uma execução SQL aparecem live e terminam; o v0 não afirma capturar o texto passado a `spark.sql(...)`, e qualquer descrição fornecida pelo status store permanece oculta por default ou é redigida quando explicitamente habilitada.

**Files:**

- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/TextRedactor.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/api/TextRedactorSpec.scala`
- Create: `spark-observer/src/test/scala/org/apache/spark/dataship/v412/Spark412SqlSnapshotSpec.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/api/SnapshotModels.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/api/LiveSnapshotService.scala`
- Modify: `spark-observer/src/main/scala/org/apache/spark/dataship/v412/Spark412SnapshotSource.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala`
- Modify: `spark-observer/src/test/scala/io/dataship/spark/observer/ObserverConfigSpec.scala`
- Modify: `build/scripts/run-observer-live-probe.sh`
- Modify: `src/apps/observer_live_probe.py`

**Interfaces:**

- `SqlExecutionView` contém somente id, status, `description`, start/end, `descriptionAvailable` e `descriptionSource`.
- `descriptionSource` poderá ser apenas `SPARK_STATUS_STORE` ou `NONE`; ele nunca será rotulado como `SQL_TEXT`.
- Com `spark.dataship.observer.sql.description.enabled=false`, `description=null`, `descriptionAvailable=false` e `descriptionSource=NONE`.
- Quando habilitada, a descrição passa por `spark.redaction.string.regex` quando configurada e por uma política conservadora do plugin para assignments sensíveis, bearer tokens e access keys; qualquer correspondência duvidosa pode reduzir a descrição inteira a `[REDACTED]`.
- Redação ocorre antes do truncamento em `spark.dataship.observer.sql.maxLength`.
- Ausência de descrição confiável não usa fonte Python, stack trace, plano ou o argumento de `spark.sql(...)` como substituto.

**Red phase:**

- [ ] Criar specs provando descrição desabilitada por default, `spark.redaction.string.regex`, senha, valor de access key, valor de bearer token, string longa e descrição ausente.
- [ ] Criar teste do adapter com eventos SQL start/end.
- [ ] Acrescentar ao harness exigência de execução SQL ativa/final.
- [ ] Executar e confirmar falhas esperadas.

**Green phase:**

- [ ] Implementar allowlist, opt-in e redação antes do truncamento.
- [ ] Localizar a store/listener SQL somente no adapter v4.1.2.
- [ ] Mapear estado sem instrumentar `SparkPlan`, extensions ou AQE.
- [ ] Executar `make observer-runtime-refresh` e validar o checksum antes dos cenários live.
- [ ] Executar live com `spark.sql(...)`; capturar a execução ativa e final sem exigir o SQL literal.
- [ ] Executar o default e provar `descriptionAvailable=false`.
- [ ] Executar um fixture sintético que define `spark.job.description`, habilita a descrição e configura redação de string; documentar explicitamente que isso testa redação, não captura universal do SQL.
- [ ] Executar caminho DataFrame API; provar que nenhum código-fonte é reconstruído.
- [ ] Examinar JSON e confirmar ausência dos valores sensíveis de teste.

**Acceptance criteria:**

- Uma execução iniciada por `spark.sql(...)` aparece por id/status, sem promessa de SQL literal.
- Descrição fica ausente por default; no fixture sintético opt-in, aparece redigida, truncada e rotulada `SPARK_STATUS_STORE`.
- Uma transição SQL live é observada.
- DataFrame API não recebe código-fonte reconstruído.
- Nenhum plano é envolvido, reescrito ou modificado.
- Respostas não contêm segredo conhecido do fixture.

**User gate:** apresentar metadados/transição, descrição sintética redigida e caso DataFrame; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer src/apps/observer_live_probe.py build/scripts/run-observer-live-probe.sh docs/spark-observer/execution-log.md
git commit -m "feat: add safe live SQL execution snapshots"
```

---

## Task 10: anexar aba DataShip e assets mínimos à Spark UI

**Hipótese:** a UI nativa mostra uma aba DataShip servida pelo próprio driver, usa somente os endpoints v1 e interrompe polling em estado terminal.

**Files:**

- Create: `spark-observer/src/main/scala/org/apache/spark/dataship/v412/DataShipUITab.scala`
- Create: `spark-observer/src/main/scala/org/apache/spark/dataship/v412/DataShipUIPage.scala`
- Create: `spark-observer/src/main/scala/org/apache/spark/dataship/v412/StaticResourceHandler.scala`
- Create: `spark-observer/src/main/resources/io/dataship/spark/observer/ui/index.html`
- Create: `spark-observer/src/main/resources/io/dataship/spark/observer/ui/app.css`
- Create: `spark-observer/src/main/resources/io/dataship/spark/observer/ui/app.js`
- Create: `spark-observer/src/test/scala/org/apache/spark/dataship/v412/DataShipUITabSpec.scala`
- Create: `spark-observer/src/test/js/polling.test.mjs`
- Modify: `spark-observer/src/main/scala/org/apache/spark/dataship/v412/Spark412Bridge.scala`
- Modify: `Makefile`
- Modify: `build/scripts/run-observer-live-probe.sh`

**Interfaces:**

- Tab name: `DataShip`; prefix: `dataship`.
- Polling de resumo: `1000 ms`.
- A página mostra health, versões, appId, jobs/stages/SQL, fila, received/processed/dropped e timestamp.
- JavaScript exporta `startPolling`, `stopPolling` e `isTerminal`.

**Red phase:**

- [ ] Criar spec que exige tab, page e recursos no classpath.
- [ ] Criar teste `node:test` com relógio/fetch fake que exige intervalo de um segundo e parada terminal.
- [ ] Executar `make observer-tests` e `make observer-ui-tests`; confirmar falhas de comportamento por tab/assets/exports ausentes, nunca por Node ou target ausente.
- [ ] Criar spec que chama o installer duas vezes e exige apenas uma tab e um conjunto de handlers.
- [ ] Acrescentar ao harness `GET /dataship/` e assets; confirmar `404`.

**Green phase:**

- [ ] Implementar página pequena com HTML sem framework e identidade visual própria.
- [ ] Empacotar assets no JAR e anexar handler estático pelo adapter v4.1.2.
- [ ] Fazer polling apenas de health, counters e snapshot limitado.
- [ ] Parar polling em `DISABLED`, `STOPPING`, app final, unload ou abort.
- [ ] Executar `make observer-tests`, `make observer-ui-tests` e `make observer-runtime-refresh`; validar o checksum antes do live.
- [ ] Executar o cenário live e o cenário `spark.ui.enabled=false` já definido na Task 5.
- [ ] Inspecionar o JAR e confirmar os três assets.

**Acceptance criteria:**

- Spark UI contém link `DataShip`.
- Página e assets retornam `200` e content types corretos.
- Dados visíveis vêm dos endpoints versionados.
- Polling usa um segundo e para nos estados definidos.
- Falha de fetch exibe estado degradado local, sem afetar o job.
- Não há nome, logo, asset ou código visual do DataFlint.
- Instalação repetida não duplica aba ou handlers; sem Spark UI, o job continua sem aba.

**User gate:** apresentar HTML/requests, teste de polling e job concluído; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add Makefile spark-observer build/scripts/run-observer-live-probe.sh docs/spark-observer/execution-log.md
git commit -m "feat: add minimal DataShip Spark UI tab"
```

---

## Task 11: provar fail-open, limites e segurança

**Hipótese:** configuração inválida, runtime não suportado, snapshot quebrado, fila cheia e polling acelerado degradam somente o observador, sem introduzir um limitador HTTP não justificado no v0.

**Files:**

- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/FailOpenSpec.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipDriverPlugin.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/api/HealthServlet.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/api/CountersServlet.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/api/SnapshotServlet.scala`
- Modify: `build/scripts/assert-observer-response.py`
- Modify: `build/scripts/run-observer-live-probe.sh`

**Interfaces:**

- Erros públicos são códigos estáveis; stack traces permanecem apenas no log redigido.
- Erro isolado do snapshot recebe `500`, health continua disponível e o job continua.
- Config inválida em Spark 4.1.2 recebe `DEGRADED` com `INVALID_CONFIG`.
- Versão não suportada impede a factory do adapter `v412`, registra `UNSUPPORTED_SPARK_VERSION` e não promete rota HTTP.
- Não existe resposta `429` no contrato v0; proteção HTTP própria só entra em outro design se medições demonstrarem necessidade.

**Red phase:**

- [ ] Criar testes unitários para cada fronteira de falha e para a guarda que prova que a factory `v412` não é chamada em versão diferente.
- [ ] Criar integração com polling acelerado por intervalo fixo, sem expectativa de `429`.
- [ ] Criar fixtures com credenciais sentinela para MinIO/ClickHouse sem imprimir valores.
- [ ] Executar testes e confirmar que os comportamentos ainda não existem.

**Green phase:**

- [ ] Implementar fail-open por fronteira: config, listener, snapshot, servlet e UI.
- [ ] Garantir que qualquer erro atualiza `internalFailures` e `lastErrorCode`.
- [ ] Executar `make observer-runtime-refresh` e validar o checksum antes dos cenários live.
- [ ] Executar cenário de fila cheia, snapshot falho e polling acelerado.
- [ ] Executar scan automatizado das respostas contra valores sentinela.
- [ ] Confirmar que cada workload termina com mesmo resultado e exit code `0`.

**Acceptance criteria:**

- Nenhum cenário controlado derruba o driver.
- Polling acelerado não derruba o driver e não adiciona código/contrato `429` ao v0.
- Após um `500` de snapshot, `/health` ainda responde.
- Config inválida no runtime suportado é visível em `/health`; versão não suportada não instancia o adapter e é comprovada por código/log estável, não por endpoint.
- Respostas não expõem credenciais, ambiente, stack trace ou SparkConf completo.
- Fila cheia continua não bloqueante.

**User gate:** apresentar matriz falha → HTTP/status → resultado do job; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/assert-observer-response.py build/scripts/run-observer-live-probe.sh docs/spark-observer/execution-log.md
git commit -m "feat: make Spark Observer fail open"
```

---

## Task 12: consolidar o E2E live e executar regressão do lado direito

**Hipótese:** uma única execução automatizada prova todos os critérios live e, depois, os gates existentes confirmam que o caminho durável permaneceu intacto.

**Files:**

- Create: `build/scripts/validate-observer-e2e.sh`
- Modify: `Makefile`
- Modify: `docs/dev-design/operations.md`
- Modify: `docs/dev-design/compatibility.md`
- Modify: `docs/spark-observer/execution-log.md`
- No changes under `build/clickhouse/` or `build/images/eventlog-loader/`.

**Interfaces:**

- Make target `observer-smoke`.
- O script produz um resumo final `PASS`/`FAIL` e preserva artefatos de falha sem segredos.

**Harness preparation:**

- [ ] Criar o target `observer-smoke` e o entrypoint executável antes do teste de comportamento; falha por target/script ausente não conta como vermelho.

**Red phase:**

- [ ] Escrever o gate exigindo os 12 critérios do design.
- [ ] Executar `make observer-smoke`.
- [ ] Confirmar exit code diferente de zero no primeiro critério comportamental ainda não orquestrado, com mensagem `FAIL` específica; erro de Make, permissão ou ferramenta não é um vermelho válido.

**Green phase:**

- [ ] Iniciar com `make observer-runtime-refresh`, validar checksum e então orquestrar compose, submit live, health, counters `t1/t2`, snapshots, transição, UI, backpressure, fail-open, plugin off e cleanup.
- [ ] Confirmar automaticamente processo vivo em cada prova HTTP.
- [ ] Executar `make validate`, `make tests`, `make observer-tests`, `make smoke` e `make spark-logs`.
- [ ] Confirmar event logs em `spark-logs/events`, aplicação no History Server e carga existente no ClickHouse.
- [ ] Comparar os caminhos congelados com `89202730dfd19d35d52c35d61b739dad4fcca345`; exigir diff vazio.
- [ ] Validar semanticamente no Compose/config final: bucket `spark-logs`, prefixo `events/`, event log habilitado e caminhos `s3a://spark-logs/events` de Spark e History.
- [ ] Documentar comandos operacionais e matriz exata suportada.

**Acceptance criteria:**

- O teste automatizado demonstra, na mesma execução, os 12 itens da seção 14 do design.
- Plugin ligado e desligado produzem o mesmo resultado funcional.
- Event log, History, loader e ClickHouse continuam nos caminhos atuais.
- Nenhum schema ClickHouse, loader Go, bucket ou prefixo muda.
- Todos os comandos retornam exit code `0`.
- Antes do commit, o status contém somente os arquivos da Task 12 e `execution-log.md`; depois do commit, a árvore fica limpa.

**User gate:** apresentar relatório final completo e parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add Makefile build/scripts/validate-observer-e2e.sh docs/dev-design/operations.md docs/dev-design/compatibility.md docs/spark-observer/execution-log.md
git commit -m "test: validate Spark Observer end to end"
```

---

## Definition of Done

O v0 live só está completo depois do aceite e commit da Task 12. Até lá, cada commit representa uma fatia comprovada, mas não autoriza afirmar que o Observer inteiro está pronto.

O histórico esperado será:

1. baseline registrada;
2. toolchain JVM conteinerizado;
3. plugin mínimo carregável;
4. workload live determinístico;
5. lifecycle e health;
6. fila interna limitada;
7. listener e contadores;
8. snapshots de jobs/stages;
9. metadados SQL e descrição opt-in redigida;
10. aba DataShip;
11. fail-open e segurança;
12. E2E e regressão durável.

Qualquer mudança de arquitetura descoberta durante a execução deve gerar uma parada e uma atualização aprovada deste plano antes de modificar código fora da task ativa.
