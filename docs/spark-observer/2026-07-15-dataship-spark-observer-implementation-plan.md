# DataShip Spark Observer v0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:test-driven-development` and `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do not use subagents unless the user explicitly authorizes them.

**Goal:** construir e provar um plugin JVM opt-in no driver Spark 4.1.2 que exponha estado live, endpoints versionados e uma aba mínima na Spark UI sem interferir no job nem alterar o caminho durável existente.

**Architecture:** o plugin terá uma entrada pública pequena em `io.dataship.spark.observer` e todo acesso a APIs internas do Spark ficará isolado em `org.apache.spark.dataship.v412`. Jobs, stages e SQL serão lidos dos stores nativos; apenas contadores e uma janela limitada de transições pertencerão ao plugin. Build e testes JVM usarão um container sbt/Java 17, enquanto o JAR será executado exclusivamente pelo driver no container Spark.

**Tech Stack:** Spark `4.1.2`, Scala `2.13.16`/binary `2.13`, Java `17`, sbt `1.10.11`, ScalaTest `3.2.19`, PySpark, Bash, Python `3.10+`, Node `24` apenas para testes do JavaScript estático, Docker Compose.

**Design aprovado:** `docs/spark-observer/2026-07-15-dataship-spark-observer-live-driver-design.md`

## Global Constraints

- O host não deve receber Java, Scala ou sbt; ele precisa apenas das ferramentas já usadas pelo repositório, especialmente Docker e Make.
- A imagem de build JVM será `sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.10.11_2.13.16`.
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
- Nenhuma resposta poderá expor `SparkConf` completo, ambiente, credenciais, fonte do usuário ou SQL sem redação e truncamento.
- Nenhuma task poderá alterar código de tasks posteriores para “adiantar” trabalho.

---

## Frame obrigatório de execução

Cada task é uma unidade de revisão e um commit. O executor deve seguir esta sequência:

1. Confirmar branch, baseline, árvore limpa e arquivos da task.
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
10. Depois do aceite, executar somente o checkpoint de commit da task e mostrar hash/status.
11. Parar novamente. A próxima task exige um novo pedido explícito.

Se qualquer gate falhar, não commitar e não iniciar outra task. Aplicar `superpowers:systematic-debugging`, registrar `FAIL` ou `BLOCKED` e pedir direção quando necessário.

Antes de cada commit, este comando deve continuar sem saída:

```bash
git diff --name-only main -- build/clickhouse build/images/eventlog-loader
```

Uma saída não vazia bloqueia o commit e exige decisão de arquitetura do usuário.

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
| `spark.dataship.observer.http.maxConcurrentRequests` | `4` | inteiro entre `1` e `32` |
| `spark.dataship.observer.testMode` | `false` | permite controles determinísticos somente nos testes |
| `spark.dataship.observer.test.processingDelayMs` | `0` | deve ser `0` quando `testMode=false`; máximo `1000` |

O runtime suportado é exatamente Spark `4.1.2`. Uma versão diferente produz estado `DEGRADED`, código `UNSUPPORTED_SPARK_VERSION` e nenhuma instalação de listener, snapshot ou aba.

### Estado e rotas

- Estados: `STARTING`, `READY`, `DEGRADED`, `DISABLED`, `STOPPING`.
- Fila do listener bus: `dataship-observer`.
- `GET /dataship/api/v1/health`
- `GET /dataship/api/v1/debug/counters`
- `GET /dataship/api/v1/snapshot?limit=N`
- `GET /dataship/`
- Assets: `/dataship/static/app.css` e `/dataship/static/app.js`.

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
- `spark-observer/src/test/js/`: testes `node:test` do polling.

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

**Hipótese:** a plataforma existente passa seus gates sem o plugin, a porta `24040` não está publicada e o lado direito funciona antes de qualquer feature.

**Files:**

- Create: `docs/spark-observer/execution-log.md`
- No production code changes.

**Test first:**

- [ ] Registrar branch, `HEAD`, status e nomes dos serviços.
- [ ] Executar `make validate` e `make tests`.
- [ ] Executar `make compose` e `make smoke`.
- [ ] Executar `curl --fail --silent --show-error http://127.0.0.1:24040/dataship/api/v1/health`.
- [ ] Confirmar que o `curl` falha com conexão recusada porque a porta do driver ainda não existe.
- [ ] Executar `make spark-logs` e registrar apenas contagens, nunca credenciais.

**Acceptance criteria:**

- `make validate`, `make tests`, `make smoke` e `make spark-logs` retornam exit code `0`.
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

**Hipótese:** o módulo Scala compila e testa sem Java, Scala ou sbt instalados no host.

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

**Interfaces:**

- `BuildInfo.PluginVersion = "0.1.0-SNAPSHOT"`
- `BuildInfo.SupportedSparkVersion = "4.1.2"`
- `BuildInfo.ScalaBinaryVersion = "2.13"`
- Make targets: `observer-tests` and `observer-jar`.
- `SBT_IMAGE=sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.10.11_2.13.16`.

**Red phase:**

- [ ] Criar `BuildInfoSpec` verificando as três constantes e o nome do artefato.
- [ ] Executar `make observer-tests`.
- [ ] Confirmar falha `No rule to make target 'observer-tests'`.

**Green phase:**

- [ ] Criar o projeto sbt com Scala `2.13.16`, Spark Core/SQL `4.1.2` como `provided` e ScalaTest `3.2.19` em `Test`.
- [ ] Fazer `bootstrap.sh` puxar a imagem sbt e aquecer caches em `build/cache/sbt` e `build/cache/coursier`.
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

- `command -v java`, `command -v scalac` e `command -v sbt` podem continuar falhando no host.
- Build e testes usam somente Docker.
- Spark/Scala não são empacotados.
- `make validate` e `make tests` continuam verdes.

**User gate:** mostrar comandos, imagem usada, conteúdo relevante do JAR e parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add .env.example .gitignore Makefile build/scripts/bootstrap.sh build/scripts/validate-bootstrap.sh spark-observer
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

**Red phase:**

- [ ] Criar testes que instanciam `SparkDataShipPlugin`, verificam o tipo do driver, `executorPlugin() == null` e os estados enabled/disabled.
- [ ] Executar `make observer-tests`.
- [ ] Confirmar falha de compilação porque as classes ainda não existem.

**Green phase:**

- [ ] Implementar apenas as classes de bootstrap e o parsing da flag enabled; `init` retorna mapa vazio e não faz I/O.
- [ ] Integrar o JAR produzido ao contexto da imagem Spark sem colocá-lo no bootstrap manifest de dependências externas.
- [ ] Executar `make observer-tests`, `make observer-jar` e `make build`.
- [ ] Executar `make compose` e `make smoke` sem flags do plugin.
- [ ] Executar `check_sanity.py` com `spark.plugins` e `spark.dataship.observer.enabled=true`.
- [ ] Confirmar exit code `0` nos dois modos e nenhuma rota `/dataship/`.

**Acceptance criteria:**

- O JAR está em `/opt/spark/jars/`.
- Estar no classpath não ativa o plugin.
- O plugin habilitado é carregado no driver e não nos executors.
- Nenhuma thread, listener, endpoint ou aba customizada existe ainda.
- O resultado do workload é idêntico com plugin ligado e desligado.

**User gate:** mostrar testes unitários, duas execuções Spark e parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add Makefile build/scripts/prepare-image-contexts.sh spark-observer
git commit -m "feat: add minimal driver-only Spark plugin"
```

---

## Task 4: criar workload e harness determinísticos para prova live

**Hipótese:** um `spark-submit` controlado permanece vivo tempo suficiente para duas leituras, executa dois jobs, um shuffle e SQL, e sempre limpa processo e porta.

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
- O harness aceita `OBSERVER_ENABLED=true|false` e grava apenas artefatos temporários em `/tmp`.

**Red phase:**

- [ ] Criar testes puros para defaults, rejeição de valores negativos e resultado determinístico esperado.
- [ ] Executar `uv run pytest tests/test_observer_live_probe.py -q`.
- [ ] Confirmar falha porque o módulo não existe.
- [ ] Criar uma asserção de Compose que procura a porta 4040.
- [ ] Executar `docker compose --env-file .env -f build/docker-compose.yml config` e confirmar ausência da publicação.

**Green phase:**

- [ ] Implementar o workload com imports PySpark dentro de `main`, dois jobs, um shuffle, uma consulta `spark.sql(...)` e atraso configurável dentro do trabalho distribuído.
- [ ] Implementar harness com preflight da porta, `trap` para TERM/INT/EXIT, PID do submit, timeout explícito e propagação do exit code.
- [ ] Publicar a porta apenas em `127.0.0.1`.
- [ ] Forçar `spark.ui.port=4040` e `spark.port.maxRetries=0`.
- [ ] Executar o workload com `OBSERVER_ENABLED=false` e `true`.
- [ ] Durante ambas as execuções, consultar a raiz da Spark UI e provar HTTP `200` com o processo vivo.
- [ ] Confirmar que `/dataship/` ainda retorna `404`.

**Acceptance criteria:**

- O processo está vivo durante pelo menos duas leituras.
- O workload termina sozinho com exit code `0` nos dois modos.
- Há dois jobs, shuffle, múltiplos stages e uma execução SQL.
- Falha induzida no harness não deixa processo nem porta.
- Master UI em `28081` e driver UI em `24040` são demonstradas como interfaces distintas.

**User gate:** apresentar timeline do PID, respostas HTTP e cleanup; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add .env.example Makefile build/docker-compose.yml build/scripts/run-observer-live-probe.sh src/apps/observer_live_probe.py tests/test_observer_live_probe.py
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
- Modify: `SparkDataShipDriverPlugin.scala`
- Modify: `build/scripts/run-observer-live-probe.sh`

**Interfaces:**

- `init(sc, context)` valida configuração, guarda referências e não instala HTTP.
- `registerMetrics(appId, context)` instala o handler uma vez.
- `shutdown()` muda para `STOPPING` e fecha recursos idempotentemente.
- JSON health contém o envelope e `status`, `uiAttached`, `listenerInstalled`, `supportedRuntime`, `queueCapacity`, `lastErrorCode`.

**Red phase:**

- [ ] Criar testes de config, envelope, estados e serialização sem campos extras.
- [ ] Criar testes Python que rejeitam status, content type, JSON ou campos incorretos.
- [ ] Executar `make observer-tests` e os dois testes Python; confirmar falhas esperadas.
- [ ] Executar o harness esperando `/health`; confirmar `404`.

**Green phase:**

- [ ] Implementar config imutável e DTOs próprios.
- [ ] Usar Jackson fornecido pelo Spark para serializar somente maps/lists/primitivos allowlisted.
- [ ] Implementar o adapter v4.1.2 sob `org.apache.spark.dataship.v412`.
- [ ] Anexar o servlet à mesma Spark UI, sem iniciar servidor adicional.
- [ ] Implementar espera com timeout no harness e validação via Python, sem `jq`.
- [ ] Executar teste live e coletar duas respostas health com o submit vivo.
- [ ] Após shutdown, provar que a porta é liberada para uma segunda execução.

**Acceptance criteria:**

- `/health` responde `200` e `application/json`.
- `schemaVersion=v1`, `pluginVersion=0.1.0-SNAPSHOT`, `sparkVersion=4.1.2`, `mode=live`.
- `appId` não é vazio, `status=READY`, `uiAttached=true`, `supportedRuntime=true`.
- `capturedAt` é ISO-8601 UTC e não é comparado como literal.
- `init` não faz I/O nem scan de store.
- Com `spark.plugins` presente e `enabled=false`, apenas `/health` responde `DISABLED`; listener, snapshot e aba não são instalados.
- Duas execuções consecutivas não encontram thread ou porta presa.

**User gate:** apresentar o JSON real redigido, PID vivo e segunda execução; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/assert-observer-response.py build/scripts/run-observer-live-probe.sh tests/test_observer_response_assertions.py
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
- Modify: `ObserverConfig.scala`
- Modify: `ObserverRuntime.scala`

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

**Acceptance criteria:**

- Teste cheio gera exatamente os descartes esperados sem bloquear.
- Invariante permanece verdadeiro em todos os snapshots de teste.
- Janela nunca excede a capacidade.
- Shutdown repetido não lança exceção nem deixa thread viva.
- Nenhum código desta task importa Spark internals.

**User gate:** apresentar tempos, contadores e repetição sem flakiness; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer/src/main/scala/io/dataship/spark/observer/events spark-observer/src/test/scala/io/dataship/spark/observer/events spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala
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
- Modify: `Spark412Bridge.scala`
- Modify: `ObserverRuntime.scala`
- Modify: `HealthResponse.scala`
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
git add spark-observer build/scripts/run-observer-live-probe.sh
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
- Modify: `Spark412Bridge.scala`
- Modify: `build/scripts/run-observer-live-probe.sh`

**Interfaces:**

- `ApplicationView`, `JobView`, `StageView`, `SqlExecutionView` são DTOs próprios.
- Tasks aparecem somente como agregados dentro de `StageView`.
- Parâmetro `limit` aceita `1..200`; ausência usa config `50`.
- Resposta informa `truncated=true|false`.

**Red phase:**

- [ ] Criar fake `SparkSnapshotSource` com job/stage ativo e completo.
- [ ] Testar limit, ordenação recente, agregados e resposta `400` fora da faixa.
- [ ] Executar specs e confirmar falha de compilação.
- [ ] Acrescentar ao harness polling de `/snapshot`; confirmar `404`.

**Green phase:**

- [ ] Implementar service contra a interface fake antes do adapter real.
- [ ] Implementar adapter read-only sobre `AppStatusStore` dentro do namespace v4.1.2.
- [ ] Não chamar `taskList` sem limite nem materializar map de tasks.
- [ ] Instalar servlet com `200`, `400`, `409` e `500` isolado.
- [ ] Executar live e capturar um job/stage ativo em `t1` e finalizado em `t2`.
- [ ] Executar com `limit=1` e provar truncamento.

**Acceptance criteria:**

- Uma transição `RUNNING -> SUCCEEDED` é vista com submit vivo.
- Contagens de tasks por stage são coerentes e não negativas.
- Coleções nunca excedem o limite solicitado.
- Nenhum objeto Spark é serializado diretamente.
- Falta temporária da store retorna `409`, não falha o driver.

**User gate:** apresentar snapshots `t1`/`t2`, limite e transição; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/run-observer-live-probe.sh
git commit -m "feat: expose live job and stage snapshots"
```

---

## Task 9: acrescentar snapshot SQL com redação e sem fonte inventada

**Hipótese:** SQL real aparece live e termina, enquanto DataFrame API sem texto SQL permanece descrita apenas pelos dados que o Spark fornece.

**Files:**

- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/TextRedactor.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/api/TextRedactorSpec.scala`
- Create: `spark-observer/src/test/scala/org/apache/spark/dataship/v412/Spark412SqlSnapshotSpec.scala`
- Modify: `SnapshotModels.scala`
- Modify: `LiveSnapshotService.scala`
- Modify: `Spark412SnapshotSource.scala`
- Modify: `build/scripts/run-observer-live-probe.sh`
- Modify: `src/apps/observer_live_probe.py`

**Interfaces:**

- `SqlExecutionView` contém somente id, status, description, start/end e indicador `textAvailable`.
- Redação usa as regexes de redação do Spark e depois aplica truncamento em `spark.dataship.observer.sql.maxLength`.
- Ausência de SQL textual produz `textAvailable=false`; não usa fonte Python, stack trace ou plano como substituto.

**Red phase:**

- [ ] Criar specs com senha, access key, bearer token, string longa e texto ausente.
- [ ] Criar teste do adapter com eventos SQL start/end.
- [ ] Acrescentar ao harness exigência de execução SQL ativa/final.
- [ ] Executar e confirmar falhas esperadas.

**Green phase:**

- [ ] Implementar redação antes do truncamento.
- [ ] Localizar a store/listener SQL somente no adapter v4.1.2.
- [ ] Mapear estado sem instrumentar `SparkPlan`, extensions ou AQE.
- [ ] Executar live com `spark.sql(...)`; capturar ativo e final.
- [ ] Executar caminho DataFrame API; provar `textAvailable=false` quando texto original não existe.
- [ ] Examinar JSON e confirmar ausência dos valores sensíveis de teste.

**Acceptance criteria:**

- SQL literal aparece redigido e limitado.
- Uma transição SQL live é observada.
- DataFrame API não recebe código-fonte reconstruído.
- Nenhum plano é envolvido, reescrito ou modificado.
- Respostas não contêm segredo conhecido do fixture.

**User gate:** apresentar SQL redigido, transição e caso DataFrame; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer src/apps/observer_live_probe.py build/scripts/run-observer-live-probe.sh
git commit -m "feat: add redacted live SQL snapshots"
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
- Modify: `Spark412Bridge.scala`
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
- [ ] Executar `make observer-tests` e `node --test spark-observer/src/test/js/polling.test.mjs`; confirmar falhas.
- [ ] Acrescentar ao harness `GET /dataship/` e assets; confirmar `404`.

**Green phase:**

- [ ] Implementar página pequena com HTML sem framework e identidade visual própria.
- [ ] Empacotar assets no JAR e anexar handler estático pelo adapter v4.1.2.
- [ ] Fazer polling apenas de health, counters e snapshot limitado.
- [ ] Parar polling em `DISABLED`, `STOPPING`, app final, unload ou abort.
- [ ] Executar testes Scala, JavaScript e live.
- [ ] Inspecionar o JAR e confirmar os três assets.

**Acceptance criteria:**

- Spark UI contém link `DataShip`.
- Página e assets retornam `200` e content types corretos.
- Dados visíveis vêm dos endpoints versionados.
- Polling usa um segundo e para nos estados definidos.
- Falha de fetch exibe estado degradado local, sem afetar o job.
- Não há nome, logo, asset ou código visual do DataFlint.

**User gate:** apresentar HTML/requests, teste de polling e job concluído; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add Makefile spark-observer build/scripts/run-observer-live-probe.sh
git commit -m "feat: add minimal DataShip Spark UI tab"
```

---

## Task 11: provar fail-open, limites e segurança

**Hipótese:** configuração inválida, runtime não suportado, snapshot quebrado, fila cheia e excesso de requests degradam somente o observador.

**Files:**

- Create: `spark-observer/src/main/scala/io/dataship/spark/observer/api/RequestLimiter.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/FailOpenSpec.scala`
- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/api/RequestLimiterSpec.scala`
- Modify: `ObserverConfig.scala`
- Modify: `ObserverRuntime.scala`
- Modify: `SparkDataShipDriverPlugin.scala`
- Modify: servlets em `spark-observer/src/main/scala/io/dataship/spark/observer/api/`
- Modify: `build/scripts/assert-observer-response.py`
- Modify: `build/scripts/run-observer-live-probe.sh`

**Interfaces:**

- Erros públicos são códigos estáveis; stack traces permanecem apenas no log redigido.
- Limite concorrente default `4`; excedentes recebem `429`.
- Erro isolado do snapshot recebe `500`, health continua disponível e o job continua.
- Config inválida recebe `DEGRADED` com `INVALID_CONFIG`.

**Red phase:**

- [ ] Criar testes unitários para cada falha e limite.
- [ ] Criar integração que dispara requests concorrentes acima do limite.
- [ ] Criar fixtures com credenciais sentinela para MinIO/ClickHouse sem imprimir valores.
- [ ] Executar testes e confirmar que os comportamentos ainda não existem.

**Green phase:**

- [ ] Implementar fail-open por fronteira: config, listener, snapshot, servlet e UI.
- [ ] Implementar semáforo não bloqueante do HTTP.
- [ ] Garantir que qualquer erro atualiza `internalFailures` e `lastErrorCode`.
- [ ] Executar cenário de fila cheia, snapshot falho e excesso de polling.
- [ ] Executar scan automatizado das respostas contra valores sentinela.
- [ ] Confirmar que cada workload termina com mesmo resultado e exit code `0`.

**Acceptance criteria:**

- Nenhum cenário controlado derruba o driver.
- `429` aparece somente quando a concorrência excede o limite.
- Após um `500` de snapshot, `/health` ainda responde.
- Config e versão inválidas são explícitas e não instalam coleta incompatível.
- Respostas não expõem credenciais, ambiente, stack trace ou SparkConf completo.
- Fila cheia continua não bloqueante.

**User gate:** apresentar matriz falha → HTTP/status → resultado do job; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/assert-observer-response.py build/scripts/run-observer-live-probe.sh
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

**Red phase:**

- [ ] Escrever o gate exigindo os 12 critérios do design.
- [ ] Executar `make observer-smoke`.
- [ ] Confirmar falha porque o target/script consolidado ainda não existe.

**Green phase:**

- [ ] Orquestrar build, compose, submit live, health, counters `t1/t2`, snapshots, transição, UI, backpressure, fail-open, plugin off e cleanup.
- [ ] Confirmar automaticamente processo vivo em cada prova HTTP.
- [ ] Executar `make validate`, `make tests`, `make observer-tests`, `make smoke` e `make spark-logs`.
- [ ] Confirmar event logs em `spark-logs/events`, aplicação no History Server e carga existente no ClickHouse.
- [ ] Comparar schemas e arquivos congelados com `main`; exigir diff vazio.
- [ ] Documentar comandos operacionais e matriz exata suportada.

**Acceptance criteria:**

- O teste automatizado demonstra, na mesma execução, os 12 itens da seção 14 do design.
- Plugin ligado e desligado produzem o mesmo resultado funcional.
- Event log, History, loader e ClickHouse continuam nos caminhos atuais.
- Nenhum schema ClickHouse, loader Go, bucket ou prefixo muda.
- Todos os comandos retornam exit code `0`.
- A árvore fica limpa após cleanup e antes do commit.

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
9. snapshot SQL redigido;
10. aba DataShip;
11. fail-open e segurança;
12. E2E e regressão durável.

Qualquer mudança de arquitetura descoberta durante a execução deve gerar uma parada e uma atualização aprovada deste plano antes de modificar código fora da task ativa.
