# DataShip Spark Observer v0 Implementation Plan

**Revisão:** 2026-07-16 — feedback técnico incorporado antes da Task 1.

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:test-driven-development` and `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do not use subagents unless the user explicitly authorizes them.

**Goal:** construir e provar um plugin JVM opt-in no driver Spark 4.1.2 que exponha estado live, endpoints versionados e uma aba mínima na Spark UI sem interferir no job nem alterar o caminho durável existente.

**Architecture:** o plugin terá uma entrada pública pequena em `io.dataship.spark.observer` e todo acesso a APIs internas do Spark ficará isolado em `org.apache.spark.dataship.v412`. Jobs, stages e SQL serão lidos dos stores nativos; apenas contadores e uma janela limitada de transições pertencerão ao plugin. Build e testes JVM usarão um container sbt/Java 17, enquanto o JAR será executado exclusivamente pelo driver no container Spark.

**Tech Stack:** Spark `4.1.2`, Scala `2.13.17`/binary `2.13`, Java `17`, sbt `1.10.11`, ScalaTest `3.2.19`, PySpark, Bash, Python `3.10+`, Node `24.13.1` exclusivamente em container para testes do JavaScript estático, Docker Compose.

**Design aprovado:** `docs/spark-observer/2026-07-15-dataship-spark-observer-live-driver-design.md`

## Global Constraints

- O host não deve receber Java, Scala, sbt ou Node; ele precisa apenas das ferramentas já usadas pelo repositório, especialmente Docker e Make.
- A imagem de build JVM será `sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.10.11_2.13.16`.
- A tag da imagem sbt permanece fixada em `2.13.16`, mas o projeto e suas dependências Scala usarão `2.13.17`, alinhados ao runtime real do Spark `4.1.2`.
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
- Toda task que alterar o JAR deverá executar `make observer-runtime-refresh` antes de qualquer prova live; esse target recompila, faz staging, reconstrói a imagem Spark, recria `spark-master` e `spark-worker` e aguarda prontidão com timeout.
- Nenhuma resposta poderá expor `SparkConf` completo, ambiente, credenciais, fonte do usuário ou descrição SQL bruta.
- Nenhuma task poderá alterar código de tasks posteriores para “adiantar” trabalho.
- Toda task produzirá uma evidência visual proporcional ao comportamento entregue. Superfícies HTTP/UI serão abertas e capturadas com Playwright quando existirem; tasks sem superfície visual produzirão uma tabela ou relatório visual inspecionável por uma pessoa, com o estado, artefato ou transição comprovada.
- Evidências visuais de aceite ficarão em `docs/spark-observer/evidence/task-XX/`, sem segredos, e serão versionadas no mesmo checkpoint da task. O `execution-log.md` registrará o comando, o caminho e o que cada artefato comprova.

---

## Correções incorporadas antes da Task 1

| # | Decisão executável |
| --- | --- |
| 1 | `execution-log.md` participa de todos os checkpoints de commit. |
| 2 | Toda prova live usa `observer-runtime-refresh`, aguarda master + worker prontos e só então valida o checksum do JAR no container. |
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
   | Evidência visual | screenshot ou relatório proporcional, com caminho e o que ele prova |
   | Regressão | comandos e resultados |
   | Risco restante | o que ainda não foi provado |
   | Próxima fatia | apenas o título, sem iniciá-la |

9. Parar sem commit e aguardar o usuário responder `ACEITO`.
10. Depois do aceite, executar somente o checkpoint de commit da task, sempre incluindo `docs/spark-observer/execution-log.md` e `docs/spark-observer/evidence/task-XX/`, e mostrar hash/status.
11. Parar novamente. A próxima task exige um novo pedido explícito.

Se qualquer gate falhar, não commitar e não iniciar outra task. Aplicar `superpowers:systematic-debugging`, registrar `FAIL` ou `BLOCKED` e pedir direção quando necessário.

A partir da Task 2, toda task executará `make tests` antes do user gate para manter ativo `tests/test_observer_platform_contract.py`. Se a task alterar o JAR e possuir prova live, `make observer-runtime-refresh` é pré-condição obrigatória dessa prova.

Antes de cada commit, a lista de arquivos alterados deve ser subconjunto de `Files` da task mais `docs/spark-observer/execution-log.md` e `docs/spark-observer/evidence/task-XX/`. Também deve continuar sem saída:

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
- `docs/spark-observer/evidence/task-XX/`: screenshots e relatórios visuais persistidos para revisão humana e por outros agentes.

---

## Task 1: congelar a baseline e o protocolo de evidência

**Hipótese:** a plataforma pode ser preparada a partir de checkout novo, passa seus gates sem o plugin, não publica `24040` na baseline e mantém o lado direito funcional antes de qualquer feature.

**Files:**

- Create: `docs/spark-observer/execution-log.md`
- Create: `docs/spark-observer/evidence/task-01/spark-master-baseline.png`
- Create: `docs/spark-observer/evidence/task-01/spark-history-baseline.png`
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

**Evidência visual para aceite:** capturar com Playwright a Spark Master UI após o smoke, mostrando o worker `ALIVE`, e a History Server UI com a aplicação reconstruída; registrar separadamente que `24040/dataship` não possui superfície para capturar na baseline.

**User gate:** apresentar a baseline completa, as capturas da Master/History e parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-01
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
- Create: `build/scripts/verify-observer-toolchain.sh`
- Modify: `docs/spark-observer/2026-07-15-dataship-spark-observer-implementation-plan.md`
- Create: `spark-observer/src/test/js/toolchain.test.mjs`
- Create: `tests/test_observer_platform_contract.py`

**Interfaces:**

- `BuildInfo.PluginVersion = "0.1.0-SNAPSHOT"`
- `BuildInfo.SupportedSparkVersion = "4.1.2"`
- `BuildInfo.ScalaBinaryVersion = "2.13"`
- Make targets: `observer-tests`, `observer-jar`, `observer-ui-tests` e `observer-verify`.
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

- [ ] Adicionar `BuildInfo` e configurar Scala `2.13.17`, alinhado ao runtime do Spark `4.1.2`, Spark Core/SQL `4.1.2` como `provided` e ScalaTest `3.2.19` em `Test`.
- [ ] Fazer os targets sbt rodarem em Docker com usuário do host e volumes de cache, sem chamar `java` ou `sbt` no host.
- [ ] Executar `make observer-tests`; esperar todos os testes verdes.
- [ ] Executar `make observer-jar`; esperar `spark-observer/target/scala-2.13/dataship-spark-observer_2.13-0.1.0-SNAPSHOT.jar`.
- [ ] Listar o JAR dentro do container e confirmar ausência de `org/apache/spark/` e `scala/`.

### Hardening do gate de aceite após review

O gate final da Task 2 será reproduzível a partir de arquivos versionados. Ele não chamará helpers em `.superpowers/`, `/tmp` ou qualquer outro caminho ignorado.

**RED adicional:**

- [ ] Estender `tests/test_observer_platform_contract.py` para exigir os valores completos:

```text
SBT_IMAGE=sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.10.11_2.13.16
NODE_IMAGE=node:24.13.1-bookworm-slim
```

- [ ] O mesmo teste exigirá `build/scripts/verify-observer-toolchain.sh`, o target `observer-verify`, o trap de exit code, a inspeção das imagens e a sequência completa de comandos.
- [ ] Executar `uv run pytest tests/test_observer_platform_contract.py -q`.
- [ ] Confirmar falha porque o script e o target ainda não existem. Ausência de Docker ou de imagens não será um RED válido.

**GREEN adicional:**

- [ ] Criar `build/scripts/verify-observer-toolchain.sh` com `set -euo pipefail` e trap `EXIT` que sempre imprima `observer_verify_exit_code=<código>`.
- [ ] Fixar dentro do script as imagens sbt e Node exatas acima e passá-las como variáveis de linha de comando para os targets Observer, impedindo override silencioso por `.env`.
- [ ] Imprimir `git rev-parse HEAD`.
- [ ] Para cada imagem fixada, executar `docker image inspect` e imprimir nome, ID real e `RepoDigests`; imagem ausente encerra o gate com código diferente de zero.
- [ ] Executar, nesta ordem:

```text
make observer-tests
make observer-ui-tests
make observer-jar
jar tf do artefato dentro da imagem sbt fixada
verificação de ausência de org/apache/spark/ e scala/
wc -c e sha256sum do artefato
make tests
make validate
```

- [ ] Adicionar `observer-verify` ao `Makefile` chamando somente `build/scripts/verify-observer-toolchain.sh`.
- [ ] Executar novamente o teste focado; esperar todos verdes.
- [ ] Capturar a execução diretamente, sem wrapper intermediário:

```bash
script -qefc 'make observer-verify' \
  docs/spark-observer/evidence/task-02/task-02-reproducible-verification.txt
```

- [ ] Confirmar no header do transcript `COMMAND="make observer-verify"`, commit `4ed06277a2b94a486dd5b42c78abfae930403534` e `COMMAND_EXIT_CODE="0"`.
- [ ] Renderizar uma captura Playwright desse transcript e atualizar o relatório visual.
- [ ] Não recriar o RED original removendo `BuildInfo`. Como nenhum transcript bruto original foi conservado, registrar apenas essa ausência e manter o RED documental existente.

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
- `make observer-verify` é reproduzível sem helpers ignorados e registra commit, imagens reais, IDs/digests, testes, JAR e exit code final.
- `SBT_IMAGE` e `NODE_IMAGE` possuem valores completos protegidos por teste e são forçados pelo gate, mesmo se `.env` contiver overrides locais.

**Evidência visual para aceite:** apresentar um relatório tabular renderizado com imagem sbt/Node, IDs/digests reais, versões, testes, tamanho do JAR e verificação de ausência de classes Spark/Scala. A evidência primária será a captura derivada do transcript produzido diretamente por `script -qefc 'make observer-verify'`; não criar UI de produto nesta task.

**User gate:** mostrar comandos, imagem usada, relatório visual do artefato, conteúdo relevante do JAR e parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add .env.example .gitignore Makefile build/scripts/bootstrap.sh build/scripts/validate-bootstrap.sh build/scripts/verify-observer-toolchain.sh spark-observer tests/test_observer_platform_contract.py docs/spark-observer/2026-07-15-dataship-spark-observer-implementation-plan.md docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-02
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
- Create: `build/scripts/wait_spark_runtime_ready.py`
- Create: `tests/test_spark_runtime_readiness.py`

**Interfaces:**

- Public class: `io.dataship.spark.observer.SparkDataShipPlugin`.
- `driverPlugin(): DriverPlugin` returns a new `SparkDataShipDriverPlugin`.
- `executorPlugin(): ExecutorPlugin` returns `null`.
- `ObserverConfig.from(sc.getConf)` lê `spark.dataship.observer.enabled`, com default `false`.
- O JAR gerado é copiado para `/opt/spark/jars/` durante `make build`.
- O target `observer-runtime-refresh` executa, nesta ordem: `observer-jar`, staging pelo `prepare-image-contexts.sh`, rebuild da imagem `SPARK_RUNTIME_IMAGE`, remoção dos containers Spark antigos, preflight da porta host quando o Compose publicar 4040, recriação de `spark-master` e `spark-worker`, e `uv run python build/scripts/wait_spark_runtime_ready.py`.
- O readiness usa polling a cada dois segundos e timeout explícito de `120` segundos; exige HTTP `200` da Spark Master UI e pelo menos um worker registrado com estado `ALIVE`.
- Checksum do JAR e testes live só podem começar depois que o readiness retornar exit code `0`.

**Red phase:**

- [ ] Criar testes que instanciam `SparkDataShipPlugin`, verificam o tipo do driver, `executorPlugin() == null` e os estados enabled/disabled.
- [ ] Criar `tests/test_spark_runtime_readiness.py` com respostas fake para master indisponível, master sem worker, worker `ALIVE` e timeout determinístico.
- [ ] Executar `make observer-tests`.
- [ ] Confirmar falha de compilação porque as classes ainda não existem.
- [ ] Executar `uv run pytest tests/test_spark_runtime_readiness.py -q`; confirmar falha porque o helper de readiness ainda não existe.

**Green phase:**

- [ ] Implementar apenas as classes de bootstrap e o parsing da flag enabled; `init` retorna mapa vazio e não faz I/O.
- [ ] Integrar o JAR produzido ao contexto da imagem Spark sem colocá-lo no bootstrap manifest de dependências externas.
- [ ] Implementar `wait_spark_runtime_ready.py` com funções testáveis, timeout, diagnóstico final sem segredos e exit code diferente de zero quando master ou worker não ficarem prontos.
- [ ] Executar `uv run pytest tests/test_spark_runtime_readiness.py -q`; esperar todos os casos verdes sem esperar 120 segundos reais.
- [ ] Executar `make observer-tests` e `make observer-runtime-refresh`.
- [ ] Confirmar no output que a Master UI respondeu e ao menos um worker ficou `ALIVE` antes da etapa de checksum.
- [ ] Verificar dentro de `spark-master` que o checksum do JAR corresponde ao artefato recém-produzido no host.
- [ ] Executar `make compose` e `make smoke` sem flags do plugin.
- [ ] Executar `check_sanity.py` com `spark.plugins` e `spark.dataship.observer.enabled=true`.
- [ ] Confirmar exit code `0` nos dois modos e nenhuma rota `/dataship/`.

**Acceptance criteria:**

- O JAR está em `/opt/spark/jars/`.
- O refresh falha claramente após 120 segundos se master ou worker não ficarem prontos.
- Em sucesso, master responde HTTP `200` e existe pelo menos um worker `ALIVE` antes do checksum.
- O checksum prova que a imagem/container usa o JAR recém-produzido, não um artefato anterior.
- Estar no classpath não ativa o plugin.
- O plugin habilitado é carregado no driver e não nos executors.
- Nenhuma thread, listener, endpoint ou aba customizada existe ainda.
- O resultado do workload é idêntico com plugin ligado e desligado.

**Evidência visual para aceite:** capturar a Spark Master UI após as execuções e apresentar um relatório lado a lado com readiness, checksums e resultados plugin off/on.

**User gate:** mostrar testes unitários, readiness do master/worker, checksum, duas execuções Spark, evidência visual e parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add Makefile build/scripts/prepare-image-contexts.sh build/scripts/wait_spark_runtime_ready.py tests/test_spark_runtime_readiness.py spark-observer docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-03
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

- `observer_live_probe.py --rows 40 --partitions 4 --delay-ms 75 --hold-seconds 10`.
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
- O default de dez segundos fornece a janela live sem depender de velocidade específica do CI.
- O workload termina sozinho com exit code `0` nos dois modos.
- Há dois jobs, shuffle, múltiplos stages e uma execução SQL.
- Falha induzida no harness não deixa processo do probe nem endpoint respondendo; o mapping do container permanece, como esperado.
- Master UI em `28081` e driver UI em `24040` são demonstradas como interfaces distintas.
- Uma segunda execução reutiliza `24040:4040` com sucesso.

**Evidência visual para aceite:** capturar com Playwright a Spark UI nativa do driver em `24040` enquanto o probe está vivo e a Master UI em `28081`, comprovando visualmente que são interfaces distintas.

**User gate:** apresentar timeline do PID, respostas HTTP, capturas das duas UIs e cleanup; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add .env.example Makefile build/docker-compose.yml build/scripts/run-observer-live-probe.sh src/apps/observer_live_probe.py tests/test_observer_live_probe.py docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-04
git commit -m "test: add deterministic live observer workload"
```

---

## Task 5: implement lifecycle and the health endpoint

**Hypothesis:** `registerMetrics` attaches `/health` after `appId` exists, and the response remains `READY` while the submit process is alive.

**Files:**

- Modify: `docs/spark-observer/2026-07-15-dataship-spark-observer-implementation-plan.md` (keep this Task 5 section and its artifact rule in English)
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

- `init(sc, pluginContext)` validates configuration, stores the `SparkContext`, and does not install HTTP.
- `registerMetrics(appId, pluginContext)` uses the stored `SparkContext` to access `sc.ui` and installs the handler at most once when the supported Spark UI exists.
- `PluginContext` is used only through its public APIs, such as configuration and metrics; no task assumes `pluginContext.ui` exists.
- `shutdown()` transitions to `STOPPING` and closes resources idempotently.
- The health JSON contains the stable envelope plus `status`, `uiAttached`, `listenerInstalled`, `supportedRuntime`, `queueCapacity`, and `lastErrorCode`.
- Without a Spark UI, the plugin records the stable `NO_SPARK_UI` code, installs no handlers, and lets the job complete; no HTTP endpoint is promised in this mode.
- Every source file, test, message, Task 5 execution-log entry, evidence report, transcript label, and screenshot label created or changed by this task must be in English.

**Red phase:**

- [ ] Create tests for configuration, the envelope, lifecycle states, and serialization without extra fields.
- [ ] Create tests for calling the installer twice and for the `sc.ui.isEmpty` path, using the `SparkContext` stored by `init`.
- [ ] Create Python tests that reject an incorrect status, content type, JSON document, or required field.
- [ ] Run `make observer-tests` and both Python test modules; confirm the expected behavioral failures.
- [ ] Run the harness expecting `/health`; confirm the endpoint is absent under Spark's native unknown-route behavior.

**Green phase:**

- [ ] Implement immutable configuration and task-owned DTOs.
- [ ] Use the Jackson runtime provided by Spark to serialize only allowlisted maps, lists, and primitive values.
- [ ] Implement the Spark 4.1.2 adapter under `org.apache.spark.dataship.v412`.
- [ ] Attach the servlet to the existing Spark UI without starting another server.
- [ ] Make installation idempotent and treat a missing Spark UI as an isolated degradation.
- [ ] Implement bounded waiting in the harness and Python validation without `jq`.
- [ ] Run `make observer-runtime-refresh` and use a checksum to confirm that the container received this task's JAR before the live test.
- [ ] Run the live test and collect two health responses while the submit process is alive.
- [ ] After shutdown, prove that the endpoint is unavailable, the identified process is absent inside the container, and a second run succeeds through the same persistent mapping.
- [ ] Run the workload with `spark.ui.enabled=false`; require exit code `0` and `NO_SPARK_UI` in the redacted log without promising `/health`.

**Acceptance criteria:**

- `/health` responds with `200` and `application/json`.
- `schemaVersion=v1`, `pluginVersion=0.1.0-SNAPSHOT`, `sparkVersion=4.1.2`, `mode=live`.
- `appId` is non-empty, `status=READY`, `uiAttached=true`, and `supportedRuntime=true`.
- `capturedAt` is an ISO-8601 UTC timestamp and is not compared as a fixed literal.
- `init` performs no I/O and does not scan stores.
- Two installation calls do not duplicate a handler or resource.
- With `spark.ui.enabled=false`, the job completes and no handler is installed.
- With `spark.plugins` present and `enabled=false`, only `/health` responds with `DISABLED`; no listener, snapshot, or tab is installed.
- Two consecutive runs leave no stuck thread or process and reuse the persistent mapping.

**Visual acceptance evidence:** open `/dataship/api/v1/health` with Playwright during the submit process and capture the rendered JSON with visible `READY`, `appId`, and versions, without secrets.

**User gate:** present the real allowlisted JSON, the health screenshot, the live internal process, and the second run; then stop.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/assert-observer-response.py build/scripts/run-observer-live-probe.sh tests/test_observer_response_assertions.py docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-05
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

**Evidência visual para aceite:** apresentar um relatório visual compacto com a sequência `received -> queued/inFlight -> processed/dropped`, capacidades e tempos observados nos testes de fila.

**User gate:** apresentar tempos, contadores, relatório visual e repetição sem flakiness; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer/src/main/scala/io/dataship/spark/observer/events spark-observer/src/test/scala/io/dataship/spark/observer/events spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-06
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

**Evidência visual para aceite:** capturar com Playwright `/debug/counters` em `t1`, `t2` e no cenário de drop, ou montar comparação lado a lado das três respostas reais.

**User gate:** apresentar `t1`, `t2`, cenário de drop, comparação visual e resultados; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/run-observer-live-probe.sh docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-07
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

**Evidência visual para aceite:** capturar `/snapshot` em `t1` e `t2` e apresentar lado a lado o job/stage `RUNNING -> SUCCEEDED`, incluindo o caso `limit=1`.

**User gate:** apresentar snapshots `t1`/`t2`, comparação visual, limite e transição; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/run-observer-live-probe.sh docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-08
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
- [ ] Acrescentar ao harness a exigência de
  `receivedByCategory.sql > 0` enquanto o mesmo driver e a execução SQL estão
  vivos; classificação unitária isolada não conta como prova live.
- [ ] Executar e confirmar falhas esperadas.

**Green phase:**

- [ ] Implementar allowlist, opt-in e redação antes do truncamento.
- [ ] Localizar a store/listener SQL somente no adapter v4.1.2.
- [ ] Mapear estado sem instrumentar `SparkPlan`, extensions ou AQE.
- [ ] Executar `make observer-runtime-refresh` e validar o checksum antes dos cenários live.
- [ ] Executar live com `spark.sql(...)`; capturar a execução ativa e final sem exigir o SQL literal.
- [ ] Correlacionar a execução SQL capturada com uma resposta live de
  `/debug/counters` que tenha `receivedByCategory.sql > 0` no mesmo run.
- [ ] Executar o default e provar `descriptionAvailable=false`.
- [ ] Executar um fixture sintético que define `spark.job.description`, habilita a descrição e configura redação de string; documentar explicitamente que isso testa redação, não captura universal do SQL.
- [ ] Executar caminho DataFrame API; provar que nenhum código-fonte é reconstruído.
- [ ] Examinar JSON e confirmar ausência dos valores sensíveis de teste.

**Acceptance criteria:**

- Uma execução iniciada por `spark.sql(...)` aparece por id/status, sem promessa de SQL literal.
- Descrição fica ausente por default; no fixture sintético opt-in, aparece redigida, truncada e rotulada `SPARK_STATUS_STORE`.
- Uma transição SQL live é observada.
- A categoria SQL do listener é comprovada em runtime por
  `receivedByCategory.sql > 0`, no mesmo run da transição SQL.
- DataFrame API não recebe código-fonte reconstruído.
- Nenhum plano é envolvido, reescrito ou modificado.
- Respostas não contêm segredo conhecido do fixture.

**Evidência visual para aceite:** capturar o recorte SQL do snapshot mostrando transição e descrição redigida; a captura deve permitir verificar que o valor sentinela não aparece.

**User gate:** apresentar metadados/transição, captura redigida, descrição sintética e caso DataFrame; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer src/apps/observer_live_probe.py build/scripts/run-observer-live-probe.sh docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-09
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

**Evidência visual para aceite:** usar Playwright para capturar a aba DataShip dentro da Spark UI durante o job, além do estado terminal/degradado relevante; verificar no browser que os dados vêm das rotas v1.

**User gate:** apresentar screenshots da aba, HTML/requests, teste de polling e job concluído; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add Makefile spark-observer build/scripts/run-observer-live-probe.sh docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-10
git commit -m "feat: add minimal DataShip Spark UI tab"
```

---

## Task 11: provar fail-open, limites e segurança

**Hipótese:** configuração inválida, runtime não suportado, snapshot quebrado, fila cheia e polling acelerado degradam somente o observador, sem introduzir um limitador HTTP não justificado no v0.

**Files:**

- Create: `spark-observer/src/test/scala/io/dataship/spark/observer/FailOpenSpec.scala`
- Create: `spark-observer/src/test/scala/org/apache/spark/dataship/v412/Spark412BridgeLifecycleSpec.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/SparkDataShipDriverPlugin.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/api/CountersResponse.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/api/HealthServlet.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/api/CountersServlet.scala`
- Modify: `spark-observer/src/main/scala/io/dataship/spark/observer/api/SnapshotServlet.scala`
- Modify: `spark-observer/src/main/scala/org/apache/spark/dataship/v412/Spark412Bridge.scala`
- Modify: `build/scripts/assert-observer-response.py`
- Modify: `build/scripts/run-observer-live-probe.sh`

**Interfaces:**

- Erros públicos são códigos estáveis; stack traces permanecem apenas no log redigido.
- Erro isolado do snapshot recebe `500`, health continua disponível e o job continua.
- Config inválida em Spark 4.1.2 recebe `DEGRADED` com `INVALID_CONFIG`.
- Versão não suportada impede a factory do adapter `v412`, registra `UNSUPPORTED_SPARK_VERSION` e não promete rota HTTP.
- Não existe resposta `429` no contrato v0; proteção HTTP própria só entra em outro design se medições demonstrarem necessidade.
- Antes do primeiro evento, `lastEventAt` é JSON `null`; depois de
  `listenerReceived > 0`, ele é um timestamp UTC ISO-8601 válido.
- O fechamento do adapter tenta remover listener, health handler e counters
  handler independentemente; uma falha de detach não impede os demais
  cleanups.

**Red phase:**

- [ ] Criar testes unitários para cada fronteira de falha e para a guarda que prova que a factory `v412` não é chamada em versão diferente.
- [ ] Injetar uma exceção inesperada em `process(event)`; confirmar primeiro que o worker atual encerra e que eventos posteriores não são processados.
- [ ] Criar teste do contrato zero-evento: `listenerReceived=0` e
  `lastEventAt=null`; depois do primeiro evento, exigir timestamp UTC válido.
- [ ] Criar teste direto do lifecycle do adapter: instalação registra uma vez
  os dois handlers e o listener na fila `dataship-observer`; fechamento remove
  o listener e tenta remover ambos os handlers.
- [ ] Induzir falha no primeiro cleanup do adapter e provar que os recursos
  restantes ainda recebem tentativa de remoção.
- [ ] Criar integração com polling acelerado por intervalo fixo, sem expectativa de `429`.
- [ ] Criar fixtures com credenciais sentinela para MinIO/ClickHouse sem imprimir valores.
- [ ] Executar testes e confirmar que os comportamentos ainda não existem.

**Green phase:**

- [ ] Implementar fail-open por fronteira: config, listener, snapshot, servlet e UI.
- [ ] Tratar exceções inesperadas do processamento sem encerrar o worker; concluir a transição contábil, incrementar `internalFailures`, atualizar `lastErrorCode` e continuar processando eventos posteriores.
- [ ] Garantir que qualquer erro atualiza `internalFailures` e `lastErrorCode`.
- [ ] Tornar o cleanup do adapter best-effort e idempotente: tentar cada
  detach e a remoção do listener mesmo quando uma etapa falhar, preservando a
  primeira falha e adicionando as posteriores como suprimidas quando cabível.
- [ ] Serializar `lastEventAt` como `null` antes do primeiro evento e exigir
  timestamp UTC após o primeiro recebimento; atualizar o validator sem aceitar
  string vazia.
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
- Exceção inesperada em `process(event)` não encerra o worker, aparece nos contadores/health e não altera o resultado do workload.
- O teste direto do adapter comprova instalação e remoção do listener e dos
  dois handlers, inclusive cleanup restante após uma falha induzida.
- O contrato de `lastEventAt` é determinístico tanto antes quanto depois do
  primeiro evento.

**Evidência visual para aceite:** renderizar a matriz falha → HTTP/status → resultado do job e capturar `/health` ainda disponível depois do `500` induzido no snapshot.

**User gate:** apresentar matriz visual, health pós-falha e resultado do job; parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add spark-observer build/scripts/assert-observer-response.py build/scripts/run-observer-live-probe.sh docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-11
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
- [ ] No mesmo run da transição SQL, exigir
  `receivedByCategory.sql > 0`; o E2E não pode depender apenas do teste
  unitário do classificador.
- [ ] Incluir no relatório os resultados dos testes focados de lifecycle do
  adapter e do contrato zero-evento de `lastEventAt`, executados por
  `make observer-tests`.
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
- O relatório E2E registra a prova live da categoria SQL e os gates de
  lifecycle/cleanup do adapter e `lastEventAt` inicial.
- Antes do commit, o status contém somente os arquivos da Task 12, `execution-log.md` e `evidence/task-12/`; depois do commit, a árvore fica limpa.

**Evidência visual para aceite:** capturar a aba DataShip live, a aplicação final no History Server e o relatório E2E `PASS`, formando a evidência visual final do caminho live e da regressão durável.

**User gate:** apresentar relatório final completo, conjunto de capturas e parar.

**Commit checkpoint after `ACEITO`:**

```bash
git add Makefile build/scripts/validate-observer-e2e.sh docs/dev-design/operations.md docs/dev-design/compatibility.md docs/spark-observer/execution-log.md docs/spark-observer/evidence/task-12
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
