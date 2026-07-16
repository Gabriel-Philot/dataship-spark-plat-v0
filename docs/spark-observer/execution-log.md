# DataShip Spark Observer Execution Log

## Task 1 — congelar a baseline e o protocolo de evidência

**Data:** 2026-07-16
**Branch:** `exp-dataflint-based-test-jar`
**HEAD inicial:** `6b65090f0bb6afc4ff7a5898635f066d0c8d5bac`
**Baseline arquitetural fixada:** `89202730dfd19d35d52c35d61b739dad4fcca345`

**Hipótese:** a plataforma pode ser preparada a partir do checkout atual, passa seus gates sem o plugin, não publica `24040` na baseline e mantém o caminho durável existente funcional antes de qualquer feature.

**Mudança mínima:** registrar somente as evidências da baseline neste arquivo; nenhum código de produção será alterado.

**Verificações de ausência esperada:** o Compose da baseline não deve possuir mapping para `spark-master:4040`, e `http://127.0.0.1:24040/dataship/api/v1/health` deve recusar conexão. Esses resultados não são falhas da plataforma; eles comprovam que o Observer ainda não existe na baseline.

**Retorno observável esperado:** bootstrap, build, validações, testes, smoke e carga existente de event logs terminam com exit code `0`; Spark Master e History Server ficam visualmente inspecionáveis; a rota DataShip continua ausente.

**Regressão:** nenhum diff nos caminhos protegidos de ClickHouse, loader Go, MinIO init ou `spark-defaults.conf`.

**Evidência visual planejada:** screenshots da Spark Master UI com worker `ALIVE` e da History Server UI com a aplicação reconstruída, persistidos em `docs/spark-observer/evidence/task-01/`.

**Status:** `PASS — ACEITO` — Task 1 encerrada pelo usuário em 2026-07-16.

### Preflight

- Branch confirmada: `exp-dataflint-based-test-jar`.
- Árvore limpa antes da task: sim.
- Commit documental anterior: `6b65090f0bb6afc4ff7a5898635f066d0c8d5bac`.
- Baseline fixa resolvida: `89202730dfd19d35d52c35d61b739dad4fcca345`.
- Allowlist final da task: `docs/spark-observer/execution-log.md` e `docs/spark-observer/evidence/task-01/`.

### Execução

| Gate | Comando | Exit code | Evidência |
| --- | --- | ---: | --- |
| Bootstrap | `make bootstrap` | 0 | Dependências Python sincronizadas, imagens base disponíveis, JARs/wheels/vendor confirmados. |
| Build | `make build` | 0 | Imagens locais da plataforma reconstruídas. |
| Validação | `make validate` | 0 | `Validation passed`. |
| Testes | `make tests` | 0 | `12 passed in 0.06s`. |
| Limpeza prévia | `make down` | 0 | Containers antigos removidos sem apagar volumes. |
| Preflight 24040 | `ss -ltn 'sport = :24040'` | 0 | Nenhum listener encontrado. A primeira chamada confinada não pôde abrir netlink; a repetição somente leitura fora do sandbox confirmou a porta livre. |
| Compose | `make compose` | 0 | MinIO, ClickHouse, Spark Master, um worker e History Server prontos. |
| Smoke | `make smoke` | 0 | Landing, bronze e sanity concluíram; `Smoke validation passed`. A primeira tentativa foi bloqueada pelo sandbox ao acessar o Docker socket; a repetição com o mesmo comando e acesso autorizado passou. |
| Mapping ausente | `docker compose --env-file .env -f build/docker-compose.yml port spark-master 4040` | 1 esperado | `no port 4040/tcp`; somente a UI do master está publicada em `28081`. |
| Observer ausente | `curl --fail --silent --show-error --connect-timeout 3 http://127.0.0.1:24040/dataship/api/v1/health` | 7 esperado | Conexão recusada; nenhuma rota DataShip existe na baseline. |
| Event logs | `make spark-logs` | 0 | `raw=499`, `sql=13`, `stages=15`, `tasks=168`; `ClickHouse validation passed`. |
| Master UI | Playwright em `http://127.0.0.1:28081/` | 0 | Worker `ALIVE` e três aplicações concluídas visíveis. |
| History UI | Playwright em `http://127.0.0.1:28080/` | 0 | Três aplicações reconstruídas a partir de `s3a://spark-logs/events`. |

### Aplicações observadas

- `spark-plat-v0-sample-customer-landing`
- `spark-plat-v0-smoke-customer-bronze`
- `spark-plat-v0-sanity-customer-lakehouse`

As três aplicações aparecem uma vez na Spark Master UI e uma vez na API/UI do History Server.

### Evidência visual

- `docs/spark-observer/evidence/task-01/spark-master-baseline.png`
  - Spark `4.1.2`;
  - um worker no estado `ALIVE`;
  - três aplicações no estado `FINISHED`.
- `docs/spark-observer/evidence/task-01/spark-history-baseline.png`
  - diretório `s3a://spark-logs/events`;
  - as mesmas três aplicações reconstruídas e concluídas.

Os dois arquivos estão versionados dentro de `docs/` para que a evidência seja visível na branch e possa ser revisada por outros agentes.

### Regressão e escopo

- O escopo final da Task 1 contém somente `docs/spark-observer/execution-log.md` e as duas capturas em `docs/spark-observer/evidence/task-01/`.
- O diff contra `89202730dfd19d35d52c35d61b739dad4fcca345` permanece vazio em:
  - `build/clickhouse`;
  - `build/images/eventlog-loader`;
  - `build/images/minio/init-buckets.sh`;
  - `build/config/spark/spark-defaults.conf`.
- Nenhum código de produção foi alterado.

### Aceite do usuário

- Resultado: `ACEITO`.
- Task 1 encerrada em 2026-07-16.
- O aceite não inicia automaticamente a Task 2.

### Risco restante

A Task 1 caracteriza somente a baseline sem plugin. Ela ainda não prova toolchain JVM, carregamento do JAR, endpoints, listener ou aba DataShip.

### Próxima fatia

Task 2 — adicionar o toolchain JVM totalmente conteinerizado.

## Task 2 — adicionar o toolchain JVM totalmente conteinerizado

**Data:** 2026-07-16
**Branch:** `exp-dataflint-based-test-jar`
**HEAD inicial:** `ec3e76486cdd6ae6a08a003c8c9584af853274f0`
**Status:** `READY — aguardando ACEITO do usuário`

**Hipótese:** o módulo Scala e os testes JavaScript compilam/testam sem Java, Scala, sbt ou Node instalados no host.

**Plano de execução:** preparar os targets e caches Docker fixados, validar o contrato existente da plataforma, capturar RED por ausência exclusiva de `BuildInfo`, implementar o mínimo para GREEN, construir e inspecionar o JAR e executar `make validate`/`make tests`.

**Imagens fixadas:**

- sbt/Scala/JDK: `sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.10.11_2.13.16`;
- Node: `node:24.13.1-bookworm-slim`.

**Preflight:** branch e HEAD conferidos; árvore limpa; Docker `29.2.1`, GNU Make `4.3` e Python `3.12.3` disponíveis. `java`, `scalac` e `sbt` não estão no `PATH` do host. Node `24.13.1` existe no host, mas não será chamado por nenhum target do Observer.

**Allowlist:** somente os arquivos da Task 2, este log e `docs/spark-observer/evidence/task-02/`.

### Preparação do toolchain

| Gate | Comando | Exit code | Resultado |
| --- | --- | ---: | --- |
| Contrato existente | `make tests` antes de alterar `.env.example` | 0 | `13 passed in 0.07s`; o novo teste confirmou bucket `spark-logs`, prefixo `events/`, event log habilitado e os dois caminhos `s3a://spark-logs/events`. |
| Validação estática | `bash -n build/scripts/bootstrap.sh build/scripts/validate-bootstrap.sh` | 0 | Scripts sintaticamente válidos. |
| Dry run dos targets | `make -n observer-tests observer-jar observer-ui-tests` | 0 | Os três targets chamam imagens Docker fixadas; nenhum chama Java, sbt ou Node do host. |
| Bootstrap inicial | `make bootstrap` | 2 | Falha de scaffold antes do RED: `artifactName` recebeu `String`, mas sbt `1.10.11` espera função. O override redundante foi removido; o naming padrão do sbt produz o nome exigido. |
| Bootstrap final | `make bootstrap` | 0 | Imagens sbt e Node puxadas; caches `build/cache/sbt` e `build/cache/coursier` aquecidos por `sbt update`; bootstrap existente permaneceu íntegro. |
| Node conteinerizado | `make observer-ui-tests` | 0 | `1` teste, `1` passou, `0` falhou dentro de `node:24.13.1-bookworm-slim`. A primeira chamada confinada não acessou o Docker socket; a repetição autorizada passou. |
| Contrato focado | `uv run pytest tests/test_observer_platform_contract.py -q` | 0 | `1` teste passou. |

### RED válido

`BuildInfoSpec.scala` foi criado antes de `BuildInfo.scala`.

| Comando | Exit code | Falha observada | Motivo do RED ser válido |
| --- | ---: | --- | --- |
| `make observer-tests` | 2 | Quatro erros `not found: value BuildInfo` nas linhas `7`, `11`, `15` e `20`; `(Test / compileIncremental) Compilation failed`. | O target, Docker, sbt `1.10.11`, Java `17.0.15`, projeto e runner carregaram corretamente. A única ausência era a implementação testada. |

### GREEN e artefato

| Gate | Comando | Exit code | Resultado |
| --- | --- | ---: | --- |
| Scala GREEN | `make observer-tests` | 0 | `BuildInfoSpec`: `4` testes, `4` passaram, `0` falharam; `All tests passed`. |
| Empacotamento | `make observer-jar` | 0 | JAR criado em `spark-observer/target/scala-2.13/dataship-spark-observer_2.13-0.1.0-SNAPSHOT.jar`. |
| Metadados do JAR | `ls -l ...jar && sha256sum ...jar` | 0 | `2522` bytes; SHA-256 `ba3e358661a8e707eb238c5a00cfcdcd60bb02c14d44d3485c123da97630019c`. |
| Conteúdo no container | `docker run ... jar tf ...jar` com verificação de prefixes | 0 | Somente manifesto, diretórios `io/dataship/...` e `BuildInfo$.class`/`BuildInfo.class`; `PASS: no org/apache/spark/ or scala/ entries`. |

Spark Core `4.1.2` e Spark SQL `4.1.2` estão configurados como `provided`. O JAR não inclui classes do Spark nem da biblioteca Scala.

### Regressão final

| Comando | Exit code | Resultado |
| --- | ---: | --- |
| `make validate` | 0 | `Validation passed`. |
| `make tests` | 0 | `13 passed in 0.04s`. |
| `make observer-ui-tests` | 0 | `1` teste passou, `0` falhou. |
| `make observer-tests` | 0 | `4` testes passaram, `0` falharam; `All tests passed`. |
| `git diff --check` | 0 | Nenhum erro de whitespace. |

### Evidência visual

O relatório tabular renderizável está em:

- `docs/spark-observer/evidence/task-02/toolchain-report.md`.

Também foram persistidas evidências de terminal:

- `docs/spark-observer/evidence/task-02/task-02-tests-terminal.txt`;
- `docs/spark-observer/evidence/task-02/task-02-tests-terminal.png`;
- `docs/spark-observer/evidence/task-02/task-02-jar-terminal.txt`;
- `docs/spark-observer/evidence/task-02/task-02-jar-terminal.png`.

Os arquivos `.txt` foram capturados por `script` em sessões PTY reais e terminam com `COMMAND_EXIT_CODE="0"`. As imagens foram produzidas com Playwright a partir desses transcripts, sem reescrever o output. Elas mostram os testes Scala/Node/Python, a validação, o build, a listagem do JAR, sua verificação de conteúdo, tamanho e SHA-256.

O relatório registra ainda imagens e versões fixadas, fronteira host/container e as verificações de ausência de classes Spark/Scala. Nenhuma UI de produto foi criada.

### Gate do usuário

Antes do review, a Task 2 havia encerrado a primeira rodada de implementação e verificação sem commit, push ou stage. O review abaixo reabriu a task; o gate `ACEITO` continua pendente e a Task 3 não foi iniciada.

### Correções pós-review — implementação verificada

O review técnico reabriu a Task 2 para quatro correções aceitas:

- desabilitar `autoScalaLibrary` e declarar Scala explicitamente como `Provided`, alinhada ao runtime `2.13.17`;
- proteger o override real de `SPARK_HISTORY_OPTS`, o provider do History Server e `spark.eventLog.logStageExecutorMetrics`;
- substituir a validação permissiva dos caches por um marker vinculado à imagem sbt e aos dois arquivos de build;
- tornar o parser de `spark-defaults.conf` tolerante a linhas vazias e comentários.

**RED focado:** `uv run pytest tests/test_observer_platform_contract.py -q` terminou com exit `1` e três falhas esperadas: parser sem argumento/linhas vazias, `autoScalaLibrary := false` ausente e marker de cache ausente.

**GREEN local:** após as correções mínimas, o mesmo comando terminou com exit `0` e `4 passed`. `make tests` terminou com `16 passed in 0.05s`; `bash -n build/scripts/bootstrap.sh build/scripts/validate-bootstrap.sh` e `git diff --check` terminaram com exit `0`.

Esses comandos foram concluídos na rodada final pós-alinhamento, registrada ao fim desta seção.

#### Verificação Docker antes do override determinístico

O controlador executou a primeira rodada pós-review:

| Gate | Resultado |
| --- | --- |
| `make bootstrap` | PASS; marker correspondeu ao fingerprint esperado com prefixo `8adcc1…`; `build/cache/sbt/boot` e `build/cache/coursier/https` estavam não vazios |
| `make validate` | PASS |
| `make observer-tests` | PASS; `4/4` |
| `make observer-jar` | PASS |
| `make observer-ui-tests` | PASS; `1/1` |
| `make tests` | PASS; `16/16` |
| Inspeção do JAR | `2522` bytes; SHA-256 `ba3e358661a8e707eb238c5a00cfcdcd60bb02c14d44d3485c123da97630019c`; listing anterior preservado; nenhuma classe Spark/Scala |

O diagnóstico `show Provided / managedClasspath` não é uma task válida no sbt e terminou com `No such setting/task`. Ele foi substituído pela comparação válida:

- `Compile / managedClasspath` continha Scala e Spark;
- `Runtime / managedClasspath` estava vazio, comprovando o escopo `Provided`.

Essa comparação revelou que o sbt selecionava `scala-library-2.13.18.jar`, apesar da declaração direta `2.13.16`. A causa é eviction: Spark `4.1.2` requer Scala `2.13.17`, e `jackson-module-scala_2.13:2.21.2` requer `2.13.18`.

#### Tentativa intermediária de override 2.13.16 — TDD, posteriormente rejeitada

- RED: o contrato focado passou a exigir `dependencyOverrides` para `scala-library` `2.13.16`; `uv run pytest tests/test_observer_platform_contract.py -q` terminou com exit `1`, com somente essa assertion ausente.
- GREEN local do contrato estático: foi adicionado `dependencyOverrides += "org.scala-lang" % "scala-library" % "2.13.16"`; o teste focado terminou com `4 passed`, mas o bootstrap posterior rejeitou a combinação.
- Regressão local: `make tests` terminou com `16 passed in 0.05s`; sintaxe Bash e `git diff --check` passaram.

Como `spark-observer/build.sbt` participa do fingerprint, o override invalidou corretamente o marker anterior e exigiu a nova rodada Docker registrada abaixo.

#### Falha do override 2.13.16 e alinhamento ao runtime

A tentativa intermediária de forçar somente `scala-library` `2.13.16` foi rejeitada corretamente pelo `make bootstrap`:

- exit `2`;
- conflito Coursier `SameVersion`;
- `scala-library` forçada em `2.13.16` enquanto `scala-reflect` resolvia `2.13.17`.

A inspeção do runtime real `apache/spark:4.1.2-scala2.13-java17-python3-ubuntu` confirmou:

- `/opt/spark/jars/scala-library-2.13.17.jar`;
- `/opt/spark/jars/scala-reflect-2.13.17.jar`;
- `spark-submit` reporta Scala `2.13.17`.

O alinhamento correto é:

- `scalaVersion := "2.13.17"`;
- `scala-library:2.13.17` explícita em `Provided`;
- overrides determinísticos para `scala-library` e `scala-reflect` em `2.13.17`, impedindo que Jackson eleve somente a library para `2.13.18`;
- a tag `SBT_IMAGE` continua fixada em `..._2.13.16`; o sbt resolve o compiler/dependências do projeto em `2.13.17`.

**RED de alinhamento:** após mudar primeiro a expectativa do contrato, `uv run pytest tests/test_observer_platform_contract.py -q` terminou com exit `1`; somente `scalaVersion` ainda estava em `2.13.16`.

**GREEN local:** após alinhar build e overrides, o teste focado terminou com `4 passed`; `make tests` terminou com `16 passed in 0.05s`; Bash e diff checks passaram.

O plano `docs/spark-observer/2026-07-15-dataship-spark-observer-implementation-plan.md` foi corrigido minimamente para Scala `2.13.17` e incluído nos Files/checkpoint da Task 2.

#### Verificação Docker final após alinhamento Scala 2.13.17

| Gate | Resultado final |
| --- | --- |
| `make bootstrap` | exit `0`; marker igual ao fingerprint esperado `ca234188d67de43708a5fb97c9cbfce97fffd48ec68559f232563d26437bbdf3`; subtrees `build/cache/sbt/boot` e `build/cache/coursier/https` válidos e não vazios |
| `make validate` | exit `0` |
| `make observer-tests` | exit `0`; `4/4` testes |
| `make observer-jar` | exit `0` |
| `make observer-ui-tests` | exit `0`; `1/1` teste |
| `make tests` | exit `0`; `17/17` testes após o hardening final do contrato de History |
| `Compile / managedClasspath` | contém `scala-library:2.13.17`, `scala-reflect:2.13.17`, Spark Core `4.1.2` e Spark SQL `4.1.2` |
| `Runtime / managedClasspath` | vazio, confirmando o escopo `Provided` |
| JAR | listing exato anterior preservado; `2522` bytes; SHA-256 `ba3e358661a8e707eb238c5a00cfcdcd60bb02c14d44d3485c123da97630019c`; nenhuma entrada `org/apache/spark/` ou `scala/` |

**Estado final desta execução:** `READY — aguardando ACEITO do usuário`, sem commit, stage ou push. Isso não constitui `PASS`/`ACEITO`, e a Task 3 não foi iniciada.

#### Hardening do contrato live de History — re-review

- RED: `uv run pytest tests/test_observer_platform_contract.py -q` terminou com exit `1`; o novo teste falhou com `NameError` porque `_assert_single_system_property` ainda não existia.
- GREEN: foi adicionado um helper stdlib baseado em `shlex.split`; ele exige exatamente um token `-Dspark.history.fs.logDirectory=` igual ao caminho esperado e rejeita drift/duplicatas. O teste focado terminou com `5 passed`.
- Regressão: `make tests` terminou com `17 passed in 0.05s`; `git diff --check` terminou com exit `0`.
- Nenhum arquivo de build/script foi alterado, portanto nenhuma repetição Docker foi necessária.

#### Re-review independente final

| Critério | Resultado |
| --- | --- |
| Spec compliant | Yes |
| Quality | Ready |
| Critical | `0` |
| Important | `0` |
| Minor | `0` |
| Actionable issues | nenhum |

Task 2 está `READY — aguardando ACEITO do usuário`. Nenhum commit, stage ou push foi realizado, e a Task 3 permanece não iniciada.
