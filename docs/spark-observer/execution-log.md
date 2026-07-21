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
**Status:** `PASS — ACEITO`

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

- `docs/spark-observer/evidence/task-02/task-02-reproducible-verification.txt`;
- `docs/spark-observer/evidence/task-02/task-02-reproducible-verification.png`;
- `docs/spark-observer/evidence/task-02/task-02-tests-terminal.txt`;
- `docs/spark-observer/evidence/task-02/task-02-tests-terminal.png`;
- `docs/spark-observer/evidence/task-02/task-02-jar-terminal.txt`;
- `docs/spark-observer/evidence/task-02/task-02-jar-terminal.png`.

O gate primário foi capturado diretamente por `script -qefc 'make observer-verify'`; seu header contém `COMMAND="make observer-verify"` e não chama wrapper ignorado. Os arquivos `.txt` foram capturados em sessões PTY reais e terminam com `COMMAND_EXIT_CODE="0"`. As imagens foram produzidas com Playwright a partir desses transcripts, sem reescrever o output. Elas mostram os testes Scala/Node/Python, a validação, o build, a listagem do JAR, sua verificação de conteúdo, tamanho e SHA-256.

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

**Estado final daquela execução:** `READY — aguardando ACEITO do usuário`, sem commit, stage ou push. Naquele momento isso ainda não constituía `PASS`/`ACEITO`, e a Task 3 não havia sido iniciada.

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

Naquele gate, a Task 2 estava `READY — aguardando ACEITO do usuário`. O checkpoint foi posteriormente publicado como `967035b`; a Task 3 permaneceu não iniciada.

#### Gate reproduzível solicitado antes do ACEITO

O review externo apontou que as primeiras capturas chamavam helpers em `.superpowers/sdd/`, ignorados pelo Git. O hardening começou sobre o HEAD já publicado `967035baf78d7c199786571693ec3219cd5598b4`. O script, target, teste e adendo do plano foram então versionados no commit `4ed06277a2b94a486dd5b42c78abfae930403534`; a captura primária foi executada diretamente nesse commit.

**RED adicional:** `uv run pytest tests/test_observer_platform_contract.py -q` terminou com exit `1`; `5` testes passaram e o novo contrato falhou exclusivamente porque `build/scripts/verify-observer-toolchain.sh` ainda não existia.

**GREEN adicional:** foram adicionados:

- `build/scripts/verify-observer-toolchain.sh`;
- target `make observer-verify`;
- contrato dos valores completos de `SBT_IMAGE` e `NODE_IMAGE`;
- contrato estrutural da sequência, inspeção de imagens/JAR e exit code final.

O teste focado terminou com `6 passed`.

**Captura direta:**

```bash
script -qefc 'make observer-verify' \
  docs/spark-observer/evidence/task-02/task-02-reproducible-verification.txt
```

Resultados observados:

| Campo | Evidência |
| --- | --- |
| Commit impresso | `4ed06277a2b94a486dd5b42c78abfae930403534` |
| Imagem sbt ID/digest | `sha256:16b0af1a3fddcd4cbf14731c2695876354f96ee4ca43e6252580c23282166849` |
| Imagem Node ID/digest | `sha256:a81a03dd965b4052269a57fac857004022b522a4bf06e7a739e25e18bce45af2` |
| Scala | `4/4` |
| Node | `1/1` |
| Python | `18/18` |
| JAR | `2522` bytes; SHA-256 `ba3e358661a8e707eb238c5a00cfcdcd60bb02c14d44d3485c123da97630019c`; sem classes Spark/Scala |
| Validação | `Validation passed` |
| Exit do verifier | `observer_verify_exit_code=0` |
| Exit do transcript | `COMMAND_EXIT_CODE="0"` |

O header do transcript contém `COMMAND="make observer-verify"`. O script força as imagens fixadas nos targets Observer, mesmo se `.env` definir outros valores.

Não existe transcript bruto do RED original por ausência de `BuildInfo`; ele não foi recriado artificialmente removendo código.

**Estrutura dos checkpoints:** `4ed0627` contém o gate reproduzível versionado. O checkpoint documental seguinte conserva o transcript e a captura gerados diretamente sobre esse commit. A Task 3 permanece não iniciada.

### Aceite do usuário

- Resultado: `PASS — ACEITO`.
- A Task 2 foi encerrada pelo usuário em 2026-07-16 após o gate reproduzível e a re-review independente sem findings.
- O aceite da Task 2 não autorizou automaticamente a Task 3; o usuário autorizou a Task 3 separadamente em 2026-07-16.

## Task 3 — produzir o JAR mínimo carregável e opt-in

**Data:** 2026-07-16
**Branch:** `exp-dataflint-based-test-jar`
**HEAD inicial:** `c4d014caa55c935596b323ede6f66736594cb708`
**Status:** `PASS — ACEITO`

**Hipótese:** Spark carrega o plugin somente com as duas flags opt-in, cria
apenas o componente de driver, não cria componente de executor e preserva o
resultado do workload existente.

### RED válido

| Comando | Exit | Falha observada | Por que é válido |
| --- | ---: | --- | --- |
| `make observer-tests` | 2 | Sete erros `not found` para `ObserverConfig`, `SparkDataShipPlugin` e `SparkDataShipDriverPlugin`. | Docker, sbt, Java, projeto e runner carregaram; somente as classes sob teste estavam ausentes. |
| `uv run pytest tests/test_spark_runtime_readiness.py -q` | 2 | `ModuleNotFoundError: No module named 'build.scripts.wait_spark_runtime_ready'`. | Pytest coletou o arquivo focado e falhou exclusivamente porque o helper ainda não existia. |

A primeira tentativa confinada de `make observer-tests` não foi contada como
RED porque o sandbox bloqueou o socket Docker. O transcript válido foi
capturado ao repetir o mesmo target com acesso autorizado, antes de criar as
classes de produção.

### GREEN, runtime e workloads

| Gate | Comando | Exit | Resultado |
| --- | --- | ---: | --- |
| Readiness unitário | `uv run pytest tests/test_spark_runtime_readiness.py -q` | 0 | `4 passed`; master indisponível, sem worker, worker `ALIVE` e timeout determinístico sem espera real. |
| Scala focado | `make observer-tests` | 0 | `9/9`; driver novo, executor nulo e configuração enabled/default false. |
| Refresh | `make observer-runtime-refresh` | 0 | JAR recompilado e staged, imagem Spark reconstruída, containers antigos removidos, master/worker recriados; depois `Spark Master UI responded HTTP 200` e `1 registered worker(s) ALIVE`. |
| Checksum | host + `spark-master:/opt/spark/jars/...` | 0 | Ambos `6cfc48c8943666de90bd134bec0bda4a1f4bb84202472c5c4cc98a1d4a27cefa`. |
| Compose | `make compose` | 0 | Validação e readiness existentes passaram. |
| Plugin off | `make smoke` | 0 | `Smoke validation passed`; sanity `landing_rows=5 bronze_rows=5 country_groups=2`. |
| Plugin on | `check_sanity.py` com as duas flags | 0 | Spark registrou o componente do driver; sanity idêntico `5/5/2`. |
| Ausência de rota | mapping `4040` + `curl 24040/dataship/` | 0 no wrapper | Exits internos esperados `1` e `7`; nenhuma rota/porta DataShip. |
| Regressões | `make tests && make validate && make observer-ui-tests` | 0 | `22 passed`, `Validation passed`, Node `1/1`. |
| Verifier aceito | `make observer-verify` | 0 | Scala `9/9`, Node `1/1`, Python `22/22`, JAR sem classes Spark/Scala, validate PASS. |

### Evidência

- relatório humano: `docs/spark-observer/evidence/task-03/bootstrap-report.md`;
- Master UI: `spark-master-plugin-bootstrap.png`;
- JSON que sustenta a captura: `spark-master-plugin-bootstrap.json`;
- transcripts RED/GREEN: `task-03-scala-red.txt`,
  `task-03-scala-green.txt`, `task-03-python-red.txt`,
  `task-03-python-green.txt`;
- runtime/checksum: `task-03-runtime-refresh.txt`,
  `task-03-jar-checksum.txt`;
- workloads: `task-03-plugin-off-smoke.txt`,
  `task-03-plugin-on-sanity.txt`;
- regressões e ausência: `task-03-regressions.txt`,
  `task-03-observer-verify.txt`, `task-03-route-absence.txt`.

### Escopo e risco restante

- O plugin contém somente bootstrap e parsing da flag enabled.
- `init` faz apenas parsing local e retorna mapa vazio; não há I/O externo.
- Não existem thread, listener, endpoint, aba, executor plugin ou adapter de
  internals nesta task.
- Nenhum arquivo protegido do caminho durável foi alterado.
- Task 4 não foi iniciada: não há publicação de `4040`, probe live ou harness.
- O estado é `READY`; nenhum commit, stage, push ou aceite foi executado.

### Correções pós-review — timeout estrito e gate reproduzível

O review encontrou dois pontos `Important`, ambos corrigidos sem ampliar o
escopo da Task 3.

#### Deadline estrito da readiness

Foi adicionado um teste determinístico no qual a consulta começa antes do
deadline, termina depois dele e devolve um worker `ALIVE`.

- RED: `uv run pytest tests/test_spark_runtime_readiness.py -q` terminou com
  exit `1`; `4` testes passaram e o novo teste falhou porque o helper retornou
  `True` após o deadline.
- Correção mínima: o relógio é consultado imediatamente depois de
  `fetch_status`; o timeout é aplicado antes de aceitar uma resposta ready.
- GREEN: o mesmo comando terminou com exit `0` e `5 passed`.

Evidências:

- `task-03-timeout-review-red.txt`;
- `task-03-timeout-review-green.txt`.

#### Verifier versionado e vinculado à identidade

Foi adicionado `build/scripts/verify-observer-bootstrap.sh`, exposto por
`make observer-bootstrap-verify`. O script:

- calcula um fingerprint de uma allowlist explícita de `11` arquivos Task 3;
- exclui `.env`, arquivos ignorados, relatórios e evidência gerada;
- imprime commit e fingerprint antes dos gates;
- executa, em ordem, readiness, Scala, refresh, identidade da imagem,
  igualdade do checksum do JAR, compose, plugin off, plugin on, ausência de
  mapping/rota, regressões, validação e verifiers existentes;
- repete commit e fingerprint ao final e exige igualdade;
- emite status e exit code finais explícitos.

O contrato foi conduzido por TDD:

- RED comportamental: o target e o esqueleto já existiam, mas o teste focado
  terminou com exit `1` porque a allowlist de fingerprint ainda estava
  ausente;
- GREEN: `uv run pytest tests/test_observer_platform_contract.py -q`
  terminou com exit `0` e `7 passed`.

Evidências:

- `task-03-verifier-contract-red.txt`;
- `task-03-verifier-contract-green.txt`.

#### Gate autoritativo

Captura direta:

```bash
script -qefc 'make observer-bootstrap-verify' \
  docs/spark-observer/evidence/task-03/task-03-bootstrap-verification.txt
```

| Campo | Resultado |
| --- | --- |
| Commit inicial/final | `c4d014caa55c935596b323ede6f66736594cb708` |
| Fingerprint inicial/final | `50e5a70b92511257f7d22525ef1100c77617b42f0db41112c606adb80b739e1d` |
| Imagem runtime ID/digest | `sha256:ba58541d76f336354e6fadcf2c9dd6adcc2ef1c984b6ec15bea8b83700e48cff` |
| Readiness | `5/5` |
| Scala | `9/9` |
| Python | `24/24` |
| Node | `1/1` |
| Workloads off/on | ambos `landing_rows=5 bronze_rows=5 country_groups=2` |
| Host/container JAR | SHA-256 idêntico `6cfc48c8943666de90bd134bec0bda4a1f4bb84202472c5c4cc98a1d4a27cefa` |
| Mapping/rota | exits internos esperados `1` e `7` |
| Status | `observer_bootstrap_verify_status=PASS` |
| Exits finais | verifier `0`; transcript `COMMAND_EXIT_CODE="0"` |

O JSON e a captura da Spark Master foram regenerados depois desse gate. Eles
mostram um worker `ALIVE` e quatro aplicações concluídas, incluindo as duas
execuções sanity usadas na comparação plugin off/on.

Os transcripts separados de runtime, checksum e workloads continuam
preservados como histórico da implementação, mas o transcript autoritativo
acima os substitui como evidência de aceite da Task 3.

#### Verificação independente do controlador

Depois da re-review aprovada, o controlador repetiu diretamente o mesmo gate:

```bash
script -qefc 'make observer-bootstrap-verify' \
  docs/spark-observer/evidence/task-03/task-03-controller-verification.txt
```

O segundo transcript confirmou novamente:

- commit inicial/final `c4d014caa55c935596b323ede6f66736594cb708`;
- fingerprint inicial/final
  `50e5a70b92511257f7d22525ef1100c77617b42f0db41112c606adb80b739e1d`;
- readiness `5/5`, Scala `9/9`, Python `24/24` e Node `1/1`;
- workloads off/on com resultado idêntico `5/5/2`;
- inicialização do componente de driver somente no modo opt-in;
- checksum idêntico do JAR no host e no `spark-master`;
- ausência esperada do mapping `4040` e da rota `/dataship/`;
- `observer_bootstrap_verify_status=PASS`;
- verifier e transcript com exit code `0`.

A captura e o JSON da Spark Master foram regenerados depois da repetição do
controlador e mostram um worker `ALIVE` e quatro aplicações concluídas.

#### Escopo final e risco menor conhecido

- Foi adicionada uma regra `.gitattributes` limitada aos transcripts de
  evidência `task-*/*.txt`. Ela preserva o output bruto de PTY/Docker e
  desabilita somente o detector de whitespace para esses arquivos; código e
  documentação continuam cobertos por `git diff --check`.
- Nenhum caminho protegido foi alterado.
- Nenhum arquivo reservado à Task 4 foi alterado.
- Não foi adicionado teste unitário que invoque diretamente
  `SparkDataShipDriverPlugin.init`; esse finding `Minor` permanece registrado.
  O caminho de `init` está coberto pela inicialização live observada no log do
  Spark e por inspeção do código, mas não por chamada unitária direta.
- O estado passou para `PASS — ACEITO` após autorização explícita do usuário
  em 2026-07-16.

### Aceite do usuário

- Resultado: `PASS — ACEITO`.
- O usuário autorizou o commit e o push da Task 3 para validação externa.
- O aceite e a publicação desta task não autorizam o início da Task 4.

## Task 4 — workload e harness determinísticos para prova live

**Data de execução:** 2026-07-16
**Estado:** `PASS — ACEITO` — aceite explícito do usuário em 2026-07-16
**HEAD das evidências principais:** `f2fa08dc2151fc3058a33d6d23ae05ce39929dfd`
**Checkpoint publicado da Task 4:** `db7b9a2885d8d6ab28c8ce4e58ee4a749a4aff9b`

Nenhum código da Task 5 foi executado.

### Implementação

- `src/apps/observer_live_probe.py` fornece defaults `40/4/75/10`, rejeita
  negativos, mantém imports PySpark dentro de `main`, executa uma ação RDD
  atrasada e uma ação SQL com shuffle, e verifica resultado determinístico
  `rowCount=40`, `valueSum=780`, buckets `180/190/200/210`.
- `build/scripts/run-observer-live-probe.sh` cria `OBSERVER_RUN_ID` único,
  identifica o Java `SparkSubmit` real dentro de `spark-master`, força
  `spark.ui.port=4040` e `spark.port.maxRetries=0`, usa timeout explícito,
  traps `EXIT/TERM/INT`, propaga o exit do submit e mantém temporários sob
  `/tmp`.
- `observer-live` é o target primário versionado.
- O Compose publica somente
  `127.0.0.1:${SPARK_DRIVER_UI_PORT:-24040}:4040` no `spark-master`.
- Ao ganhar o mapping, o `observer-runtime-refresh` existente removeu os
  containers Spark antigos, confirmou `24040` livre antes da recriação e
  aguardou Master HTTP `200` com um worker `ALIVE`. O harness apenas verifica
  o mapping persistente e não repete esse preflight.

### RED e GREEN

| Gate | Resultado |
| --- | --- |
| Python RED | `9` falhas válidas: módulo, mapping e harness/target ausentes |
| Compose RED | lookup filtrado terminou `1`; publicação `4040` ausente |
| Redirect RED | `8 passed, 1 failed`; faltava seguir redirect nativo da raiz |
| Rota nativa RED | `8 passed, 1 failed`; faltava prova honesta de ausência |
| Python GREEN final | `9 passed` |
| Compose GREEN | `host_ip=127.0.0.1`, target `4040`, published `24040`; `port` retorna `127.0.0.1:24040` |
| Runtime refresh | exit `0`; porta livre, containers recriados, `1` worker `ALIVE` |

Evidências principais: `task-04-python-red.txt`,
`task-04-compose-red.txt`, `task-04-root-redirect-red.txt`,
`task-04-native-route-red.txt`, `task-04-python-green.txt`,
`task-04-compose-green.txt` e `task-04-runtime-refresh.txt`.

### Timeline live e cleanup

| Modo | Run/PID | Leituras live | Resultado | Cleanup |
| --- | --- | --- | --- | --- |
| plugin off | `observer-live-20260716T194351Z-272105-13791` / `3528` | `200`, `200`, mesmo PID vivo | exit `0`; `40/780`; jobs `0`–`4` | PID ausente, HTTP indisponível, mapping `127.0.0.1:24040` persiste |
| plugin on | `observer-live-20260716T194435Z-273727-1865` / `3903` | `200`, `200`, mesmo PID vivo | plugin inicializado; exit `0`; resultado idêntico | PID ausente, HTTP indisponível, mapping persiste |
| timeout direto | `observer-live-20260716T195336Z-287384-18652` / `5802` | `200`, `200`, mesmo PID vivo | harness retorna `124` | PID ausente, HTTP indisponível, mapping persiste |
| reutilização | `observer-live-20260716T194814Z-280115-18624` / `4986` | `200`, `200`, mesmo PID vivo | exit `0` após a falha induzida | cleanup completo; mesmo mapping |

Os logs nativos mostram duas ações explícitas: job `0` para `sum` e a ação
SQL `collect`, que gera jobs `1`–`4` adicionais do Spark/AQE. Também mostram
`ShuffleMapStage`, múltiplos stages e a execução SQL. Os jobs internos extras
são esperados.

Transcripts autoritativos: `task-04-plugin-disabled.txt`,
`task-04-plugin-enabled.txt`, `task-04-direct-timeout-status.txt`,
`task-04-deliberate-timeout.txt`, `task-04-mapping-reuse.txt` e
`task-04-mode-comparison.txt`.

### Discrepância observada: `/dataship/` nativo

O plano previa `404`, mas o Spark `4.1.2` redireciona qualquer path UI
desconhecido para `/jobs/`. Com o Java PID `3137` vivo,
`task-04-native-route-diagnostic.txt` mostrou comportamento idêntico para:

- `/dataship/`;
- `/dataship/api/v1/health`;
- `/observer-random-unknown/`.

Todos retornaram `302 Location: http://127.0.0.1:24040/jobs/` e depois
`200` final em `/jobs/`. Para não fabricar um `404`, o harness prova ausência
por equivalência exata entre os dois paths DataShip e um path aleatório
único. Nenhuma rota, servlet, listener ou UI DataShip foi implementada.

A raiz nativa também passa a responder `302` depois que a UI completa é
anexada; `task-04-root-redirect-diagnostic.txt` prova `302` sem follow e
`200` final com follow. Por isso as duas leituras live registram status final
`200`.

### Evidência visual

`task-04-playwright-capture.txt` prova que o Java PID `5390` continuava vivo
depois das três capturas, com HTTP `200` nas UIs de driver e Master:

- `spark-driver-jobs-live.png`: UI nativa do driver em `24040`, cinco jobs;
- `spark-driver-sql-live.png`: SQL/DataFrame, execução ligada aos jobs
  `1`–`4`;
- `spark-master-live.png`: UI distinta em `28081`, worker `ALIVE` e a mesma
  aplicação nomeada em estado `RUNNING`.

O run correlacionado está em `task-04-screenshot-correlation-run.txt` e
termina com exit `0`, PID ausente, endpoint indisponível e mapping
persistente.

### Regressões e escopo

Captura direta:

```bash
script -qefc 'make observer-verify' \
  docs/spark-observer/evidence/task-04/task-04-regressions.txt
```

Resultados: Scala `9/9`, Node `1/1`, Python `33/33`, JAR sem classes
Spark/Scala, `Validation passed` e verifier exit `0`.

- Nenhum caminho protegido do fluxo durável foi alterado.
- Nenhum source JVM do Spark Observer foi alterado.
- Nenhum endpoint/listener/tab da Task 5+ foi criado.
- Relatório detalhado:
  `docs/spark-observer/evidence/task-04/task-04-report.md`.
- Risco conhecido: o literal `404` não existe para paths UI desconhecidos no
  runtime testado; a evidência substituta por equivalência está explicitada.
- Estado final: `READY`; ainda não `PASS — ACEITO`.

### Correções após review independente

O review encontrou dois problemas importantes no lifecycle do harness e dois
pontos menores de validação/teste. A correção permaneceu restrita à Task 4:

- quando nenhum Java `SparkSubmit` é descoberto, um submit `0` agora vira
  falha do harness `1`, pois não houve prova live; um submit não zero continua
  sendo propagado sem normalização;
- o cleanup não depende mais de `DRIVER_PID`: ele lê um snapshot
  `ps pid/ppid/pgid/comm/args` sem passar o run ID ao processo de inspeção,
  seleciona somente `comm=timeout` com o ID único e Java
  `org.apache.spark.deploy.SparkSubmit` com o mesmo ID, encerra o grupo do
  wrapper e verifica a ausência dos dois papéis;
- a seleção ignora comandos de inspeção que apenas contêm os mesmos tokens,
  pois exige o `comm` exato;
- todos os spellings numéricos de zero são rejeitados, incluindo `00`,
  `0.00` e `000.000`;
- os testes agora executam o fluxo real do harness sobre uma fronteira
  Docker/curl fake, em vez de apenas procurar tokens no arquivo.

Novo RED focado:

```bash
script -qefc 'uv run pytest tests/test_observer_live_probe.py -q' \
  docs/spark-observer/evidence/task-04/task-04-review-fixes-red.txt
```

Resultado: exit `1`, cinco falhas comportamentais esperadas. O transcript
mostra submit `0` retornando `0`, os três spellings adicionais de zero sendo
aceitos e o wrapper ainda vivo após TERM antes da descoberta do driver. O
erro inicial de cache uv do sandbox foi sobrescrito e não conta como RED.

GREEN focado e sintaxe:

```bash
script -qefc \
  'uv run pytest tests/test_observer_live_probe.py -q &&
   bash -n build/scripts/run-observer-live-probe.sh' \
  docs/spark-observer/evidence/task-04/task-04-review-fixes-green.txt
```

Resultado: `17` testes focados, exit `0`; `bash -n` também retorna `0`.

A prova real de interrupção usa o comando versionado de evidência
`task-04-early-term-command.sh`. O transcript
`task-04-early-term-cleanup.txt` registra:

- run `observer-live-20260716T202125Z-313416-24704`;
- harness congelado durante descoberta, antes de qualquer
  `spark_driver_pid=`;
- wrapper `6620` e Java no mesmo PGID `6620`;
- TERM seguido de exit `143`;
- marcadores de ausência do wrapper e do Java;
- nenhuma linha restante do run ID no snapshot de processos;
- UI indisponível e mapping persistente `127.0.0.1:24040`.

Verificações proporcionais adicionais:

- `task-04-review-fixes-plugin-enabled.txt`: plugin inicializado, submit e
  harness `0`, Java/wrapper ausentes, UI indisponível e mapping persistente;
- `task-04-review-fixes-direct-timeout.txt`: submit e harness `124`, mesma
  prova de cleanup;
- `task-04-review-fixes-regressions.txt`: Scala `9/9`, Node `1/1`, Python
  `41/41`, JAR sem classes Spark/Scala, `Validation passed` e verifier `0`.

Nenhum screenshot foi regenerado: a superfície visual, o workload e a
correlação visual existente não mudaram. O estado continua
`READY — awaiting user acceptance`.

### Verificação independente do controlador e re-review

O controlador repetiu os dois gates mais sensíveis depois das correções:

- `task-04-controller-early-term.txt`: o harness foi congelado antes de
  registrar `spark_driver_pid=`, o container mostrou wrapper e Java no PGID
  `7409`, TERM produziu exit `143`, ambos os processos desapareceram, a UI
  ficou indisponível e o mapping permaneceu `127.0.0.1:24040`;
- `task-04-controller-final-regressions.txt`: Scala `9/9`, Node `1/1`,
  Python `41/41`, JAR sem classes Spark/Scala, `Validation passed`,
  `observer_verify_exit_code=0` e transcript com exit `0`.

O teste focado também foi repetido diretamente pelo controlador e terminou
com `17 passed`; os dois scripts shell da task passaram `bash -n` e
`git diff --check` permaneceu limpo.

A re-review independente final retornou:

- spec compliance: aprovado;
- Critical: `0`;
- Important: `0`;
- Minor: `0`;
- divergência `/dataship/` → `/jobs/`: aceita como prova honesta de ausência
  por equivalência com uma rota aleatória desconhecida.

### Aceite do usuário

- Em 2026-07-16, após a validação externa do checkpoint remoto `db7b9a2`, o
  usuário declarou a Task 4 aceita.
- Resultado final: `PASS — ACEITO`.
- As duas ressalvas não bloqueantes do review — dependência documentada de
  `.env` em checkout novo e transcripts capturados antes do commit final —
  foram preservadas como aprendizados para as próximas tasks no `AGENTS.md`.
- O aceite da Task 4 não autoriza iniciar a Task 5; ela continua aguardando um
  novo pedido explícito do usuário.

## Task 5 — lifecycle and health endpoint

**Task:** Task 5 — implement lifecycle and the health endpoint

**Hypothesis:** after `registerMetrics` receives a non-empty `appId`, the
driver plugin attaches `/dataship/api/v1/health` to the supported Spark UI and
returns `READY` while the identified Spark submit process remains alive.

**Minimum change:** add immutable lifecycle state, an allowlisted health DTO
and JSON renderer, one idempotently installed Spark 4.1.2 UI handler, strict
response assertions, and bounded live-harness polling.

**Red test:** focused Scala and Python tests must fail because lifecycle,
serialization, response validation, and handler installation are absent; the
live harness must prove the health resource is still absent under Spark's
native unknown-route behavior before implementation.

**Expected observable result:** two HTTP `200` JSON responses from the same
live driver show the stable envelope and `READY`; a Playwright capture shows
the allowlisted payload; shutdown removes the endpoint and process; a second
run reuses the persistent mapping; the no-UI and disabled modes fail open.

**Regression:** `make observer-tests`, the focused Python modules, `make
tests`, `make observer-runtime-refresh`, the Task 5 live scenarios, protected
path guards, and `git diff --check`.

**Status:** `PASS — ACCEPTED` — the user explicitly accepted Task 5 on
2026-07-20 after the missing historical Task-5-specific live RED capture and
its Task 4 substitute evidence had been disclosed.

### Start state

- User authorization: explicit `go task 5` on 2026-07-16.
- Branch: `exp-dataflint-based-test-jar`.
- Starting `HEAD`: `43b4541b284ba054a8a507fe73dc970a21d28c15`.
- Upstream matched the starting `HEAD` and the working tree was clean.
- Protected durable-path diff was empty.
- Baseline `make observer-tests`: exit `0`, Scala `9/9`.
- Baseline `make tests`: exit `0`, Python `41/41`.
- All Task 5 source, tests, messages, documentation, evidence labels, and
  visual artifacts will be written in English.
- No Task 6 implementation is authorized.

### Implementation and RED/GREEN evidence

- `task-05-scala-red.txt`: `make observer-tests` exited `2` with 25 expected
  errors for the absent queue configuration, runtime, health DTO, and
  installer lifecycle.
- `task-05-python-red.txt`: the 17 existing harness tests passed and all 21
  new response-contract tests failed as expected before the validator and
  health reads existed.
- The starting implementation's endpoint absence is retained in
  `evidence/task-04/task-04-plugin-enabled.txt`: the future health path and a
  random unknown route both used Spark's native `302` redirect to `/jobs/`.
- `task-05-final-scala.txt`: 18/18 Scala tests passed.
- `task-05-final-focused-python.txt`: 38/38 focused Python tests passed.
- `task-05-final-python-regression.txt`: `make tests` passed 62/62.

The implementation validates queue capacity from 1 through 65536 with a
default of 1024. `init` retains configuration and `SparkContext` without
installing HTTP. `registerMetrics` installs the health resource at most once,
after the application ID exists. The response is serialized from exactly 12
allowlisted fields with Spark-provided Jackson. Shutdown moves the runtime to
`STOPPING` and closes the resource once. `listenerInstalled` stays `false` and
no listener, snapshot, or tab was added.

### Live and visual evidence

- `task-05-runtime-refresh.txt` records a successful runtime refresh.
- `task-05-jar-checksum.txt` proves the host, staged, and live JARs all used
  SHA-256
  `019ae046ca12277e0f8682b839521f11be4ce5f39cda34250391d6ec9a433b4a`.
- `task-05-live-ready-first-run.txt` records two HTTP `200`
  `application/json` READY documents from live Java PID `136` and application
  ID `app-20260716225513-0000`, followed by submit exit `0`, endpoint removal,
  absent driver/wrapper, and persistent mapping retention.
- `task-05-live-ready-mapping-reuse.txt` repeats the proof with PID `517` and
  application ID `app-20260716225556-0001` on the same mapping.
- `task-05-live-disabled.txt` records two `DISABLED` health documents with no
  listener and successful cleanup.
- `task-05-live-no-spark-ui.txt` records no endpoint promise, stable error
  code `NO_SPARK_UI`, workload and submit exit `0`, cleanup, and retained
  mapping when `spark.ui.enabled=false`.
- `spark-observer-health-ready.png` is the Playwright capture of the exact
  live health JSON. `task-05-playwright-capture.txt` records SHA-256
  `bb777ab4b69f89009cb1211a54c8a7f2a476cde157b95675f8438d94f836903c`
  and confirms Java PID `1596` was alive. The correlated
  `task-05-playwright-live-run.txt` contains the same visible application ID,
  two READY reads, exit `0`, endpoint removal, and process cleanup.

### Final guards and remaining risk

Python compilation, shell syntax, `git diff --check`, and the protected
durable-path diff all passed. The detailed English evidence report is
`docs/spark-observer/evidence/task-05/task-05-report.md`.

The version-pinned `v412` adapter invokes Spark's exact runtime handler
methods reflectively because the published Spark Scala metadata exposes an
unshaded Jetty type that is absent from the Spark POM. The runtime signatures
were checked with `javap` and exercised by the live proofs. This remains an
explicit compatibility boundary for a future Spark upgrade.

No commit was created. Task 6 was not started.

### Final rereview lifecycle fix (2026-07-17)

The final rereview found that invalid application-ID callbacks could overwrite
an unrelated error and could degrade an already READY runtime. Two tests were
added before production code changed:

- `INVALID_CONFIG -> blank -> null -> valid -> UI attached` must retain
  `INVALID_CONFIG` with DEGRADED status; and
- `READY -> blank -> null` must retain the established application ID, READY
  status, and empty error.

The focused RED transcript records `make observer-tests` exiting `2`: 23 tests
ran, 21 passed, and both new tests failed for the expected behavior. The first
observed READY instead of DEGRADED, and the second observed DEGRADED instead of
READY.

The minimal invalid-ID branch now creates `EMPTY_APP_ID` only when no
application ID and no prior error exist. Other invalid callbacks return
`false` without mutating runtime state. A later valid callback still clears
only a genuinely transient `EMPTY_APP_ID`.

Focused GREEN records Scala 23/23. The regression transcript records Scala
23/23, focused Python 39/39, full Python 63/63, Python compilation, and shell
syntax, all with exit `0`.

The final runtime refresh exited `0` with one ALIVE worker. Host, staged, and
live JARs all use SHA-256
`5334786124080256cf83e959f90776c575e612a56d20f6462533dc7042bf3544`.
Every final-rereview live run records starting `HEAD`
`43b4541b284ba054a8a507fe73dc970a21d28c15`, master and worker image ID
`sha256:2a79ca1478cf0e54e0156e8d06ddce2d835b3f6b4c00b6b8f3a765039b58e728`,
and matching start/end fingerprint
`b43cf8f7399029ae4a32a69a558fa29e0fe108c94e605ab0d0eadc0f72a722be`.

The two READY runs, DISABLED run, no-Spark-UI run, and held visual run all
exited `0`, completed their workloads, cleaned up driver and wrapper processes,
and retained the persistent mapping. The visual capture shows READY application
`app-20260716234710-0004` while PID `1547` was alive before and after capture.
`spark-observer-health-ready-final-rereview.png` was visually inspected and
has SHA-256
`792503de400010cdcd1f4f56b08f28e847d8cea02a7340aa81f3e3f29ad1e268`.

The final guard transcript records unchanged starting `HEAD`, an empty index,
`git diff --check`, Python compilation, shell syntax, protected-path and
changed-path allowlists, secret-pattern filename checks, English-only changed
content, and source trailing-whitespace checks all passing.

The explicit historical Task-5-specific live RED waiver remains unchanged and
is the only acceptance concern. No commit was created, nothing was staged, and
Task 6 was not started.

### Post-review fixes (2026-07-17)

The review findings were first reproduced in
`evidence/task-05/task-05-review-fixes-red.txt`. Scala failed because the
runtime registration method did not report whether the application ID was
valid and because the package-private runtime seam was absent. Python failed
because the live harness had no explicit non-secret input-fingerprint
contract. The separate sandbox-attempt transcript contains only Docker socket
and `uv` cache access failures and is not counted as behavioral RED.

The fix validates the application ID before consuming the endpoint-install
attempt. Blank and null values perform zero installer factory calls and zero
install calls; a later valid ID installs exactly once and reports READY with a
non-empty application ID. Only transient `EMPTY_APP_ID` is cleared. Unrelated
failures remain degraded, installer-factory exceptions fail open and preserve
safe shutdown, and unsupported runtimes do not invoke the installer factory or
claim support. The runtime-construction seam is package-private.

Focused GREEN evidence records Scala 22/22 and response assertions 22/22. The
post-review regression transcript records Scala 22/22, focused Python 39/39,
and full Python 63/63, plus successful Python compilation and shell syntax.

After those source and harness changes, `make observer-runtime-refresh`
rebuilt the runtime. Host, staged, and live JAR SHA-256 values all matched
`a3f83d6b9df46caf34a337ca71a87f18468d08f690363a1933d0ebd0cf483c76`.
Every successful post-review live run recorded starting `HEAD`
`43b4541b284ba054a8a507fe73dc970a21d28c15`, master and worker image ID
`sha256:e0631eb6ff27614641e88a741dd8b5baa7f8b4eb84c8602d8a763d4962fe2bc9`,
and identical start/end ordered-input fingerprint
`2e7d50ed3e6931e61286af4bfdbe57305d46bd98e47ff1c2cd3c944e6c018857`.
The allowlist excludes `.env`, evidence, generated outputs, caches, and
secret-prone inputs.

Post-review live evidence covers two READY runs on the persistent mapping,
DISABLED, and no-Spark-UI, each with successful workload, exit, cleanup, and
fingerprint validation. The post-review visual run captured application
`app-20260716232730-0004` while PID `1584` was alive. The screenshot
`spark-observer-health-ready-post-review.png` visibly contains the allowlisted
READY document and has SHA-256
`dfc0833b162b1956771a71ae561d0003765fb1236e363c59d2344031c8bb1775`.

The Spark 4.1.2 `javap` transcript independently confirms public
`SparkContext.ui`, `WebUI.attachHandler`, and `WebUI.detachHandler` signatures.
It supports but does not replace the successful live probes.

The final guard transcript records passing Python compilation, shell syntax,
`git diff --check`, protected durable paths, the Task 5 changed-path allowlist,
secret-pattern filenames, English-only changed content, and source trailing
whitespace checks.

Historical RED waiver: a Task-5-specific live harness RED was not captured
before the original implementation. Task 4 evidence truthfully captures the
exact starting implementation's endpoint-absence behavior, but it is
substituted evidence. Task 5 cannot be reported as fully compliant or accepted
unless the user explicitly waives this historical evidence gap.

No commit was created. Task 6 was not started.

### User acceptance (2026-07-20)

- The user explicitly approved Task 5 after reviewing the implementation,
  automated gates, live evidence, visual evidence, independent review, and
  the disclosed historical live-RED gap.
- This acceptance waives the missing Task-5-specific pre-implementation live
  RED transcript and accepts the Task 4 starting-state route evidence as the
  truthful substitute. No RED was recreated by removing working code.
- Final result: `PASS — ACCEPTED`.
- This acceptance does not authorize Task 6. A new explicit user request is
  still required before any Task 6 implementation begins.

## Task 6 — bounded internal handoff and consistent state

**Status:** `PASS — ACCEPTED`

**Hypothesis:** the internal handoff never waits for queue capacity,
deterministically accounts for refused events, and preserves
`listenerReceived = processed + queued + inFlight + droppedByPlugin` during
concurrent offers, snapshots, FIFO eviction, and repeated shutdown.

**Minimal change:** add task-owned event, counter, state, and bounded-queue
types; add the fixed transition-window capacity to configuration; and make the
runtime own and close the queue. Do not install a Spark listener or expose the
counters endpoint, which belong to Task 7.

**Focused RED:** run only `BoundedEventQueueSpec` and `ObserverStateSpec` after
creating them. They must fail to compile because the Task 6 event-state and
queue types do not exist; a missing runner, image, dependency, or Make target
is not a valid RED.

**Expected observable result:** with queue capacity `1`, the worker processing
the first event is held by a latch, the second event remains queued, and the
third `offer` returns promptly with exactly one plugin drop. Every captured
snapshot satisfies the invariant, the recent window evicts FIFO, two closes
are safe, and the worker terminates.

**Regression:** run the two focused specs fifty times, `make observer-tests`,
`make tests`, `make observer-runtime-refresh`, a host/staged/container JAR
checksum comparison, and the Task 5 live health probe using the rebuilt JAR.
Return the Compose stack to down after the live regression.

### Start state (2026-07-20)

- Branch: `exp-dataflint-based-test-jar`.
- Starting commit: `d8f0e31b6521f856c870e16d1c3e890307e7aae2`.
- Upstream matched the starting commit.
- Compose reported no containers and no repository-specific
  `spark-plat-v0-*` images were present.
- The only pre-existing working-tree change was this file's accepted Task 5
  status. It is intentionally preserved. This documented exception avoids an
  unauthorized standalone commit while keeping the Task 6 code baseline
  unchanged.
- Task 6 implementation allowlist: the eight planned Scala source/test paths,
  this execution log, and `docs/spark-observer/evidence/task-06/`.
- Frozen durable-path changes at start: none.

### Delegation decision and outcome

- Task 6 was suitable for a bounded implementation subagent because the queue,
  state, and focused specs were isolated from the live infrastructure.
- The implementation subagent produced no workspace change or usable report
  after two status checks, so it was interrupted. The root agent then executed
  the approved Task 6 scope directly.
- A separate review subagent inspected the completed diff and initially found
  two concrete accounting defects: null offers could increment `received`
  before `ArrayBlockingQueue` rejected the value, and shutdown could leave an
  accepted queued event permanently counted as queued after the worker exited.
- Both findings received focused regression tests before their fixes. The same
  reviewer re-reviewed the corrected implementation and reported no remaining
  critical, important, or minor finding.

### RED evidence

- Command: focused `testOnly` run for `BoundedEventQueueSpec` and
  `ObserverStateSpec` in the pinned sbt container.
- Result: exit `1`, with the test sources compiling far enough to fail on the
  deliberately absent Task 6 classes, runtime owner, and transition-capacity
  field. The runner, image, and dependencies were available, so this was a
  feature-level RED.
- Evidence: `docs/spark-observer/evidence/task-06/task-06-focused-red.txt`.
- The two independent-review regressions then produced a second valid RED:
  five tests passed and two failed for the exact null-offer and shutdown
  accounting defects described above.
- Evidence:
  `docs/spark-observer/evidence/task-06/task-06-review-fixes-red.txt`.

### Minimal implementation

- `ObserverEvent` is the immutable internal handoff item.
- `ObserverCounters` is an immutable snapshot and computes the accounting
  invariant from its five counters.
- `ObserverState` serializes queue and counter transitions under one monitor,
  takes atomic snapshots, and retains only a fixed FIFO window of completed
  events.
- `BoundedEventQueue` uses an `ArrayBlockingQueue`, nonwaiting `offer`, and one
  daemon worker. Rejected null/closed/full offers remain accounted, and close
  interrupts and joins the worker with a fixed five-second boundary before
  atomically reclassifying any remaining queued events as plugin drops.
- `ObserverConfig` now parses
  `spark.dataship.observer.transitions.capacity`, default `128`, accepted range
  `1..1024`.
- `ObserverRuntime` lazily owns the queue only while enabled and closes that
  owned resource outside its synchronized lifecycle section.
- No listener, listener-bus integration, counters endpoint, HTTP response
  change, or Task 7 behavior was added.

### GREEN evidence

- Initial focused GREEN: `5/5` tests passed.
- Review-fix GREEN: `7/7` tests passed.
- Fifty consecutive focused executions passed in one pinned sbt container:
  `50 × 7 = 350/350` tests, with no flaky failure.
- Each repetition reported a zero-millisecond refused offer in the deterministic
  capacity-one fixture.
- Each concurrency fixture received exactly `1,200` offers from six producers,
  kept the recent window at or below `8/8`, and ended with processed plus drops
  equal to received.
- Observed focused-suite duration range: `349..909 ms`.
- Evidence:
  `task-06-focused-green.txt`, `task-06-review-fixes-green.txt`, and
  `task-06-focused-50-repetitions.txt` under the Task 6 evidence directory.

### Runtime and regression evidence

- `make observer-tests`: `30/30` Scala tests passed.
- `make tests`: `63/63` Python tests passed.
- Fresh pre-commit reruns also passed with `30/30` Scala tests and `63/63`
  Python tests. Their raw transcripts are
  `task-06-final-observer-tests.txt` and
  `task-06-final-python-regression.txt`.
- The first `make observer-runtime-refresh` truthfully failed because the Task
  5 teardown had removed project-local MinIO images required by the Compose
  dependency graph. This was an environment prerequisite failure, not a queue
  failure.
- The documented fresh-checkout prerequisite `make build` recreated those
  local images without starting the stack. Re-running the exact runtime refresh
  then passed with the master ready and one ALIVE worker.
- Host, staged, master-container, and worker-container JAR SHA-256 all matched:
  `2c2ec3ea83efef09fdd0f2046a369dd28eb63d19d52f2a57b4757aac4d009ea6`.
- The rebuilt Task 5 live probe returned two HTTP `200` health responses from
  the same live driver PID `131`, both `READY`, with Spark `4.1.2`, plugin
  `0.1.0-SNAPSHOT`, and queue capacity `1024`.
- `listenerInstalled=false` remains intentional because listener installation
  belongs exclusively to Task 7. The Task 6 queue is therefore lazy during
  this health-only compatibility run.
- The live workload retained row count `40`, value sum `780`, bucket totals
  `180/190/200/210`, and submit exit `0`. Cleanup removed the driver and wrapper,
  and the endpoint became unavailable while the persistent mapping remained
  reusable.
- Evidence: all `task-06-runtime-*`, `task-06-jar-checksum.txt`,
  `task-06-live-health-regression.txt`, `task-06-observer-tests.txt`, and
  `task-06-python-regression.txt` files in the Task 6 evidence directory.
- `task-06-source-fingerprint.txt` binds the pre-commit evidence to starting
  HEAD `d8f0e31b6521f856c870e16d1c3e890307e7aae2`, the branch name, and each of
  the eight Task 6 source/test file hashes without including generated evidence
  or secret-prone local configuration.

### Visual evidence

- `task-06-queue-report.md` presents the actual deterministic counters,
  invariant equations, timing, stress results, runtime regression, and source
  evidence in one acceptance report.
- `task-06-queue-flow.svg` is the versioned source and
  `task-06-queue-flow.png` is its Playwright-rendered acceptance view.
- The visual explicitly distinguishes full-queue refusal, accepted-work
  completion, and terminal shutdown accounting. It also calls out that Task 7
  listener and HTTP counters are intentionally absent.

### Final environment state

- `make down` completed successfully.
- A direct `docker compose ps` verification returned only its empty header: no
  project service is running.
- Repository images remain locally built so the next explicitly approved task
  can reuse the documented prerequisite; image presence is not running
  infrastructure.
- Evidence: `task-06-infra-down.txt` and
  `task-06-infra-down-verification.txt`.

### Task result

- Status: `PASS — ACCEPTED` on 2026-07-20.
- Task 6 was committed as
  `12c4f8e4215036ff242b16750af81c6a15b8b540` and pushed to
  `origin/exp-dataflint-based-test-jar`; local and upstream identities matched
  after the push.
- Acceptance carried one nonblocking follow-up for Task 7/11: the worker
  currently handles `InterruptedException`, while another unexpected exception
  from `process(event)` could terminate it. This does not invalidate Task 6
  because its current processor is the no-op default. Before fail-open is
  considered complete, a future focused test must inject an unexpected
  processing exception, prove the worker continues serving later events, and
  prove the failure is exposed through `internalFailures`/`lastErrorCode`
  without changing the Spark workload result.
- Task 7 was not started. Acceptance of Task 6 does not authorize it; a new
  explicit user request is still required.

## Task 7 — dedicated listener and live counters endpoint

**Status:** `PASS — ACCEPTED`

**Hypothesis:** real Spark events enter the dedicated `dataship-observer`
listener-bus queue, the stable counters endpoint grows between two reads while
the same driver is alive, and deliberately induced plugin backpressure records
drops without changing the deterministic workload result.

**Minimal change:** add event classification, fixed per-category counters, the
versioned `/dataship/api/v1/debug/counters` response, listener installation in
the Spark 4.1.2 adapter, and live-probe assertions for growth and induced drop.
Do not add snapshot-store reads, SQL descriptions, a UI tab, or Task 8 work.

**Focused RED:** create `ObserverListenerSpec` and `CountersResponseSpec`, then
run only those specs and confirm they fail because the Task 7 classes and
runtime contract are absent. Extend the versioned live harness first and prove
the current rebuilt JAR cannot satisfy the counters route; a missing tool,
image, dependency, or target is not a valid RED.

**Expected observable result:** `t1` and `t2` come from the same live driver;
`listenerReceived(t2) > listenerReceived(t1)`, both snapshots satisfy the Task
6 invariant, and the capacity-one test-mode run has
`droppedByPlugin > 0`. Both normal and drop runs must finish with the same
deterministic result and Spark submit exit code `0`.

### Start state (2026-07-21)

- Branch: `exp-dataflint-based-test-jar`.
- Starting commit:
  `85efbf4f0b48edd119ce59615bff5b4ff8afef6c`.
- The branch was one local documentation commit ahead of upstream because the
  user requested add/commit but did not request push.
- Working tree was clean before Task 7 evidence was created.
- Compose reported an empty service table; the platform was down.
- Baseline `make observer-tests`: `30/30` passed.
- Baseline `make tests`: `63/63` passed.
- Baseline evidence:
  `docs/spark-observer/evidence/task-07/task-07-baseline-observer-tests.txt`
  and `task-07-baseline-python-tests.txt`.

### Execution decision and allowlist clarification

- Task 7 is being executed directly because listener installation, runtime
  ownership, response serialization, and the live harness form one dependent
  sequence with shared files and runtime state.
- The planned file list omitted three implementation necessities already
  implied by the Task 7 gates: parsing `testMode/processingDelayMs` for the
  deterministic drop scenario, extending the versioned response validator for
  the counters contract, and testing those changes. The Task 7 allowlist is
  therefore the planned source/test/harness files plus:
  `ObserverConfig.scala`, `ObserverConfigSpec.scala`, `JsonRenderer.scala`,
  `HealthResponseSpec.scala`, `assert-observer-response.py`,
  `tests/test_observer_response_assertions.py`, and
  `tests/test_observer_live_probe.py`.
- The per-category totals and `listenerReceived` must come from the same atomic
  snapshot. Keeping category counters only in `ObserverListener` would create a
  race between two independent snapshots, so the allowlist also includes the
  Task 6 state DTOs (`ObserverEvent.scala`, `ObserverCounters.scala`,
  `ObserverState.scala`) and their existing focused tests. Queue capacity and
  nonblocking behavior remain unchanged.
- Task-owned documentation is this execution log and
  `docs/spark-observer/evidence/task-07/`.
- Frozen durable-path changes at start: none.

### RED evidence

- The focused Scala RED failed compilation only on the absent Task 7
  contracts: `ObserverListener`, the counters response/runtime methods, and
  the deterministic test-mode configuration. Evidence:
  `task-07-focused-scala-red.txt`.
- The focused Python RED recorded 16 expected contract/harness failures and 36
  passes before the implementation existed. Evidence:
  `task-07-focused-python-red.txt`.
- After the versioned validator and harness were in place, the only remaining
  harness test failure was the intentional absence of the three production
  Task 7 files from the source fingerprint. Evidence:
  `task-07-harness-pre-live-red.txt`.
- The rebuilt pre-implementation JAR could not satisfy the counters contract:
  the requested path returned `text/html;charset=utf-8` instead of the
  required JSON endpoint. The driver was alive and the deterministic workload
  itself still completed correctly. Evidence:
  `task-07-live-endpoint-red.txt`.

### Focused GREEN and lifecycle correction

- The first GREEN compile exposed invalid Scala placeholder inference in the
  listener factory; it was corrected to an explicitly typed queue lambda.
- The next run exposed listener registration before checking `sc.ui`. The
  adapter now installs both listener and HTTP handlers only inside the
  existing Spark UI lifecycle branch, preserving the `NO_SPARK_UI` contract.
- Final focused/full Scala result at this stage: `35/35` passed. Targeted
  Python response/harness result: `52/52` passed. Evidence:
  `task-07-focused-scala-green.txt` and
  `task-07-focused-python-green.txt`.

### Runtime identity and live proof

- `make observer-runtime-refresh` rebuilt and staged the JAR, rebuilt the Spark
  image, recreated master/worker, observed Spark Master HTTP 200, and observed
  one ALIVE worker. Evidence: `task-07-runtime-refresh-green.txt`.
- Host, staged context, and active master-container JARs all matched SHA-256
  `f65f49c56ec005d50e322325dcca025553ec998b79c6982920f1cdb4e6d53825`.
  Evidence: `task-07-jar-checksum.txt`.
- Normal live run: application `app-20260720233715-0000`, PID `132`, counters
  grew from 1 to 6, both responses reported `invariantHolds=true`, and plugin
  drops stayed 0. The workload returned 40 rows and sum 780 with submit exit
  0. Cleanup proved driver/wrapper absence and endpoint unavailability.
  Evidence: `task-07-live-normal-green.txt`.
- Backpressure live run: application `app-20260720233757-0001`, PID `486`,
  capacity 1 and an explicit 1-second test delay produced two plugin drops at
  t2 with one item queued and one in flight. The invariant remained true, and
  the same 40-row/sum-780 workload still exited 0. Evidence:
  `task-07-live-drop-green.txt`.

### Real browser evidence

- Playwright captured the real counters endpoint twice for application
  `app-20260720234004-0003`: the screenshots show `listenerReceived` 42 at t1
  and 98 at t2, `droppedByPlugin=0`, and `invariantHolds=true` in both.
- Playwright captured the real capacity-one endpoint for application
  `app-20260720234200-0004`: 48 received, 14 processed, 1 queued, 1 in flight,
  32 plugin drops, and a true invariant. Its 80-row/sum-3160 visual workload
  completed with submit exit 0 and clean shutdown.
- The first t2 browser attempt truthfully failed with `ERR_EMPTY_RESPONSE`
  because that driver completed between the parsed response and browser
  navigation. The successful wider same-driver screenshots replaced it as
  visual evidence. The wider visual-only run later hit its original 90-second
  harness timeout and cleaned up with exit 124; canonical functional PASS is
  provided by the two exit-0 live runs above.
- Human-readable report: `task-07-counters-report.md`. Direct screenshots:
  `task-07-counters-wide-t1.png`, `task-07-counters-wide-t2.png`, and
  `task-07-counters-drop.png`.

### Deferred accepted follow-up

- Task 7 exposes `internalFailures`, but unexpected exceptions from
  `process(event)` remain the already-recorded Task 11 fail-open hardening
  item. No claim is made that this field is exercised by Task 7.
- A final read-only code review found no Critical or Important issue and
  assessed Task 7 as ready for user acceptance. Three Minor findings remain
  for later lifecycle/fail-open hardening: best-effort cleanup must attempt
  handler detachment and listener removal independently, an adapter-level
  regression should verify listener installation/removal rather than only the
  queue-name constant, and the zero-event `lastEventAt` representation must be
  defined and tested explicitly. None invalidates the Task 7 live proof.

### Final regression and shutdown

- `make observer-tests`: `35/35` passed. Evidence:
  `task-07-final-observer-tests.txt`.
- `make tests`: `76/76` passed. Evidence:
  `task-07-final-python-tests.txt`.
- `make observer-ui-tests`: `1/1` passed with the pinned Node 24 image.
  Evidence: `task-07-final-ui-tests.txt`.
- `make validate`: `Validation passed`. Evidence:
  `task-07-final-validate.txt`.
- Final source/document guard: `git diff --check`, shell syntax, Python
  compilation, frozen-path comparison, empty staged index, absence of Task 8
  files, and screenshot hashes all passed. Evidence:
  `task-07-final-guards.txt`.
- A fresh combined acceptance command repeated Scala `35/35`, Python `76/76`,
  Node `1/1`, `make validate`, and `git diff --check`, finishing with
  `task_07_acceptance_verification=PASS`. Evidence:
  `task-07-final-acceptance-verification.txt`.
- `make down` completed successfully and a subsequent Compose service listing
  was empty. Evidence: `task-07-infra-down.txt` and
  `task-07-infra-down-verification.txt`.

### Exact Task 7 implementation files

- `build/scripts/assert-observer-response.py`
- `build/scripts/run-observer-live-probe.sh`
- `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverConfig.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/ObserverRuntime.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/api/CountersResponse.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/api/CountersServlet.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/api/JsonRenderer.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/events/BoundedEventQueue.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverCounters.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverEvent.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverListener.scala`
- `spark-observer/src/main/scala/io/dataship/spark/observer/events/ObserverState.scala`
- `spark-observer/src/main/scala/org/apache/spark/dataship/v412/Spark412Bridge.scala`
- `spark-observer/src/test/scala/io/dataship/spark/observer/ObserverConfigSpec.scala`
- `spark-observer/src/test/scala/io/dataship/spark/observer/api/CountersResponseSpec.scala`
- `spark-observer/src/test/scala/io/dataship/spark/observer/events/ObserverListenerSpec.scala`
- `tests/test_observer_live_probe.py`
- `tests/test_observer_response_assertions.py`

Task-owned records are this execution-log section and
`docs/spark-observer/evidence/task-07/`.

### Task result and acceptance gate

- Result: `PASS — ACCEPTED`.
- The user explicitly accepted Task 7 on 2026-07-21 after an external static
  audit of commit `5a5626b8b5d7913e615a0acf8b3cf5c30e562e9a`, its fingerprints,
  transcripts, and Playwright evidence.
- No Task 8 behavior was implemented.
- On 2026-07-21, the user explicitly authorized the Task 7 commit and push so
  another model can validate the checkpoint. This publication is a review
  handoff that preceded the final acceptance recorded above.
- Follow-up routing was persisted in the implementation plan: live SQL
  category proof belongs to Task 9; adapter lifecycle/cleanup, the zero-event
  `lastEventAt` contract, and unexpected worker failures belong to Task 11;
  Task 12 must reconfirm those focused gates in the final E2E report.
- Next task title only: **Task 8 — expose live application, job, and stage
  snapshots**.
