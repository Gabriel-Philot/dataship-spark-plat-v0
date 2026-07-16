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

**Status:** `PASS TÉCNICO` — commit e push autorizados exclusivamente para revisão externa; aceite funcional do usuário permanece pendente.

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

### Risco restante

A Task 1 caracteriza somente a baseline sem plugin. Ela ainda não prova toolchain JVM, carregamento do JAR, endpoints, listener ou aba DataShip.

### Próxima fatia

Task 2 — adicionar o toolchain JVM totalmente conteinerizado. Não iniciada.
