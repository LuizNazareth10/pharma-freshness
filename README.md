# Pharma Freshness Pipeline

Projeto didático de engenharia de dados para medir o frescor de informações de
farmacovigilância. A implementação cobre:

- **Fase 1 — Fundação (Dias 1–2):** MinIO local e exploração das APIs;
- **Fase 2 — Ingestão batch (Dias 3–5):** dlt, Parquet bronze, Iceberg, snapshots, time travel,
  watermarks e idempotência para DailyMed, FAERS e RES;
- **Fase 3 — Modelagem (Dias 6–8):** dbt sobre DuckDB, camada silver, normalização RxNorm,
  modelo dimensional na gold e duas barreiras de qualidade;
- **Fase 4 — Orquestração (Dias 9–10):** Airflow em container, DAGs diária e semanal, e a
  medição do *staleness gap* por fonte com SLOs declarados.

## Arquitetura

```
   APIs públicas (FDA / NLM)
            │  dlt: paginação, retry, watermark
            ▼
   Parquet imutável no MinIO ......................... bronze/<fonte>/
            │  PyIceberg: UPSERT por chave
            ▼
   Iceberg bronze ..................................... iceberg/bronze/
            │  dbt-duckdb (plugin iceberg → Arrow)
            ▼
   DuckDB — motor de transformação .................... .local/duckdb/
     silver: limpeza, explosão de arrays, RxNorm
     gold:   dimensões + fatos
            │  PyIceberg: mesmo UPSERT da bronze
            ▼
   Iceberg silver + gold .............................. iceberg/silver/, iceberg/gold/
            │
            ▼
   Great Expectations — contrato das tabelas publicadas
            │
            ▼
   gold.metricas_frescor — staleness gap medido por fonte
```

O DuckDB é o **motor** (efêmero, reconstruível). O Iceberg é o **armazenamento de estado**
(transacional, versionado, com time travel). O **Airflow** agenda tudo isso e falha quando o
atraso é nosso.

## Início rápido

Pré-requisitos: Docker Desktop, Docker Compose, Python 3.12 e PowerShell 5.1 ou superior.

```powershell
Copy-Item .env.example .env       # apenas se .env ainda não existir
.\scripts\day1\Start-MinIO.ps1
.\scripts\day1\Initialize-MinIO.ps1
.\scripts\day6\Setup-Phase3.ps1   # cria a .venv e instala tudo, inclusive dbt
```

### Fase 2 — trazer dados reais para o lakehouse

```powershell
.\.venv\Scripts\Activate.ps1

pharma-pipeline run dailymed --initial-date 2026-07-20 `
  --page-size 10 --max-pages 1 --pipeline-suffix _meu_lab

pharma-pipeline snapshots dailymed
pharma-pipeline verify-idempotency dailymed
```

### Fase 3 — transformar em modelo analítico

```powershell
pharma-pipeline transform build     # 17 modelos + 124 testes no DuckDB
pharma-pipeline publish silver      # publica como tabelas Iceberg
pharma-pipeline publish gold
pharma-pipeline expectations        # valida o contrato do que foi publicado
```

Ou o laboratório completo, que ao final **prova a idempotência**:

```powershell
.\scripts\day8\Run-Phase3-Lab.ps1
```

### Fase 4 — deixar o pipeline rodar sozinho e medir o frescor

```powershell
.\scripts\day9\Start-Airflow.ps1          # sobe Postgres + Airflow (profile orquestracao)
.\scripts\day9\Test-Dag.ps1 -Dag diario   # roda a DAG inteira sem esperar o agendamento
.\scripts\day10\Show-Frescor.ps1          # staleness gap por fonte
```

Console do Airflow em <http://localhost:8081> (credenciais no `.env`). As DAGs começam
**pausadas** — subir o orquestrador não deve disparar ingestão sem que alguém decida isso.

```powershell
pharma-pipeline freshness --formato texto     # relatório legível
pharma-pipeline freshness --fail-on-breach    # sai 1 só se o atraso for NOSSO
```

### Consultar

```powershell
pharma-pipeline tables              # toda tabela, com chave e grão

pharma-pipeline query gold.fato_evento_adverso `
  --columns safetyreportid,id_farmaco,id_reacao,gravidade --limit 5

pharma-pipeline snapshots gold.dim_bula
pharma-pipeline query gold.dim_bula --as-of "2026-07-28T19:30:00Z" --limit 3
```

Os objetos podem ser vistos no console do MinIO em <http://localhost:9001>:

- Parquet imutável: `bronze/<fonte>/.../<tabela>/load_id=<id>/*.parquet`;
- tabelas Iceberg: `iceberg/{bronze,silver,gold}/<tabela>/{data,metadata}/`.

## Documentação

| Documento | Conteúdo |
|---|---|
| [Fase 1 — tutorial](docs/dia-1-2.md) | MinIO, Docker e exploração das APIs |
| [Fase 2 — tutorial](docs/fase-2.md) | Ingestão, Iceberg, watermarks, idempotência |
| [Fase 3 — tutorial](docs/fase-3.md) | dbt, silver, RxNorm, modelo dimensional, testes |
| [Fase 4 — tutorial](docs/fase-4.md) | Airflow, DAGs, retry, catchup e medição do frescor |
| [Contratos — Fase 2](docs/contratos-dados-fase-2.md) | Grão e chaves da bronze |
| [Contratos — Fase 3](docs/contratos-dados-fase-3.md) | Grão e chaves da silver e da gold |
| [Decisões — Fase 2](docs/decisoes-arquitetura-fase-2.md) | Trade-offs da ingestão |
| [Decisões — Fase 3](docs/decisoes-arquitetura-fase-3.md) | Trade-offs da modelagem |
| [Schema das APIs](docs/api-schema-notes.md) | Campos observados nas respostas |
| [Plano geral](docs/foundation.md) | As seis fases do projeto |

## Estrutura

```
src/pharma_pipeline/     ingestão, Iceberg, RxNorm, publicação, qualidade, frescor, CLI
transform/               projeto dbt (modelos, macros, seeds, testes)
dags/                    DAGs do Airflow e as funções que elas chamam
docker/airflow/          imagem do Airflow com o pipeline instalado
scripts/day1..day10/     roteiros PowerShell por dia de aprendizado
tests/                   testes unitários Python
docs/                    tutoriais, contratos e decisões
```

## Verificação do código

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ruff check src tests dags
.\.venv\Scripts\python.exe -m ruff format --check src tests dags
.\.venv\Scripts\pharma-pipeline.exe transform test
```

Os testes das DAGs exigem o Airflow e são pulados fora da imagem de orquestração. Para
executá-los:

```powershell
docker compose --profile orquestracao run --rm airflow-scheduler `
    python -m pytest /opt/pharma/tests/test_dags.py
```

> O projeto é educacional e os dados públicos não provam causalidade clínica. Um relato do FAERS
> registra suspeita, não nexo causal: um mesmo relato cita vários medicamentos e várias reações
> sem ligar um ao outro. Sinais devem ser descritos como associações que exigem investigação,
> nunca como prova de que um medicamento causou um evento.
