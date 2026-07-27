# Pharma Freshness Pipeline

Projeto didático de engenharia de dados para medir o frescor de informações de farmacovigilância. A implementação cobre:

- **Fase 1 — Fundação (Dias 1–2):** MinIO local e exploração das APIs;
- **Fase 2 — Ingestão batch (Dias 3–5):** dlt, Parquet bronze, Iceberg, snapshots, time travel, watermarks e idempotência para DailyMed, FAERS e RES.

## Início rápido

Pré-requisitos: Docker Desktop, Docker Compose, Python 3.12 e PowerShell 5.1 ou superior.

```powershell
Copy-Item .env.example .env  # apenas se .env ainda não existir
.\scripts\day1\Start-MinIO.ps1
.\scripts\day1\Initialize-MinIO.ps1
.\scripts\day3\Setup-Phase2.ps1
```

Ative o ambiente e faça uma ingestão pequena e isolada:

```powershell
.\.venv\Scripts\Activate.ps1
pharma-pipeline run dailymed `
  --initial-date 2026-07-20 `
  --page-size 10 `
  --max-pages 1 `
  --pipeline-suffix _meu_lab

pharma-pipeline snapshots dailymed
pharma-pipeline query dailymed --columns setid,spl_version,title,published_date,ingest_time --limit 5
pharma-pipeline verify-idempotency dailymed
```

Os objetos podem ser vistos no console do MinIO em <http://localhost:9001>:

- Parquet imutável: `bronze/<fonte>/.../<tabela>/load_id=<id>/*.parquet`;
- tabelas Iceberg: `iceberg/bronze/<tabela>/{data,metadata}/`.

## Documentação

- [Fase 1 — tutorial completo](docs/dia-1-2.md)
- [Fase 2 — tutorial, conceitos e operação](docs/fase-2.md)
- [Contratos e significado dos dados](docs/contratos-dados-fase-2.md)
- [Decisões de arquitetura](docs/decisoes-arquitetura-fase-2.md)
- [Schema observado nas APIs](docs/api-schema-notes.md)

## Verificação do código

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m ruff format --check src tests
```

> O projeto é educacional e os dados públicos não provam causalidade clínica. Sinais em FAERS devem ser descritos como associações que exigem investigação, nunca como prova de que um medicamento causou um evento.
