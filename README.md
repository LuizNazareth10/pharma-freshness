# Pharma Freshness Pipeline

Projeto didático de engenharia de dados para medir o frescor de informações de farmacovigilância. A implementação atual cobre a **Fase 1 — Fundação (Dias 1–2)**: MinIO local com Docker e exploração manual das APIs DailyMed e FAERS.

## Início rápido

Pré-requisitos: Docker Desktop com Docker Compose e PowerShell 5.1 ou superior.

```powershell
Copy-Item .env.example .env  # necessário apenas se .env ainda não existir
.\scripts\day1\Start-MinIO.ps1
```

Abra <http://localhost:9001>, entre com as credenciais de `.env`, crie o bucket `farmacovigilancia` e envie `samples/minio/arquivo-teste.json` para a pasta `laboratorio`.

Depois, valide e explore as APIs:

```powershell
.\scripts\day1\Test-MinIO.ps1
.\scripts\day2\Explore-PharmaApis.ps1
```

O tutorial completo, incluindo conceitos, comandos equivalentes, solução de problemas e exercícios, está em [docs/dia-1-2.md](docs/dia-1-2.md). O schema observado nas APIs está em [docs/api-schema-notes.md](docs/api-schema-notes.md).

> Esta fase não implementa ingestão. Os JSONs do Dia 2 são amostras locais ignoradas pelo Git; a gravação no lakehouse começa apenas no Dia 3.

