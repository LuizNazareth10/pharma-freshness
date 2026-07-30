# Dia 14 — verificacao de ponta a ponta / reprodutibilidade.
#
# Por padrao valida o estado ATUAL (transform test, metricas_frescor, freshness).
# Com -FromScratch: derruba volumes, sobe MinIO de novo e espera que voce rode a
# ingestao+transform+publish antes de revalidar (o script avisa o ponto de pausa).

param(
    [switch]$FromScratch
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

$cli = ".\.venv\Scripts\pharma-pipeline.exe"

if ($FromScratch) {
    Write-Host "`n===== FromScratch: derrubando containers e volumes =====" -ForegroundColor Yellow
    Write-Host "Isso APAGA os dados do MinIO e do Postgres do Airflow."
    docker compose --profile orquestracao down -v
    if ($LASTEXITCODE -ne 0) { throw "docker compose down falhou." }

    Write-Host "`n===== Subindo MinIO =====" -ForegroundColor Cyan
    .\scripts\day1\Start-MinIO.ps1
    .\scripts\day1\Initialize-MinIO.ps1

    Write-Host @"

Ambiente limpo. Antes de continuar, rode o pipeline completo uma vez, por exemplo:

  pharma-pipeline run dailymed --initial-date 2026-07-20 --page-size 10 --max-pages 1 --pipeline-suffix _dia14
  pharma-pipeline run faers    --initial-date 2026-07-20 --page-size 10 --max-pages 1 --pipeline-suffix _dia14
  pharma-pipeline run res      --initial-date 2026-04-01 --page-size 10 --max-pages 1 --pipeline-suffix _dia14
  pharma-pipeline transform build
  pharma-pipeline publish silver
  pharma-pipeline publish gold

Depois rode de novo SEM -FromScratch:

  .\scripts\day14\Verify-Reproducibility.ps1

"@ -ForegroundColor Cyan
    exit 0
}

Write-Host "`n===== 1. dbt test =====" -ForegroundColor Cyan
& $cli transform test
if ($LASTEXITCODE -ne 0) { throw "dbt test falhou." }

Write-Host "`n===== 2. metricas_frescor preenchida? =====" -ForegroundColor Cyan
$frescor = & $cli query gold.metricas_frescor --limit 5 | Out-String
Write-Host $frescor
if ($frescor -match '"error"' -or $frescor -match "Nao existe" -or $frescor -match "NoSuchTable") {
    throw "metricas_frescor ausente ou ilegivel. Rode transform build + publish gold."
}

Write-Host "`n===== 3. freshness (relatorio) =====" -ForegroundColor Cyan
& $cli freshness --formato texto
# Nao falhamos em atraso de fonte; so em erro de execucao do comando.
if ($LASTEXITCODE -gt 1) { throw "freshness falhou com codigo $LASTEXITCODE." }

Write-Host "`n===== 4. serving acessivel? =====" -ForegroundColor Cyan
& $cli query gold.alertas_recentes --columns safetyreportid,farmaco,fonte --limit 3
& $cli query gold.bulas_atualizadas --columns setid,farmaco,data_revisao --limit 3

Write-Host "`nVerificacao da Fase 6 concluida." -ForegroundColor Green
Write-Host "Se dbt test passou e metricas_frescor responde, o projeto esta reproduzivel neste estado."
