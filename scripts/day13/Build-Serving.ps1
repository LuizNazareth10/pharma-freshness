# Dia 13 — materializa a camada de serving e gera o catalogo dbt docs.
#
# Pre-requisito: silver/gold ja construidos pelo menos uma vez (Fases 3-4).

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

$cli = ".\.venv\Scripts\pharma-pipeline.exe"
if (-not (Test-Path $cli)) {
    throw "CLI nao encontrado em $cli. Rode .\scripts\day6\Setup-Phase3.ps1 antes."
}

Write-Host "`n===== 1. Build dos modelos de serving =====" -ForegroundColor Cyan
# Sem `+`: le dims/fatos ja materializados no DuckDB. Com `+`, reconstroi ate a bronze e
# exige MinIO no ar -- util so quando o banco local esta vazio.
& $cli transform build --select "alertas_recentes bulas_atualizadas"
if ($LASTEXITCODE -ne 0) { throw "dbt build da serving falhou." }

Write-Host "`n===== 2. Publicando gold (serving usa REPLACE) =====" -ForegroundColor Cyan
& $cli publish gold
if ($LASTEXITCODE -ne 0) { throw "publish gold falhou." }

Write-Host "`n===== 3. Gerando dbt docs (lineage) =====" -ForegroundColor Cyan
& $cli transform docs
if ($LASTEXITCODE -ne 0) { throw "dbt docs generate falhou." }

Write-Host "`nServing pronta. Para abrir o lineage:" -ForegroundColor Green
Write-Host "  .\scripts\day13\Serve-Docs.ps1"
Write-Host "`nConsultas uteis:"
Write-Host "  pharma-pipeline query gold.alertas_recentes --limit 5"
Write-Host "  pharma-pipeline query gold.bulas_atualizadas --limit 5"
