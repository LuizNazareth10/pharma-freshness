# Sobe o site local do dbt docs (lineage completo) em http://localhost:8082.
# Porta 8082 de proposito: 8080 costuma estar ocupada; 8081 e o Airflow.
# Ctrl+C encerra o servidor.

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

$cli = ".\.venv\Scripts\pharma-pipeline.exe"
if (-not (Test-Path $cli)) {
    throw "CLI nao encontrado em $cli. Rode .\scripts\day6\Setup-Phase3.ps1 antes."
}

Write-Host "Gerando catalogo e servindo em http://localhost:8082 ..." -ForegroundColor Cyan
& $cli transform docs --serve
