# Prepara o ambiente da Fase 3 e confirma que o dbt enxerga o lakehouse.
#
# Idempotente: pode ser executado quantas vezes for preciso.

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Arquivo .env criado. Revise as credenciais antes de continuar."
}

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

Write-Host "`n== Instalando dependencias (inclui dbt, DuckDB e Great Expectations) ==" -ForegroundColor Cyan
& ".\.venv\Scripts\python.exe" -m pip install --upgrade pip --quiet
& ".\.venv\Scripts\python.exe" -m pip install -e ".[dev]" --quiet

Write-Host "`n== Versoes instaladas ==" -ForegroundColor Cyan
& ".\.venv\Scripts\dbt.exe" --version

Write-Host "`n== Validando o projeto dbt e a conexao com o Iceberg ==" -ForegroundColor Cyan
# `parse` compila o grafo sem tocar nos dados; `debug` testa a conexao real com o DuckDB
# e com o catalogo Iceberg no MinIO.
& ".\.venv\Scripts\pharma-pipeline.exe" transform parse
if ($LASTEXITCODE -ne 0) { throw "O projeto dbt nao compilou. Verifique os modelos." }

Write-Host "`nAmbiente da Fase 3 pronto." -ForegroundColor Green
Write-Host "Proximo passo: .\scripts\day6\Build-Silver.ps1"
