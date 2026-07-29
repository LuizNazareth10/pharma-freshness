# Dia 8 -- roda as duas barreiras de qualidade do projeto.
#
#   1. dbt test          -> valida a TRANSFORMACAO, dentro do DuckDB, antes de publicar.
#   2. Great Expectations-> valida o CONTRATO da tabela Iceberg ja gravada no MinIO.
#
# Elas nao sao redundantes. A primeira responde "o modelo esta certo?"; a segunda responde
# "o que os consumidores enxergam esta certo?". A segunda pega falha de conversao de tipo na
# escrita, UPSERT em chave errada e publicacao parcial -- coisas invisiveis para a primeira.

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

$cli = ".\.venv\Scripts\pharma-pipeline.exe"
$falhas = @()

Write-Host "`n== 1/2 Testes do dbt (transformacao) ==" -ForegroundColor Cyan
& $cli transform test
if ($LASTEXITCODE -ne 0) { $falhas += "dbt test" }

Write-Host "`n== 2/2 Great Expectations (contrato das tabelas publicadas) ==" -ForegroundColor Cyan
& $cli expectations
if ($LASTEXITCODE -ne 0) { $falhas += "great expectations" }

Write-Host ""
if ($falhas.Count -gt 0) {
    Write-Host "Barreiras com falha: $($falhas -join ', ')" -ForegroundColor Red
    exit 1
}

Write-Host "Todas as barreiras de qualidade passaram." -ForegroundColor Green
