# Laboratorio completo da Fase 3, de ponta a ponta.
#
# Executa a modelagem inteira e depois PROVA as duas propriedades que a fase promete:
#   - determinismo: rodar o dbt de novo produz exatamente o mesmo resultado;
#   - idempotencia: republicar sem mudanca nao cria snapshot nem altera linhas.
#
# Pre-requisitos: MinIO no ar e camada bronze ja populada (Fase 2).

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

$cli = ".\.venv\Scripts\pharma-pipeline.exe"

Write-Host "`n===== 1. Construindo silver e gold com testes =====" -ForegroundColor Cyan
& $cli transform build
if ($LASTEXITCODE -ne 0) { throw "A construcao falhou." }

Write-Host "`n===== 2. Publicando no Iceberg =====" -ForegroundColor Cyan
& $cli publish silver
if ($LASTEXITCODE -ne 0) { throw "A publicacao da silver falhou." }
& $cli publish gold
if ($LASTEXITCODE -ne 0) { throw "A publicacao da gold falhou." }

Write-Host "`n===== 3. Validando o contrato das tabelas publicadas =====" -ForegroundColor Cyan
& $cli expectations
if ($LASTEXITCODE -ne 0) { throw "As expectativas falharam." }

Write-Host "`n===== 4. Prova de idempotencia =====" -ForegroundColor Cyan
Write-Host "Reconstruindo e republicando sem nenhuma mudanca de dado."
Write-Host "Toda tabela deve responder unchanged=true.`n"

& $cli transform build
if ($LASTEXITCODE -ne 0) { throw "A reconstrucao falhou." }

$saida = & $cli publish gold | Out-String
Write-Host $saida

if ($saida -match '"unchanged":\s*false') {
    Write-Host "FALHOU: alguma tabela mudou sem que o dado de origem mudasse." -ForegroundColor Red
    Write-Host "Isso indica um modelo nao deterministico -- procure por current_timestamp," -ForegroundColor Red
    Write-Host "random() ou ordenacao sem criterio de desempate nos modelos." -ForegroundColor Red
    exit 1
}

Write-Host "`n===== 5. Snapshots e time travel =====" -ForegroundColor Cyan
& $cli snapshots gold.fato_evento_adverso

Write-Host "`nLaboratorio da Fase 3 concluido com sucesso." -ForegroundColor Green
