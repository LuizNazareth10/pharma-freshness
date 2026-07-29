# Mostra o staleness gap por fonte e a serie historica das medicoes.
#
# Este script existe para tornar visivel o conceito central do projeto: o frescor deixou de ser
# intencao e virou numero medido, com limiar declarado e historico consultavel.

param(
    [switch]$Historico
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

$cli = ".\.venv\Scripts\pharma-pipeline.exe"

Write-Host "===== Avaliacao atual do frescor =====" -ForegroundColor Cyan
& $cli freshness --formato texto

Write-Host "`n===== Os tres relogios, por fonte =====" -ForegroundColor Cyan
Write-Host "atraso_da_fonte    : idade do dado quando o capturamos  (a origem controla)"
Write-Host "atraso_do_pipeline : ha quanto tempo nao capturamos     (nos controlamos)"
Write-Host "idade_do_dado      : o que o consumidor recebe agora    (soma dos dois)`n"

& $cli query gold.metricas_frescor `
    --columns fonte,medicao_em,atraso_da_fonte_horas,atraso_do_pipeline_horas,idade_do_dado_horas,situacao `
    --limit 20

if ($Historico) {
    Write-Host "`n===== Snapshots da serie de medicoes =====" -ForegroundColor Cyan
    Write-Host "Cada execucao acrescenta medicoes; e por isso que esta tabela e um log."
    & $cli snapshots gold.metricas_frescor
}

Write-Host "`nPara ver a evolucao ao longo do tempo, rode o pipeline mais de uma vez:" -ForegroundColor Yellow
Write-Host "  .\scripts\day9\Test-Dag.ps1 -Dag diario"
