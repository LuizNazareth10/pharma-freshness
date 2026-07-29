# Dia 7 -- constroi o modelo dimensional da gold e o publica no Iceberg.

param(
    # Recria os fatos incrementais do zero. Necessario quando o SQL de um fato muda de forma
    # que altera linhas ja carregadas -- o MERGE sozinho nao corrige historico.
    [switch]$FullRefresh
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

$cli = ".\.venv\Scripts\pharma-pipeline.exe"

$argumentos = @("transform", "build", "--select", "gold")
if ($FullRefresh) { $argumentos += "--full-refresh" }

Write-Host "`n== 1/2 Construindo e testando a gold no DuckDB ==" -ForegroundColor Cyan
& $cli @argumentos
if ($LASTEXITCODE -ne 0) { throw "A construcao da gold falhou; nada sera publicado." }

Write-Host "`n== 2/2 Publicando a gold como tabelas Iceberg ==" -ForegroundColor Cyan
$publishArgs = @("publish", "gold")
if ($FullRefresh) {
    # Com --full-refresh o schema do modelo pode ter mudado; recriar a tabela Iceberg evita
    # um erro de incompatibilidade de schema no commit.
    $publishArgs += "--recreate"
}
& $cli @publishArgs
if ($LASTEXITCODE -ne 0) { throw "A publicacao da gold falhou." }

Write-Host "`nGold pronta. Experimente:" -ForegroundColor Green
Write-Host "  $cli query gold.fato_evento_adverso --columns safetyreportid,id_farmaco,id_reacao,gravidade --limit 5"
Write-Host "  $cli snapshots gold.fato_evento_adverso"
