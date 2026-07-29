# Dia 6 -- constroi a camada silver e a publica no Iceberg.
#
# Sequencia: modelos + testes no DuckDB  ->  publicacao no MinIO.
# A publicacao so acontece se os testes passarem; dado que falhou no teste nao deve chegar
# ao lakehouse.

param(
    # Pula o modelo `rxnorm_mapping`, que consulta a API do RxNav. Use quando quiser uma
    # execucao rapida ou sem rede (o cache local ja resolvido continua valendo).
    [switch]$SemRxNorm
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

$cli = ".\.venv\Scripts\pharma-pipeline.exe"

$argumentos = @("transform", "build", "--select", "silver bronze")
if ($SemRxNorm) {
    $argumentos += @("--exclude", "rxnorm_mapping")
    Write-Host "Modo sem RxNorm: o mapeamento existente sera reaproveitado." -ForegroundColor Yellow
}

Write-Host "`n== 1/2 Construindo e testando a silver no DuckDB ==" -ForegroundColor Cyan
& $cli @argumentos
if ($LASTEXITCODE -ne 0) { throw "A construcao da silver falhou; nada sera publicado." }

Write-Host "`n== 2/2 Publicando a silver como tabelas Iceberg ==" -ForegroundColor Cyan
& $cli publish silver
if ($LASTEXITCODE -ne 0) { throw "A publicacao da silver falhou." }

Write-Host "`nSilver pronta. Inspecione com:" -ForegroundColor Green
Write-Host "  $cli query silver.stg_faers_drugs --columns safetyreportid,drug_seq,produto_relatado,nome_normalizado --limit 5"
Write-Host "  $cli query silver.rxnorm_mapping --columns nome_normalizado,rxcui,rxnorm_nome,tipo_correspondencia --limit 5"
