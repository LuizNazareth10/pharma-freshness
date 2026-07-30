# Sobe a stack de orquestracao da Fase 4 e espera ela ficar saudavel.
#
# O Airflow vive num profile do compose. Um `docker compose up -d` comum continua subindo
# apenas o MinIO, para que as Fases 1-3 sigam leves.

param(
    [switch]$Rebuild
)

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

if (-not (Test-Path ".env")) {
    throw "Arquivo .env ausente. Copie .env.example para .env antes de subir o Airflow."
}

if ($Rebuild) {
    Write-Host "Reconstruindo a imagem do Airflow com o pipeline instalado..." -ForegroundColor Cyan
    docker compose --profile orquestracao build
    if ($LASTEXITCODE -ne 0) { throw "A construcao da imagem falhou." }
}

Write-Host "Subindo MinIO, Postgres e os componentes do Airflow..." -ForegroundColor Cyan
docker compose --profile orquestracao up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose up falhou." }

# O api-server leva algum tempo para migrar o banco e abrir a porta. Esperar pelo health check
# evita o falso negativo de abrir o navegador cedo demais e ver 'connection refused'.
Write-Host "`nAguardando o api-server ficar saudavel..." -ForegroundColor Cyan
$prazo = (Get-Date).AddMinutes(5)
do {
    Start-Sleep -Seconds 5
    $estado = (docker inspect --format '{{.State.Health.Status}}' pharma-freshness-airflow-apiserver-1 2>$null)
    Write-Host "  api-server: $estado"
    if ($estado -eq "healthy") { break }
} while ((Get-Date) -lt $prazo)

if ($estado -ne "healthy") {
    docker compose --profile orquestracao ps
    throw "O api-server nao ficou saudavel a tempo. Veja: docker compose --profile orquestracao logs airflow-apiserver"
}

$porta = (Select-String -Path ".env" -Pattern "^AIRFLOW_PORT=(.+)$").Matches.Groups[1].Value
$usuario = (Select-String -Path ".env" -Pattern "^AIRFLOW_ADMIN_USER=(.+)$").Matches.Groups[1].Value

Write-Host "`nAirflow no ar." -ForegroundColor Green
Write-Host "  Console: http://localhost:$porta"
Write-Host "  Usuario / senha: os de AIRFLOW_ADMIN_* no .env (padrao admin / admin)"
Write-Host "`nAs DAGs comecam PAUSADAS. Para ligar a diaria:" -ForegroundColor Yellow
Write-Host "  docker compose --profile orquestracao exec airflow-scheduler ``"
Write-Host "    airflow dags unpause pipeline_farmacovigilancia_diario"
Write-Host "`nEnquanto uma DAG roda, nao execute o CLI pelo host: os dois escrevem no" -ForegroundColor Yellow
Write-Host "mesmo DuckDB e no mesmo catalogo SQLite do Iceberg." -ForegroundColor Yellow
