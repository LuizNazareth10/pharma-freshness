[CmdletBinding()]
param(
    [ValidateRange(10, 600)]
    [int]$TimeoutSeconds = 120
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Push-Location $ProjectRoot

try {
    if (-not (Test-Path -LiteralPath '.env')) {
        throw 'Arquivo .env ausente. Execute: Copy-Item .env.example .env'
    }

    & docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw 'O Docker Engine não está disponível. Inicie o Docker Desktop e tente novamente.'
    }

    & docker compose up -d minio
    if ($LASTEXITCODE -ne 0) {
        throw 'Não foi possível iniciar o serviço MinIO.'
    }

    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $HealthUri = 'http://localhost:9000/minio/health/live'
    do {
        try {
            $Response = Invoke-WebRequest -Uri $HealthUri -UseBasicParsing -TimeoutSec 3
            if ($Response.StatusCode -eq 200) {
                Write-Host 'MinIO está saudável.' -ForegroundColor Green
                Write-Host 'Console: http://localhost:9001'
                Write-Host 'API S3:  http://localhost:9000'
                & docker compose ps minio
                exit 0
            }
        }
        catch {
            Start-Sleep -Seconds 2
        }
    } while ((Get-Date) -lt $Deadline)

    & docker compose logs --tail 50 minio
    throw "O MinIO não ficou saudável em $TimeoutSeconds segundos."
}
finally {
    Pop-Location
}

