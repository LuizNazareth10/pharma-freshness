[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Push-Location $ProjectRoot

try {
    $Response = Invoke-WebRequest `
        -Uri 'http://localhost:9000/minio/health/live' `
        -UseBasicParsing `
        -TimeoutSec 5

    if ($Response.StatusCode -ne 200) {
        throw "Health check inesperado: HTTP $($Response.StatusCode)."
    }

    & docker compose --profile automation run --rm --no-deps minio-verify
    if ($LASTEXITCODE -ne 0) {
        throw 'O bucket ou o arquivo de teste não foi encontrado.'
    }

    Write-Host 'Validação concluída: serviço, bucket e objeto estão acessíveis.' -ForegroundColor Green
}
finally {
    Pop-Location
}

