[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Push-Location $ProjectRoot

try {
    & docker compose --profile automation run --rm minio-bootstrap
    if ($LASTEXITCODE -ne 0) {
        throw 'A criação automatizada do bucket/objeto falhou.'
    }

    Write-Host 'Bucket e objeto de laboratório criados com sucesso.' -ForegroundColor Green
}
finally {
    Pop-Location
}

