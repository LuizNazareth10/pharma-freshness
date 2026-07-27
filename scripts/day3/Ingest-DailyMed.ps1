param(
    [string]$InitialDate = "2026-07-20",
    [int]$PageSize = 10,
    [int]$MaxPages = 1
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

& ".\.venv\Scripts\pharma-pipeline.exe" ingest dailymed `
    --initial-date $InitialDate `
    --page-size $PageSize `
    --max-pages $MaxPages `
    --pipeline-suffix _day3_lab
