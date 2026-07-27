param(
    [ValidateSet("dailymed", "faers", "res")]
    [string]$Source = "dailymed"
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

& ".\.venv\Scripts\pharma-pipeline.exe" sync $Source
& ".\.venv\Scripts\pharma-pipeline.exe" snapshots $Source
