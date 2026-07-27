param(
    [int]$PageSize = 5,
    [int]$MaxPages = 1
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $projectRoot

$commands = @(
    @("dailymed", "2026-07-20"),
    @("faers", "2026-03-31"),
    @("res", "2026-07-01")
)

foreach ($item in $commands) {
    $source = $item[0]
    $initialDate = $item[1]
    & ".\.venv\Scripts\pharma-pipeline.exe" run $source `
        --initial-date $initialDate `
        --page-size $PageSize `
        --max-pages $MaxPages `
        --pipeline-suffix _day5_lab
    & ".\.venv\Scripts\pharma-pipeline.exe" verify-idempotency $source
}
