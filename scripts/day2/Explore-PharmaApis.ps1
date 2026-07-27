[CmdletBinding()]
param(
    [ValidateRange(1, 100)]
    [int]$DailyMedPageSize = 5,

    [ValidateRange(1, 1000)]
    [int]$FaersPageSize = 5,

    [string]$ApiKey,

    [string]$OutputDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $Timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $OutputDirectory = Join-Path $ProjectRoot ".local\api-samples\$Timestamp"
}

$null = New-Item -ItemType Directory -Path $OutputDirectory -Force

function Invoke-And-SaveJson {
    param(
        [Parameter(Mandatory)] [string]$Name,
        [Parameter(Mandatory)] [string]$Uri
    )

    Write-Host "GET $Uri" -ForegroundColor Cyan
    $StartedAt = Get-Date
    $Response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -TimeoutSec 30
    $ElapsedMilliseconds = [math]::Round(((Get-Date) - $StartedAt).TotalMilliseconds)
    $Body = $Response.Content | ConvertFrom-Json
    $Path = Join-Path $OutputDirectory "$Name.json"
    $Body | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $Path -Encoding utf8

    [pscustomobject]@{
        Name = $Name
        Uri = $Uri
        StatusCode = [int]$Response.StatusCode
        ElapsedMilliseconds = $ElapsedMilliseconds
        Path = $Path
        Body = $Body
    }
}

$DailyMedBase = 'https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.json'
$DailyMedPage1 = Invoke-And-SaveJson `
    -Name 'dailymed-page-1' `
    -Uri "$DailyMedBase`?pagesize=$DailyMedPageSize&page=1"
$DailyMedPage2 = Invoke-And-SaveJson `
    -Name 'dailymed-page-2' `
    -Uri "$DailyMedBase`?pagesize=$DailyMedPageSize&page=2"

$FaersBase = 'https://api.fda.gov/drug/event.json'
$ApiKeyQuery = if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    ''
}
else {
    "api_key=$([uri]::EscapeDataString($ApiKey))&"
}

$FaersPage1 = Invoke-And-SaveJson `
    -Name 'faers-page-1' `
    -Uri "$FaersBase`?$ApiKeyQuery`limit=$FaersPageSize&skip=0"
$FaersPage2 = Invoke-And-SaveJson `
    -Name 'faers-page-2' `
    -Uri "$FaersBase`?$ApiKeyQuery`limit=$FaersPageSize&skip=$FaersPageSize"

$Summary = [ordered]@{
    executed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
    output_directory = $OutputDirectory
    dailymed = [ordered]@{
        page_1 = [ordered]@{
            status = $DailyMedPage1.StatusCode
            records = @($DailyMedPage1.Body.data).Count
            current_page = $DailyMedPage1.Body.metadata.current_page
            next_page = $DailyMedPage1.Body.metadata.next_page
            total_elements = $DailyMedPage1.Body.metadata.total_elements
        }
        page_2 = [ordered]@{
            status = $DailyMedPage2.StatusCode
            records = @($DailyMedPage2.Body.data).Count
            current_page = $DailyMedPage2.Body.metadata.current_page
            previous_page = $DailyMedPage2.Body.metadata.previous_page
        }
    }
    faers = [ordered]@{
        page_1 = [ordered]@{
            status = $FaersPage1.StatusCode
            records = @($FaersPage1.Body.results).Count
            skip = $FaersPage1.Body.meta.results.skip
            limit = $FaersPage1.Body.meta.results.limit
            total = $FaersPage1.Body.meta.results.total
        }
        page_2 = [ordered]@{
            status = $FaersPage2.StatusCode
            records = @($FaersPage2.Body.results).Count
            skip = $FaersPage2.Body.meta.results.skip
            limit = $FaersPage2.Body.meta.results.limit
        }
    }
}

$SummaryPath = Join-Path $OutputDirectory 'summary.json'
$Summary | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $SummaryPath -Encoding utf8

Write-Host ''
Write-Host 'Exploração concluída.' -ForegroundColor Green
Write-Host "Amostras e resumo: $OutputDirectory"
$Summary | ConvertTo-Json -Depth 20

