[CmdletBinding()]
param(
    [ValidateRange(1, 1000)]
    [int]$Requests = 245,

    [ValidateRange(0, 60000)]
    [int]$DelayMilliseconds = 0,

    [ValidateRange(1, 50)]
    [int]$Concurrency = 20,

    [string]$ApiKey,

    [switch]$ConfirmTraffic
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $ConfirmTraffic) {
    throw @'
Este laboratório pode fazer centenas de chamadas e consumir parte da cota diária.
Leia docs/dia-1-2.md e execute novamente com -ConfirmTraffic para confirmar.
'@
}

$Endpoint = 'https://api.fda.gov/drug/event.json'
$ApiKeyQuery = if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    ''
}
else {
    "api_key=$([uri]::EscapeDataString($ApiKey))&"
}

$StartedAt = Get-Date
$SuccessCount = 0
$RateLimitedAt = $null
$RetryAfter = $null
$RequestErrors = 0
$StatusCounts = @{}
$OriginalConnectionLimit = [System.Net.ServicePointManager]::DefaultConnectionLimit
[System.Net.ServicePointManager]::DefaultConnectionLimit = [math]::Max($OriginalConnectionLimit, $Concurrency)

Add-Type -AssemblyName System.Net.Http
$Handler = [System.Net.Http.HttpClientHandler]::new()
$Client = [System.Net.Http.HttpClient]::new($Handler)
$Client.Timeout = [timespan]::FromSeconds(30)

try {
    for ($BatchStart = 0; $BatchStart -lt $Requests; $BatchStart += $Concurrency) {
        $BatchEnd = [math]::Min($BatchStart + $Concurrency, $Requests)
        $Batch = @()

        for ($Index = $BatchStart; $Index -lt $BatchEnd; $Index++) {
            $Uri = "$Endpoint`?$ApiKeyQuery`limit=1&skip=$Index"
            $Task = $Client.GetAsync(
                $Uri,
                [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead
            )
            $Batch += [pscustomobject]@{
                RequestNumber = $Index + 1
                Task = $Task
            }
        }

        $Tasks = [System.Threading.Tasks.Task[]]@($Batch | ForEach-Object { $_.Task })
        try {
            [System.Threading.Tasks.Task]::WaitAll($Tasks)
        }
        catch {
            # As falhas individuais são registradas abaixo sem esconder as demais respostas.
        }

        foreach ($Item in $Batch) {
            $Response = $null
            try {
                $Response = $Item.Task.GetAwaiter().GetResult()
                $StatusCode = [int]$Response.StatusCode
                $StatusKey = [string]$StatusCode
                if (-not $StatusCounts.ContainsKey($StatusKey)) {
                    $StatusCounts[$StatusKey] = 0
                }
                $StatusCounts[$StatusKey]++

                if ($StatusCode -eq 200) {
                    $SuccessCount++
                }
                elseif ($StatusCode -eq 429) {
                    if ($null -eq $RateLimitedAt) {
                        $RateLimitedAt = $Item.RequestNumber
                    }
                    if ($Response.Headers.RetryAfter) {
                        $RetryAfter = $Response.Headers.RetryAfter.ToString()
                    }
                }
            }
            catch {
                $RequestErrors++
                Write-Warning "Falha de transporte na requisição $($Item.RequestNumber): $($_.Exception.Message)"
            }
            finally {
                if ($null -ne $Response) {
                    $Response.Dispose()
                }
            }
        }

        Write-Host "Lote $($BatchStart + 1)-$BatchEnd/$Requests concluído. HTTP 200 acumulados: $SuccessCount."

        if ($null -ne $RateLimitedAt) {
            Write-Host "Rate limit observado a partir da requisição ${RateLimitedAt}: HTTP 429." -ForegroundColor Yellow
            break
        }

        if ($DelayMilliseconds -gt 0) {
            Start-Sleep -Milliseconds $DelayMilliseconds
        }
    }
}
finally {
    $Client.Dispose()
    $Handler.Dispose()
    [System.Net.ServicePointManager]::DefaultConnectionLimit = $OriginalConnectionLimit
}

$Result = [ordered]@{
    started_at = $StartedAt.ToString('o')
    elapsed_seconds = [math]::Round(((Get-Date) - $StartedAt).TotalSeconds, 2)
    requested_maximum = $Requests
    successful_requests = $SuccessCount
    rate_limited_at_request = $RateLimitedAt
    retry_after_header = $RetryAfter
    transport_errors = $RequestErrors
    concurrency = $Concurrency
    status_counts = $StatusCounts
    used_api_key = -not [string]::IsNullOrWhiteSpace($ApiKey)
}

$Result | ConvertTo-Json

if ($null -eq $RateLimitedAt) {
    Write-Warning 'Nenhum HTTP 429 foi observado. O intervalo pode ter virado, ou o limite aplicado pode ser diferente. Não aumente a carga indiscriminadamente.'
}
