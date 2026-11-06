$ErrorActionPreference = "Stop"

$backendRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$composeFile = Join-Path $backendRoot "deployments\docker-compose.yml"
$project = "wallet-chaos-" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
$environmentFile = Join-Path $env:TEMP ($project + ".env")
$apiPort = 18081
$postgresPort = 55441
$redisPort = 6381
$natsPort = 54231
$natsMonitorPort = 18231
$prometheusPort = 19091
$riskPort = 50052
$organizationName = "chaos-" + [Guid]::NewGuid().ToString("N")
$email = $organizationName + "@example.test"
$password = "chaos-test-password"
$transferCount = 40

$environment = @"
POSTGRES_USER=wallet_test
POSTGRES_PASSWORD=wallet_test_only
POSTGRES_DB=wallet_test
REDIS_PASSWORD=wallet_redis_test
JWT_SECRET=not-a-production-secret-for-chaos-test-only
WEBHOOK_ENCRYPTION_KEY=AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=
API_HOST_PORT=$apiPort
POSTGRES_HOST_PORT=$postgresPort
REDIS_HOST_PORT=$redisPort
NATS_HOST_PORT=$natsPort
NATS_MONITOR_HOST_PORT=$natsMonitorPort
PROMETHEUS_HOST_PORT=$prometheusPort
RISK_HOST_PORT=$riskPort
"@

Add-Type -TypeDefinition @'
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

public static class WalletChaosLoad
{
    private sealed class TimeoutWebClient : WebClient
    {
        protected override WebRequest GetWebRequest(Uri address)
        {
            WebRequest request = base.GetWebRequest(address);
            request.Timeout = 5000;
            HttpWebRequest httpRequest = request as HttpWebRequest;
            if (httpRequest != null)
                httpRequest.ReadWriteTimeout = 5000;
            return request;
        }
    }

    public static Task<string> SubmitAsync(
        string endpoint,
        string token,
        string sourceWallet,
        string destinationWallet,
        int index,
        ManualResetEventSlim start)
    {
        return Task.Run(() =>
        {
            start.Wait();
            string idempotencyKey = "chaos-transfer-" + index;
            string body = "{\"source_wallet_id\":\"" + sourceWallet +
                "\",\"destination_wallet_id\":\"" + destinationWallet +
                "\",\"currency\":\"USD\",\"amount\":100}";

            for (int attempt = 0; attempt < 60; attempt++)
            {
                try
                {
                    using (var client = new TimeoutWebClient())
                    {
                        client.Headers[HttpRequestHeader.Authorization] = "Bearer " + token;
                        client.Headers["Idempotency-Key"] = idempotencyKey;
                        client.Headers[HttpRequestHeader.ContentType] = "application/json";
                        return client.UploadString(endpoint, "POST", body);
                    }
                }
                catch (WebException exception)
                {
                    HttpWebResponse response = exception.Response as HttpWebResponse;
                    if (response != null)
                    {
                        int statusCode = (int)response.StatusCode;
                        response.Close();
                        if (statusCode != 409 && statusCode != 429 && statusCode < 500)
                            throw new InvalidOperationException(
                                "transfer " + index + " failed with HTTP " + statusCode, exception);
                    }
                }

                Thread.Sleep(Math.Min(1500, 200 + attempt * 50));
            }

            throw new TimeoutException("transfer " + index + " did not recover after container restarts");
        });
    }
}
'@

$composeArgs = @("compose", "--env-file", $environmentFile, "-p", $project, "-f", $composeFile)
function Invoke-Compose {
    param([string[]] $Arguments)
    $allArguments = $composeArgs + $Arguments
    & docker @allArguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

try {
    Set-Content -LiteralPath $environmentFile -Value $environment -Encoding ASCII
    Invoke-Compose @("up", "-d", "--build", "--wait", "--wait-timeout", "300")

    $baseUrl = "http://127.0.0.1:$apiPort"
    $registration = @{
        organization_name = $organizationName
        full_name = "Chaos Test"
        email = $email
        password = $password
    } | ConvertTo-Json -Compress
    $account = Invoke-RestMethod -Method Post -Uri "$baseUrl/api/v1/auth/register" `
        -ContentType "application/json" -Body $registration
    if (-not $account.wallet_id) {
        throw "registration did not return a default wallet"
    }
    $loginBody = @{
        email = $email
        password = $password
    } | ConvertTo-Json -Compress
    $session = Invoke-RestMethod -Method Post -Uri "$baseUrl/api/v1/auth/login" `
        -ContentType "application/json" -Body $loginBody
    $token = $session.access_token
    if (-not $token) {
        throw "login did not return an access token"
    }

    $headers = @{ Authorization = "Bearer $token" }
    $walletBody = @{ currency = "USD"; account_type = "customer" } | ConvertTo-Json -Compress
    $destination = Invoke-RestMethod -Method Post -Uri "$baseUrl/api/v1/wallets" `
        -Headers $headers -ContentType "application/json" -Body $walletBody
    $source = Invoke-RestMethod -Method Post -Uri "$baseUrl/api/v1/wallets" `
        -Headers $headers -ContentType "application/json" -Body $walletBody
    $sourceWallet = [string]$source.id
    $destinationWallet = [string]$destination.id
    $depositBody = '{"amount":1000000}'
    Invoke-RestMethod -Method Post -Uri "$baseUrl/api/v1/wallets/$sourceWallet/deposit" `
        -Headers @{ Authorization = "Bearer $token"; "Idempotency-Key" = "chaos-source-deposit" } `
        -ContentType "application/json" -Body $depositBody | Out-Null

    $start = [System.Threading.ManualResetEventSlim]::new($false)
    $tasks = @()
    for ($i = 0; $i -lt $transferCount; $i++) {
        $tasks += [WalletChaosLoad]::SubmitAsync(
            "$baseUrl/api/v1/transfers",
            $token,
            $sourceWallet,
            $destinationWallet,
            $i,
            $start
        )
    }
    $start.Set()
    Start-Sleep -Milliseconds 25

    $services = @("api-service", "saga-orchestrator", "reconciliation", "nats")
    $random = [System.Random]::new()
    for ($i = $services.Count - 1; $i -gt 0; $i--) {
        $swap = $random.Next($i + 1)
        $temporary = $services[$i]
        $services[$i] = $services[$swap]
        $services[$swap] = $temporary
    }
    foreach ($service in $services) {
        Invoke-Compose @("kill", $service)
        Invoke-Compose @("up", "-d", "--wait", "--wait-timeout", "120", $service)
    }
    [System.Threading.Tasks.Task]::WhenAll([System.Threading.Tasks.Task[]]$tasks).GetAwaiter().GetResult() | Out-Null
    [System.Threading.Tasks.Task]::WhenAll([System.Threading.Tasks.Task[]]$tasks).GetAwaiter().GetResult() | Out-Null
    $start.Dispose()

    Start-Sleep -Seconds 45
    $organizationSQL = "SELECT id FROM organizations WHERE name = '$organizationName'"
    $summarySQL = @"
SELECT
  (SELECT COUNT(*) FROM transfers WHERE organization_id = ($organizationSQL) AND status = 'COMPLETED'),
  (SELECT COUNT(*) FROM transfers WHERE organization_id = ($organizationSQL) AND status <> 'COMPLETED'),
  (SELECT COUNT(*) FROM journal_entries j
   JOIN ledger_entries le ON le.journal_entry_id = j.id
   WHERE j.organization_id = ($organizationSQL) AND j.reference_type = 'transfer'),
  (SELECT COUNT(*) FROM (
     SELECT j.id
     FROM journal_entries j JOIN ledger_entries le ON le.journal_entry_id = j.id
     WHERE j.organization_id = ($organizationSQL)
     GROUP BY j.id
     HAVING SUM(le.amount) FILTER (WHERE le.direction = 'DR')
         <> SUM(le.amount) FILTER (WHERE le.direction = 'CR')
   ) unbalanced),
  COALESCE((SELECT SUM(balance) FROM wallets WHERE organization_id = ($organizationSQL)), 0);
"@
    $psqlArgs = $composeArgs + @(
        "exec", "-T", "postgres", "psql", "-At", "-F", "|",
        "-U", "wallet_test", "-d", "wallet_test", "-c", $summarySQL
    )
    $summary = (& docker @psqlArgs).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "failed reading isolated chaos test ledger summary"
    }
    $values = $summary.Split("|")
    if ($values.Count -ne 5) {
        throw "unexpected chaos ledger summary: $summary"
    }
    $completed = [int]$values[0]
    $incomplete = [int]$values[1]
    $ledgerEntries = [int]$values[2]
    $unbalanced = [int]$values[3]
    $walletTotal = [long]$values[4]
    if ($completed -ne $transferCount -or $incomplete -ne 0 -or
        $ledgerEntries -ne ($transferCount * 3) -or $unbalanced -ne 0 -or $walletTotal -ne 0) {
        throw "chaos verification failed: completed=$completed incomplete=$incomplete ledgerEntries=$ledgerEntries unbalanced=$unbalanced walletTotal=$walletTotal"
    }
    Write-Output "CHAOS PASS: $completed transfers completed; $ledgerEntries ledger entries balanced; wallet sum=$walletTotal"
}
finally {
    if (Test-Path -LiteralPath $environmentFile) {
        $cleanupArgs = $composeArgs + @("down", "--volumes", "--remove-orphans")
        & docker @cleanupArgs | Out-Null
        Remove-Item -LiteralPath $environmentFile -Force
    }
}
