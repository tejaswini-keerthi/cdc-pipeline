<#
.SYNOPSIS
    Registers (or updates) the Debezium Postgres connector via the Kafka Connect REST API.

.DESCRIPTION
    - Loads credentials from ../.env (keeps secrets out of the JSON).
    - Substitutes ${VAR} placeholders in postgres-connector.json.
    - Waits for Kafka Connect to be reachable.
    - Idempotently PUTs the config to /connectors/<name>/config (create-or-update).
    - Prints the resulting connector status.

.EXAMPLE
    ./register-connector.ps1
    ./register-connector.ps1 -ConnectUrl http://localhost:8083
#>

[CmdletBinding()]
param(
    [string]$ConnectUrl = "http://localhost:8083",
    [string]$ConfigFile = (Join-Path $PSScriptRoot "postgres-connector.json"),
    [string]$EnvFile    = (Join-Path $PSScriptRoot ".." ".env")
)

$ErrorActionPreference = "Stop"

# --- Load .env into a hashtable ---------------------------------------------
$envVars = @{}
if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
            $idx = $line.IndexOf("=")
            $key = $line.Substring(0, $idx).Trim()
            $val = $line.Substring($idx + 1).Trim()
            $envVars[$key] = $val
        }
    }
    Write-Host "Loaded $($envVars.Count) variables from $EnvFile"
} else {
    Write-Warning "$EnvFile not found; relying on placeholders being already filled."
}

# --- Read config and substitute ${VAR} placeholders -------------------------
$json = Get-Content $ConfigFile -Raw
foreach ($key in $envVars.Keys) {
    $json = $json.Replace('${' + $key + '}', $envVars[$key])
}

$doc  = $json | ConvertFrom-Json
$name = $doc.name
# Re-serialize just the config object for the PUT .../config endpoint.
$configBody = $doc.config | ConvertTo-Json -Depth 10

# --- Wait for Kafka Connect to be ready -------------------------------------
Write-Host "Waiting for Kafka Connect at $ConnectUrl ..."
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        Invoke-RestMethod -Uri "$ConnectUrl/connectors" -Method Get -TimeoutSec 5 | Out-Null
        $ready = $true
        break
    } catch {
        Start-Sleep -Seconds 2
    }
}
if (-not $ready) {
    throw "Kafka Connect did not become ready at $ConnectUrl"
}
Write-Host "Kafka Connect is up."

# --- Register / update the connector (idempotent) ---------------------------
Write-Host "Registering connector '$name' ..."
Invoke-RestMethod -Uri "$ConnectUrl/connectors/$name/config" `
    -Method Put -ContentType "application/json" -Body $configBody | Out-Null

Start-Sleep -Seconds 2

# --- Report status ----------------------------------------------------------
$status = Invoke-RestMethod -Uri "$ConnectUrl/connectors/$name/status" -Method Get
Write-Host ""
Write-Host "Connector: $($status.name)"
Write-Host "  Connector state: $($status.connector.state)"
foreach ($task in $status.tasks) {
    Write-Host "  Task $($task.id) state: $($task.state)"
}
Write-Host ""
Write-Host "Topics will appear as: inventory.public.<table>"
Write-Host "List them with: docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list"
