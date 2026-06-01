<#
.SYNOPSIS
    Renders the Iceberg sink SQL from .env and submits it to Flink (exactly-once).

.DESCRIPTION
    The Iceberg catalog DDL needs the DB password and MinIO keys, so the SQL is a
    template with ${VAR} placeholders. This script:
      1. Loads ../.env
      2. Substitutes placeholders into flink/sql/.gen/03_iceberg_sink.sql (gitignored)
      3. Runs sql-client with settings + sources as init scripts (-i) and the
         rendered Iceberg job as -f.
    Because flink/sql is mounted into the JobManager, the rendered file is visible
    in the container at /opt/flink/sql/.gen/03_iceberg_sink.sql.

.EXAMPLE
    ./submit-iceberg-job.ps1
#>

[CmdletBinding()]
param(
    [string]$Container = "cdc-flink-jobmanager",
    [string]$Template  = (Join-Path $PSScriptRoot "sql" "03_iceberg_sink.sql.tmpl"),
    [string]$EnvFile   = (Join-Path $PSScriptRoot ".." ".env")
)

$ErrorActionPreference = "Stop"

# --- Load .env --------------------------------------------------------------
$envVars = @{}
if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
            $idx = $line.IndexOf("=")
            $envVars[$line.Substring(0, $idx).Trim()] = $line.Substring($idx + 1).Trim()
        }
    }
} else {
    throw "$EnvFile not found — cannot render Iceberg catalog credentials."
}

# --- Render template --------------------------------------------------------
$rendered = Get-Content $Template -Raw
foreach ($key in $envVars.Keys) {
    $rendered = $rendered.Replace('${' + $key + '}', $envVars[$key])
}

# Concatenate settings + sources + rendered job into ONE -f script. sql-client
# does not reliably honor multiple -i init files, so a single script in one
# session is the robust way to make the sources visible to the INSERTs.
$settings = Get-Content (Join-Path $PSScriptRoot "sql" "00_settings.sql") -Raw
$sources  = Get-Content (Join-Path $PSScriptRoot "sql" "01_sources.sql") -Raw
$sql = $settings + "`n" + $sources + "`n" + $rendered

$genDir = Join-Path $PSScriptRoot "sql" ".gen"
New-Item -ItemType Directory -Force -Path $genDir | Out-Null
$genFile = Join-Path $genDir "iceberg_job_full.sql"
# Write without BOM so Flink's SQL parser is happy.
[System.IO.File]::WriteAllText($genFile, $sql, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "Rendered Iceberg job -> $genFile"

# --- Submit -----------------------------------------------------------------
Write-Host "Submitting Iceberg sink job via sql-client in $Container ..."
docker exec $Container /opt/flink/bin/sql-client.sh `
    -f /opt/flink/sql/.gen/iceberg_job_full.sql

if ($LASTEXITCODE -ne 0) {
    throw "sql-client exited with code $LASTEXITCODE"
}

Write-Host ""
Write-Host "Iceberg sink job submitted. Watch it at http://localhost:8081"
Write-Host "Data lands in MinIO bucket 'warehouse' (console: http://localhost:9001)."
Write-Host "Each completed checkpoint (~10s) commits one Iceberg snapshot."
