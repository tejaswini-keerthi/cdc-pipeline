<#
.SYNOPSIS
    Submits the Flink CDC upsert/dedup job via the SQL Client inside the JobManager.

.DESCRIPTION
    Runs sql-client with the settings + source DDL as init scripts (-i) and the
    upsert/print job as the script file (-f). DDL lives in-session, so the INSERTs
    can see the tables. The STATEMENT SET submits all four upserts as one job.

.PARAMETER JobFile
    Which job script to run (relative to /opt/flink/sql in the container).
    Default is the Step 4 print job; Step 5 will pass the Iceberg job.

.EXAMPLE
    ./submit-job.ps1
    ./submit-job.ps1 -JobFile 03_iceberg_sink.sql
#>

[CmdletBinding()]
param(
    [string]$Container = "cdc-flink-jobmanager",
    [string]$JobFile   = "02_upsert_print.sql"
)

$ErrorActionPreference = "Stop"

Write-Host "Submitting Flink job '$JobFile' via sql-client in $Container ..."
docker exec $Container /opt/flink/bin/sql-client.sh `
    -i /opt/flink/sql/00_settings.sql `
    -i /opt/flink/sql/01_sources.sql `
    -f "/opt/flink/sql/$JobFile"

if ($LASTEXITCODE -ne 0) {
    throw "sql-client exited with code $LASTEXITCODE"
}

Write-Host ""
Write-Host "Job submitted. Check it at http://localhost:8081 (Running Jobs)."
Write-Host "Print output (Step 4 sink) appears in the TaskManager logs:"
Write-Host "    docker logs -f cdc-flink-taskmanager"
