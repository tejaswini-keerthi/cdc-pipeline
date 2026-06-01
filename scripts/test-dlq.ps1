<#
.SYNOPSIS
    Demonstrates the DLQ + alerting loop by injecting malformed events.

.DESCRIPTION
    Produces a few invalid records onto a CDC topic. The dlq-router routes them to
    dlq.inventory and increments cdc_dlq_events_total; Flink skips them
    (ignore-parse-errors) without crashing. After ~10s Prometheus fires
    DLQEventsDetected and Alertmanager posts to the webhook (check its logs).

.EXAMPLE
    ./test-dlq.ps1
#>

[CmdletBinding()]
param(
    [string]$Topic = "inventory.public.orders",
    [int]$Count = 3
)

$ErrorActionPreference = "Stop"

Write-Host "Producing $Count malformed record(s) to $Topic ..."
$bad = @()
for ($i = 1; $i -le $Count; $i++) {
    $bad += "this-is-not-valid-debezium-json-$i"
}
$payload = ($bad -join "`n")

$payload | docker exec -i cdc-kafka kafka-console-producer `
    --bootstrap-server localhost:9092 --topic $Topic

Write-Host ""
Write-Host "Injected. Now observe the DLQ loop:"
Write-Host '  1. DLQ router routed them:   docker logs --tail 20 cdc-dlq-router'
Write-Host '  2. DLQ topic contents:       docker exec cdc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic dlq.inventory --from-beginning --timeout-ms 5000'
Write-Host '  3. Metric:                   (Invoke-WebRequest http://localhost:8001/metrics).Content | Select-String cdc_dlq_events_total'
Write-Host '  4. Alert (after ~10-15s):    docker logs --tail 20 cdc-alert-webhook'
Write-Host '     or the Alertmanager UI:   http://localhost:9093'
