[CmdletBinding()]
param(
    [int]$OutageSeconds = 600,
    [switch]$Reset,
    [switch]$Cleanup
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$env:OUTBOXER_OUTAGE_SECONDS = $OutageSeconds
$env:OUTBOXER_RESET = $Reset.ToString().ToLowerInvariant()
$env:OUTBOXER_CLEANUP = $Cleanup.ToString().ToLowerInvariant()
& (Join-Path $root 'gradlew.bat') outageTest
exit $LASTEXITCODE
