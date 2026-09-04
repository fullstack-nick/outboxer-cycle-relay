[CmdletBinding()]
param(
    [int]$DurationSeconds = 60,
    [switch]$Reset,
    [switch]$Cleanup
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$env:OUTBOXER_LOAD_SECONDS = $DurationSeconds
$env:OUTBOXER_RESET = $Reset.ToString().ToLowerInvariant()
$env:OUTBOXER_CLEANUP = $Cleanup.ToString().ToLowerInvariant()
& (Join-Path $root 'gradlew.bat') loadTest
exit $LASTEXITCODE
