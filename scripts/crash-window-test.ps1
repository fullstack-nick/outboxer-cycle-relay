[CmdletBinding()]
param(
    [int]$Repetitions = 3,
    [switch]$Reset,
    [switch]$Cleanup
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$env:OUTBOXER_CRASH_REPETITIONS = $Repetitions
$env:OUTBOXER_RESET = $Reset.ToString().ToLowerInvariant()
$env:OUTBOXER_CLEANUP = $Cleanup.ToString().ToLowerInvariant()
& (Join-Path $root 'gradlew.bat') crashWindowTest
exit $LASTEXITCODE
