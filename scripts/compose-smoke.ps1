[CmdletBinding()]
param(
    [switch]$Reset,
    [switch]$Cleanup
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$env:OUTBOXER_RESET = $Reset.ToString().ToLowerInvariant()
$env:OUTBOXER_CLEANUP = $Cleanup.ToString().ToLowerInvariant()
& (Join-Path $root 'gradlew.bat') composeSmokeTest
exit $LASTEXITCODE
