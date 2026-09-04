[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
& (Join-Path $root 'gradlew.bat') `
    clean `
    check `
    integrationTest `
    contractCompatibilityTest `
    kubernetesManifestTest `
    licenseReport `
    publicationCheck `
    --no-daemon
exit $LASTEXITCODE
