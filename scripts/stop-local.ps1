[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $root 'infra\compose.yaml'
$environmentFile = Join-Path $root '.secrets\compose.env'

if (-not (Test-Path -LiteralPath $environmentFile)) {
    Write-Output 'No generated Outboxer environment was found; nothing to stop.'
    exit 0
}

& docker compose `
    --project-name outboxer `
    --file $composeFile `
    --env-file $environmentFile `
    --profile simulation `
    down --remove-orphans
exit $LASTEXITCODE
