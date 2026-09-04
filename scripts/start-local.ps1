[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $root 'infra\compose.yaml'
$environmentFile = Join-Path $root '.secrets\compose.env'

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker is required but was not found on PATH.'
}

& (Join-Path $PSScriptRoot 'generate-secrets.ps1')
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

& docker compose `
    --project-name outboxer `
    --file $composeFile `
    --env-file $environmentFile `
    --profile simulation `
    up --detach --build --wait
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Output 'Outboxer is ready.'
Write-Output 'Central API:  http://127.0.0.1:18080'
Write-Output 'Edge health:  http://127.0.0.1:18081/actuator/health/readiness'
Write-Output 'Stop safely:  .\scripts\stop-local.ps1'
