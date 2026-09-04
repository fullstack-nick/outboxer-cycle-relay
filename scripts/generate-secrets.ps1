[CmdletBinding()]
param(
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$secretRoot = Join-Path $root '.secrets'
$environmentFile = Join-Path $secretRoot 'compose.env'
$image = 'eclipse-mosquitto:2.1.2-alpine'

if ((Test-Path -LiteralPath $environmentFile) -and -not $Force) {
    Write-Output "Local secrets already exist at $environmentFile"
    exit 0
}

function New-RandomSecret {
    $bytes = [byte[]]::new(24)
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    return [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', 'A').Replace('/', 'B')
}

$factoryDirectory = Join-Path $secretRoot 'mqtt\factory'
$uplinkDirectory = Join-Path $secretRoot 'mqtt\uplink'
New-Item -ItemType Directory -Force -Path $factoryDirectory, $uplinkDirectory | Out-Null

$values = [ordered]@{
    FACTORY_SIMULATOR_PASSWORD = New-RandomSecret
    FACTORY_EDGE_PASSWORD = New-RandomSecret
    FACTORY_HEALTH_PASSWORD = New-RandomSecret
    UPLINK_EDGE_PASSWORD = New-RandomSecret
    UPLINK_CENTRAL_PASSWORD = New-RandomSecret
    UPLINK_HEALTH_PASSWORD = New-RandomSecret
    POSTGRES_SUPERUSER_PASSWORD = New-RandomSecret
    POSTGRES_APP_PASSWORD = New-RandomSecret
}

function New-PasswordFile {
    param(
        [string]$Directory,
        [array]$Entries
    )

    $resolved = (Resolve-Path -LiteralPath $Directory).Path
    $first = $true
    foreach ($entry in $Entries) {
        $arguments = @('run', '--rm', '--user', '0', '--mount', "type=bind,src=$resolved,dst=/work", $image, 'mosquitto_passwd', '-b')
        if ($first) {
            $arguments += '-c'
            $first = $false
        }
        $arguments += @('/work/passwords', $entry.User, $entry.Password)
        & docker @arguments
        if ($LASTEXITCODE -ne 0) {
            throw "Could not generate MQTT password file for $($entry.User)"
        }
    }
    & docker run --rm --user 0 --mount "type=bind,src=$resolved,dst=/work" $image chmod 0644 /work/passwords
    if ($LASTEXITCODE -ne 0) {
        throw "Could not set MQTT password-file permissions"
    }
}

New-PasswordFile -Directory $factoryDirectory -Entries @(
    @{ User = 'simulator'; Password = $values.FACTORY_SIMULATOR_PASSWORD },
    @{ User = 'edge-factory'; Password = $values.FACTORY_EDGE_PASSWORD },
    @{ User = 'health'; Password = $values.FACTORY_HEALTH_PASSWORD }
)
New-PasswordFile -Directory $uplinkDirectory -Entries @(
    @{ User = 'edge-uplink'; Password = $values.UPLINK_EDGE_PASSWORD },
    @{ User = 'central-uplink'; Password = $values.UPLINK_CENTRAL_PASSWORD },
    @{ User = 'health'; Password = $values.UPLINK_HEALTH_PASSWORD }
)

$lines = $values.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }
[System.IO.File]::WriteAllLines($environmentFile, $lines, [System.Text.UTF8Encoding]::new($false))
Write-Output "Generated local credentials under $secretRoot"
