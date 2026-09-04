[CmdletBinding()]
param(
    [switch]$KeepCluster,
    [switch]$SkipBuild,
    [switch]$OfflineOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $ProjectRoot "infra/compose.yaml"
$EnvironmentFile = Join-Path $ProjectRoot ".secrets/compose.env"
$BaseDirectory = Join-Path $ProjectRoot "infra/k8s/base"
$KindConfiguration = Join-Path $ProjectRoot "infra/k8s/kind-config.yaml"
$RuntimeDirectory = Join-Path $ProjectRoot ".outboxer/kind"
$ToolsDirectory = Join-Path $ProjectRoot ".outboxer/tools"
$EvidenceDirectory = Join-Path $ProjectRoot ".outboxer/evidence/generated"
$ClusterName = "outboxer-cycle-relay"
$ClusterContext = "kind-$ClusterName"
$Namespace = "outboxer"
$Kubeconfig = Join-Path $RuntimeDirectory "kubeconfig"
$KindVersion = "v0.33.0"
$KindChecksum = "4b22adaa135368c5a465d56bbd8e520cbea87272a06ca00b6078e7b81515c9fc"
$KindBinary = Join-Path $ToolsDirectory "kind-$KindVersion.exe"
$KubeconformVersion = "v0.8.0"
$KubeconformChecksum = "e3f56102bcf4f50b034a567e2482a1c5330799983ddd655952310211aef73d93"
$KubeconformArchive = Join-Path $ToolsDirectory "kubeconform-$KubeconformVersion-windows-amd64.zip"
$KubeconformDirectory = Join-Path $ToolsDirectory "kubeconform-$KubeconformVersion"
$KubeconformBinary = Join-Path $KubeconformDirectory "kubeconform.exe"
$SchemaCache = Join-Path $RuntimeDirectory "schema-cache"
$StartedAt = [DateTimeOffset]::UtcNow
$GitCommit = (git -C $ProjectRoot rev-parse --short HEAD).Trim()
$EvidenceStamp = $StartedAt.ToString("yyyyMMdd-HHmmss")
$EvidencePath = Join-Path $EvidenceDirectory "kind-$EvidenceStamp.md"
$ExpectedPath = Join-Path (Join-Path $ProjectRoot ".outboxer/evidence") "kind-expected.csv"
$ClusterCreated = $false
$Result = "FAIL"
$FailureMessage = "test did not complete"
$EventCount = 0
$VersionCounts = "not measured"
$ComposeServicesToRestore = [System.Collections.Generic.List[string]]::new()

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)][string]$Executable,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList
    )

    & $Executable @ArgumentList
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $Executable"
    }
}

function Invoke-Captured {
    param(
        [Parameter(Mandatory = $true)][string]$Executable,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList
    )

    $output = & $Executable @ArgumentList 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $Executable"
    }
    return (($output | ForEach-Object { "$_" }) -join "`n").Trim()
}

function Invoke-Compose {
    param([Parameter(Mandatory = $true)][string[]]$ComposeArguments)

    Invoke-Checked -Executable $script:Docker -ArgumentList (@(
            "compose",
            "--project-name", "outboxer",
            "--file", $script:ComposeFile,
            "--env-file", $script:EnvironmentFile
        ) + $ComposeArguments)
}

function Invoke-IsolatedKubectl {
    param([Parameter(Mandatory = $true)][string[]]$KubectlArguments)

    Invoke-Checked -Executable $script:Kubectl -ArgumentList (@(
            "--kubeconfig", $script:Kubeconfig,
            "--context", $script:ClusterContext
        ) + $KubectlArguments)
}

function Invoke-IsolatedKubectlCaptured {
    param([Parameter(Mandatory = $true)][string[]]$KubectlArguments)

    return Invoke-Captured -Executable $script:Kubectl -ArgumentList (@(
            "--kubeconfig", $script:Kubeconfig,
            "--context", $script:ClusterContext
        ) + $KubectlArguments)
}

function Test-ContainerRunning {
    param([Parameter(Mandatory = $true)][string]$Container)

    $state = & $script:Docker inspect --format "{{.State.Running}}" $Container 2>$null
    return $LASTEXITCODE -eq 0 -and "$state".Trim() -eq "true"
}

function Read-EnvironmentFile {
    $values = @{}
    foreach ($line in [IO.File]::ReadAllLines($script:EnvironmentFile)) {
        $trimmed = $line.Trim()
        if ($trimmed.Length -eq 0 -or $trimmed.StartsWith("#")) {
            continue
        }
        $separator = $trimmed.IndexOf("=")
        if ($separator -lt 1) {
            continue
        }
        $values[$trimmed.Substring(0, $separator)] = $trimmed.Substring($separator + 1)
    }
    return $values
}

function Require-EnvironmentValue {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Values,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if (-not $Values.ContainsKey($Name) -or [string]::IsNullOrWhiteSpace($Values[$Name])) {
        throw "Required generated value $Name is missing"
    }
    return $Values[$Name]
}

function Connect-NodeNetwork {
    param(
        [Parameter(Mandatory = $true)][string]$Node,
        [Parameter(Mandatory = $true)][string]$Network
    )

    $networkJson = Invoke-Captured -Executable $script:Docker -ArgumentList @(
        "inspect", "--format", "{{json .NetworkSettings.Networks}}", $Node
    )
    $networkNames = ($networkJson | ConvertFrom-Json).PSObject.Properties.Name
    if ($networkNames -notcontains $Network) {
        Invoke-Checked -Executable $script:Docker -ArgumentList @("network", "connect", $Network, $Node)
    }
}

function Get-ContainerNetworkAddress {
    param(
        [Parameter(Mandatory = $true)][string]$Container,
        [Parameter(Mandatory = $true)][string]$Network
    )

    $networkJson = Invoke-Captured -Executable $script:Docker -ArgumentList @(
        "inspect", "--format", "{{json .NetworkSettings.Networks}}", $Container
    )
    $networkMap = $networkJson | ConvertFrom-Json
    $property = $networkMap.PSObject.Properties[$Network]
    if ($null -eq $property -or [string]::IsNullOrWhiteSpace($property.Value.IPAddress)) {
        throw "$Container is not attached to expected network $Network"
    }
    return [string]$property.Value.IPAddress
}

function Wait-ForCondition {
    param(
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)][scriptblock]$Condition,
        [int]$TimeoutSeconds = 120
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        try {
            if (& $Condition) {
                Write-Host "PASS: $Description"
                return
            }
        }
        catch {
            # Dependencies and pods can reject requests while they are starting.
        }
        Start-Sleep -Seconds 2
    }
    throw "Timed out waiting for $Description"
}

function Write-Evidence {
    $finishedAt = [DateTimeOffset]::UtcNow
    $kindDescription = if (Test-Path -LiteralPath $script:KindBinary) {
        try { Invoke-Captured -Executable $script:KindBinary -ArgumentList @("version") } catch { "unavailable" }
    }
    else {
        "unavailable"
    }
    $validatorDescription = if (Test-Path -LiteralPath $script:KubeconformBinary) {
        try { Invoke-Captured -Executable $script:KubeconformBinary -ArgumentList @("-v") } catch { "unavailable" }
    }
    else {
        "unavailable"
    }
    $cleanup = if ($script:KeepCluster) { "retained by explicit request" } else { "deleted" }
    $safeFailure = $script:FailureMessage.Replace("`r", " ").Replace("`n", " ")
    $report = @"
# Local kind smoke result

- Result: **$($script:Result)**
- Started: ``$($script:StartedAt.ToString("O"))``
- Finished: ``$($finishedAt.ToString("O"))``
- kind: ``$kindDescription``
- Offline validator: ``$validatorDescription``
- Git commit: ``$($script:GitCommit)``
- Cluster: ``$($script:ClusterName)``
- Context: ``$($script:ClusterContext)`` through a dedicated kubeconfig
- Pre-existing kubectl context contacted: **no**
- Tracked manifest validation: strict offline schemas plus isolated-cluster server validation
- Application images: locally built and loaded; no registry push
- Verified event IDs: ``$($script:EventCount)``
- Stored schema counts (v1|v2): ``$($script:VersionCounts)``
- Edge pending rows after receipt: ``0`` when passed
- Temporary cluster cleanup: $cleanup
- Failure detail: $safeFailure
"@
    [IO.File]::WriteAllText($script:EvidencePath, $report, [Text.UTF8Encoding]::new($false))
    Write-Host "kind evidence: $($script:EvidencePath)"
}

New-Item -ItemType Directory -Force -Path $RuntimeDirectory, $ToolsDirectory, $EvidenceDirectory | Out-Null
$Docker = (Get-Command docker -ErrorAction Stop).Source
$Kubectl = (Get-Command kubectl -ErrorAction Stop).Source

try {
    if (-not (Test-Path -LiteralPath $EnvironmentFile)) {
        & (Join-Path $PSScriptRoot "generate-secrets.ps1")
        if ($LASTEXITCODE -ne 0) {
            throw "Local credential generation failed"
        }
    }

    if (-not (Test-Path -LiteralPath $KubeconformArchive) -or
        (Get-FileHash -Algorithm SHA256 -LiteralPath $KubeconformArchive).Hash.ToLowerInvariant() -ne $KubeconformChecksum) {
        $validatorDownload = "$KubeconformArchive.download"
        Invoke-WebRequest -UseBasicParsing -Uri "https://github.com/yannh/kubeconform/releases/download/$KubeconformVersion/kubeconform-windows-amd64.zip" -OutFile $validatorDownload
        $validatorHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $validatorDownload).Hash.ToLowerInvariant()
        if ($validatorHash -ne $KubeconformChecksum) {
            Remove-Item -LiteralPath $validatorDownload -Force
            throw "Downloaded manifest validator did not match the pinned SHA-256"
        }
        Move-Item -LiteralPath $validatorDownload -Destination $KubeconformArchive -Force
    }
    New-Item -ItemType Directory -Force -Path $KubeconformDirectory, $SchemaCache | Out-Null
    Expand-Archive -LiteralPath $KubeconformArchive -DestinationPath $KubeconformDirectory -Force

    $rendered = & $Kubectl kustomize $BaseDirectory
    if ($LASTEXITCODE -ne 0) {
        throw "Kustomize rendering failed"
    }
    $rendered | & $KubeconformBinary -strict -summary -kubernetes-version 1.36.1 -cache $SchemaCache
    if ($LASTEXITCODE -ne 0) {
        throw "Manifest schema cache preparation failed"
    }

    $proxyVariables = @("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY")
    $savedProxyValues = @{}
    foreach ($variable in $proxyVariables) {
        $savedProxyValues[$variable] = [Environment]::GetEnvironmentVariable($variable, "Process")
    }
    try {
        $env:HTTP_PROXY = "http://127.0.0.1:9"
        $env:HTTPS_PROXY = "http://127.0.0.1:9"
        $env:ALL_PROXY = "http://127.0.0.1:9"
        $env:NO_PROXY = ""
        $rendered | & $KubeconformBinary -strict -summary -kubernetes-version 1.36.1 -cache $SchemaCache
        if ($LASTEXITCODE -ne 0) {
            throw "Offline schema validation failed"
        }
    }
    finally {
        foreach ($variable in $proxyVariables) {
            [Environment]::SetEnvironmentVariable($variable, $savedProxyValues[$variable], "Process")
        }
    }
    Write-Host "PASS: strict Kubernetes schemas validated from the local cache with network access disabled"

    if ($OfflineOnly) {
        $Result = "PASS"
        $FailureMessage = "none"
        return
    }

    Invoke-Checked -Executable $Docker -ArgumentList @("version", "--format", "{{.Server.Version}}")

    if (-not (Test-Path -LiteralPath $KindBinary) -or
        (Get-FileHash -Algorithm SHA256 -LiteralPath $KindBinary).Hash.ToLowerInvariant() -ne $KindChecksum) {
        $downloadPath = "$KindBinary.download"
        Invoke-WebRequest -UseBasicParsing -Uri "https://kind.sigs.k8s.io/dl/$KindVersion/kind-windows-amd64" -OutFile $downloadPath
        $downloadHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $downloadPath).Hash.ToLowerInvariant()
        if ($downloadHash -ne $KindChecksum) {
            Remove-Item -LiteralPath $downloadPath -Force
            throw "Downloaded kind binary did not match the pinned SHA-256"
        }
        Move-Item -LiteralPath $downloadPath -Destination $KindBinary -Force
    }
    Write-Host "PASS: pinned kind binary checksum"

    $existingClusters = (Invoke-Captured -Executable $KindBinary -ArgumentList @("get", "clusters")) -split "`n"
    if ($existingClusters -contains $ClusterName) {
        Write-Host "Removing only the pre-existing temporary cluster: $ClusterName"
        Invoke-Checked -Executable $KindBinary -ArgumentList @(
            "delete", "cluster", "--name", $ClusterName, "--kubeconfig", $Kubeconfig
        )
    }
    if (Test-Path -LiteralPath $Kubeconfig) {
        Remove-Item -LiteralPath $Kubeconfig -Force
    }

    Invoke-Checked -Executable $KindBinary -ArgumentList @(
        "create", "cluster",
        "--name", $ClusterName,
        "--config", $KindConfiguration,
        "--kubeconfig", $Kubeconfig,
        "--wait", "5m"
    )
    $ClusterCreated = $true
    $actualContext = Invoke-Captured -Executable $Kubectl -ArgumentList @(
        "--kubeconfig", $Kubeconfig, "config", "current-context"
    )
    if ($actualContext -ne $ClusterContext) {
        throw "Dedicated kubeconfig selected unexpected context $actualContext"
    }
    Invoke-IsolatedKubectl -KubectlArguments @("cluster-info")
    Write-Host "PASS: isolated cluster context verified"

    foreach ($service in @("edge-relay", "central-cycle-service")) {
        $container = if ($service -eq "edge-relay") { "outboxer-edge-relay" } else { "outboxer-central-cycle-service" }
        if (Test-ContainerRunning -Container $container) {
            $ComposeServicesToRestore.Add($service)
        }
    }

    Invoke-Compose -ComposeArguments @("up", "-d", "--no-deps", "factory-mqtt", "uplink-mqtt", "kafka", "postgres")
    if ($ComposeServicesToRestore.Count -gt 0) {
        Invoke-Compose -ComposeArguments (@("stop") + $ComposeServicesToRestore.ToArray())
    }
    if (-not $SkipBuild) {
        Invoke-Compose -ComposeArguments @("--profile", "simulation", "build", "edge-relay", "central-cycle-service", "machine-simulator")
    }

    Invoke-Checked -Executable $KindBinary -ArgumentList @(
        "load", "docker-image", "outboxer/edge-relay:local", "outboxer/central-cycle-service:local", "--name", $ClusterName
    )

    $node = "$ClusterName-control-plane"
    Connect-NodeNetwork -Node $node -Network "outboxer-factory-net"
    Connect-NodeNetwork -Node $node -Network "outboxer-uplink-net"
    Connect-NodeNetwork -Node $node -Network "outboxer-data-net"

    $factoryAddress = Get-ContainerNetworkAddress -Container "outboxer-factory-mqtt" -Network "outboxer-factory-net"
    $uplinkAddress = Get-ContainerNetworkAddress -Container "outboxer-uplink-mqtt" -Network "outboxer-uplink-net"
    $kafkaAddress = Get-ContainerNetworkAddress -Container "outboxer-kafka" -Network "outboxer-data-net"
    $postgresAddress = Get-ContainerNetworkAddress -Container "outboxer-postgres" -Network "outboxer-data-net"

    Invoke-IsolatedKubectl -KubectlArguments @("apply", "-f", (Join-Path $BaseDirectory "namespace.yaml"))

    $dependencyResources = @"
apiVersion: v1
kind: Service
metadata:
  name: factory-mqtt
  namespace: outboxer
spec:
  ports:
    - name: mqtt
      port: 1883
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: factory-mqtt-local
  namespace: outboxer
  labels:
    kubernetes.io/service-name: factory-mqtt
addressType: IPv4
ports:
  - name: mqtt
    protocol: TCP
    port: 1883
endpoints:
  - addresses: ["$factoryAddress"]
---
apiVersion: v1
kind: Service
metadata:
  name: uplink-mqtt
  namespace: outboxer
spec:
  ports:
    - name: mqtt
      port: 1883
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: uplink-mqtt-local
  namespace: outboxer
  labels:
    kubernetes.io/service-name: uplink-mqtt
addressType: IPv4
ports:
  - name: mqtt
    protocol: TCP
    port: 1883
endpoints:
  - addresses: ["$uplinkAddress"]
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: outboxer
spec:
  ports:
    - name: kafka
      port: 9092
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: kafka-local
  namespace: outboxer
  labels:
    kubernetes.io/service-name: kafka
addressType: IPv4
ports:
  - name: kafka
    protocol: TCP
    port: 9092
endpoints:
  - addresses: ["$kafkaAddress"]
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: outboxer
spec:
  ports:
    - name: postgres
      port: 5432
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: postgres-local
  namespace: outboxer
  labels:
    kubernetes.io/service-name: postgres
addressType: IPv4
ports:
  - name: postgres
    protocol: TCP
    port: 5432
endpoints:
  - addresses: ["$postgresAddress"]
"@
    $dependencyResources | & $Kubectl --kubeconfig $Kubeconfig --context $ClusterContext apply -f -
    if ($LASTEXITCODE -ne 0) {
        throw "Local dependency endpoint application failed"
    }

    $environmentValues = Read-EnvironmentFile
    $factoryPassword = Require-EnvironmentValue -Values $environmentValues -Name "FACTORY_EDGE_PASSWORD"
    $edgeUplinkPassword = Require-EnvironmentValue -Values $environmentValues -Name "UPLINK_EDGE_PASSWORD"
    $centralUplinkPassword = Require-EnvironmentValue -Values $environmentValues -Name "UPLINK_CENTRAL_PASSWORD"
    $postgresPassword = Require-EnvironmentValue -Values $environmentValues -Name "POSTGRES_APP_PASSWORD"

    $edgeSecretArguments = @(
        "--kubeconfig", $Kubeconfig, "--context", $ClusterContext, "-n", $Namespace,
        "create", "secret", "generic", "outboxer-edge-credentials",
        "--from-literal=factory-mqtt-password=$factoryPassword",
        "--from-literal=uplink-mqtt-password=$edgeUplinkPassword",
        "--dry-run=client", "-o", "yaml"
    )
    $edgeSecret = & $Kubectl @edgeSecretArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Edge credential object generation failed"
    }
    $edgeSecret | & $Kubectl --kubeconfig $Kubeconfig --context $ClusterContext apply -f -
    if ($LASTEXITCODE -ne 0) {
        throw "Edge credential application failed"
    }

    $centralSecretArguments = @(
        "--kubeconfig", $Kubeconfig, "--context", $ClusterContext, "-n", $Namespace,
        "create", "secret", "generic", "outboxer-central-credentials",
        "--from-literal=uplink-mqtt-password=$centralUplinkPassword",
        "--from-literal=postgres-app-password=$postgresPassword",
        "--dry-run=client", "-o", "yaml"
    )
    $centralSecret = & $Kubectl @centralSecretArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Central credential object generation failed"
    }
    $centralSecret | & $Kubectl --kubeconfig $Kubeconfig --context $ClusterContext apply -f -
    if ($LASTEXITCODE -ne 0) {
        throw "Central credential application failed"
    }

    Invoke-IsolatedKubectl -KubectlArguments @("apply", "--server-side", "--dry-run=server", "-k", $BaseDirectory)
    Invoke-IsolatedKubectl -KubectlArguments @("apply", "-k", $BaseDirectory)
    Invoke-IsolatedKubectl -KubectlArguments @("-n", $Namespace, "rollout", "status", "deployment/outboxer-central", "--timeout=5m")
    Invoke-IsolatedKubectl -KubectlArguments @("-n", $Namespace, "rollout", "status", "deployment/outboxer-edge", "--timeout=5m")
    Write-Host "PASS: edge and central deployments became ready"

    if (Test-Path -LiteralPath $ExpectedPath) {
        Remove-Item -LiteralPath $ExpectedPath -Force
    }
    $runId = "kind-$([Guid]::NewGuid())"
    Invoke-Compose -ComposeArguments @(
        "--profile", "simulation", "run", "--rm", "--no-deps",
        "-e", "SIMULATOR_RUN_ID=$runId",
        "-e", "SIMULATOR_EVENT_COUNT=100",
        "-e", "SIMULATOR_MACHINE_COUNT=100",
        "-e", "SIMULATOR_CYCLE_INTERVAL_SECONDS=1",
        "-e", "SIMULATOR_V2_PERCENTAGE=50",
        "-e", "SIMULATOR_EXPECTED_FILE=/evidence/kind-expected.csv",
        "machine-simulator"
    )
    $ids = [IO.File]::ReadAllLines($ExpectedPath) | ForEach-Object { ($_ -split ",", 2)[0] }
    if ($ids.Count -ne 100) {
        throw "Simulator did not record exactly 100 expected identities"
    }
    $quotedIds = ($ids | ForEach-Object { "'$_'" }) -join ","
    $countSql = "SELECT COUNT(*) FROM cycle_event WHERE event_id IN ($quotedIds)"
    Wait-ForCondition -Description "all kind-routed event IDs reached PostgreSQL" -TimeoutSeconds 180 -Condition {
        $stored = Invoke-Captured -Executable $Docker -ArgumentList @(
            "exec", "outboxer-postgres", "psql", "-X", "-U", "outboxer_admin", "-d", "outboxer", "-At", "-c", $countSql
        )
        return [int]$stored -eq 100
    }
    $EventCount = 100
    $versionSql = "SELECT COUNT(*) FILTER (WHERE schema_version = 1), COUNT(*) FILTER (WHERE schema_version = 2) FROM cycle_event WHERE event_id IN ($quotedIds)"
    $VersionCounts = Invoke-Captured -Executable $Docker -ArgumentList @(
        "exec", "outboxer-postgres", "psql", "-X", "-U", "outboxer_admin", "-d", "outboxer", "-At", "-c", $versionSql
    )
    if ($VersionCounts -ne "50|50") {
        throw "Expected a 50|50 v1/v2 stream, observed $VersionCounts"
    }

    Wait-ForCondition -Description "Kubernetes edge outbox drained after application receipts" -TimeoutSeconds 120 -Condition {
        $pending = Invoke-IsolatedKubectlCaptured -KubectlArguments @(
            "-n", $Namespace, "exec", "deployment/outboxer-edge", "--",
            "sqlite3", "/var/lib/outboxer/edge.db", "SELECT COUNT(*) FROM outbox_event WHERE state = 'PENDING';"
        )
        return [int]$pending -eq 0
    }

    $version2Api = Invoke-IsolatedKubectlCaptured -KubectlArguments @(
        "-n", $Namespace, "exec", "deployment/outboxer-central", "--",
        "curl", "--fail", "--silent", "--header", "X-Tenant-Id: tenant-017",
        "http://127.0.0.1:8080/api/v1/machines/IMM-0001/cycles/latest"
    )
    $version1Api = Invoke-IsolatedKubectlCaptured -KubectlArguments @(
        "-n", $Namespace, "exec", "deployment/outboxer-central", "--",
        "curl", "--fail", "--silent", "--header", "X-Tenant-Id: tenant-017",
        "http://127.0.0.1:8080/api/v1/machines/IMM-0100/cycles/latest"
    )
    if ($version2Api -notmatch '"schemaVersion":2' -or $version2Api -notmatch '"energyConsumptionWh":[0-9]') {
        throw "Kubernetes API did not return the expected v2 energy shape"
    }
    if ($version1Api -notmatch '"schemaVersion":1' -or $version1Api -notmatch '"energyConsumptionWh":null') {
        throw "Kubernetes API did not return the expected nullable v1 energy shape"
    }
    Write-Host "PASS: Kubernetes API returned both compatibility shapes"

    $Result = "PASS"
    $FailureMessage = "none"
}
catch {
    $FailureMessage = $_.Exception.Message
    if ($ClusterCreated) {
        try {
            Invoke-IsolatedKubectl -KubectlArguments @("-n", $Namespace, "get", "pods", "-o", "wide")
            Invoke-IsolatedKubectl -KubectlArguments @("-n", $Namespace, "logs", "deployment/outboxer-edge", "--tail=80")
            Invoke-IsolatedKubectl -KubectlArguments @("-n", $Namespace, "logs", "deployment/outboxer-central", "--tail=80")
        }
        catch {
            Write-Warning "Could not collect complete failure diagnostics"
        }
    }
    throw
}
finally {
    try {
        Write-Evidence
    }
    catch {
        Write-Warning "Could not write kind evidence: $($_.Exception.Message)"
    }

    if ($ClusterCreated -and -not $KeepCluster) {
        try {
            Write-Host "Deleting only temporary kind cluster: $ClusterName"
            Invoke-Checked -Executable $KindBinary -ArgumentList @(
                "delete", "cluster", "--name", $ClusterName, "--kubeconfig", $Kubeconfig
            )
        }
        catch {
            Write-Warning "Temporary cluster cleanup failed: $($_.Exception.Message)"
        }
    }

    if (-not $KeepCluster -and $ComposeServicesToRestore.Count -gt 0) {
        try {
            Invoke-Compose -ComposeArguments (@("start") + $ComposeServicesToRestore.ToArray())
        }
        catch {
            Write-Warning "Could not restore the previously running Compose applications"
        }
    }
}
