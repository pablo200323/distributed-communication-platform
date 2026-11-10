param(
  [Parameter(Mandatory = $true)]
  [string]$TermuxApkPath,
  [string]$AppApkPath = 'android/app/build/outputs/apk/debug/app-debug.apk',
  [string]$AndroidSdkRoot = '',
  [switch]$ResetApplicationData
)

$ErrorActionPreference = 'Stop'
Import-Module (Join-Path $PSScriptRoot 'PlatformEnvironment.psm1') -Force

function New-TopologyEntry {
  param([hashtable]$Node)

  return [ordered]@{
    nodeId = $Node.NodeId
    ip = $Node.PeerHost
    port = $Node.PeerWssPort
    bridgePort = $Node.PeerMqttsPort
  }
}

function New-NodeTopology {
  param(
    [hashtable]$Node,
    [hashtable]$EnvironmentProfile
  )

  $primaryEntries = @(
    $EnvironmentProfile.Android.Nodes |
      Where-Object { $_.InitializationProfile -eq 'PRIMARY_BROKER' -and $_.NodeId -ne $Node.NodeId } |
      ForEach-Object { New-TopologyEntry -Node $_ }
  ) + @(
    $EnvironmentProfile.Docker.Brokers |
      Where-Object { $_.BrokerType -eq 'PRIMARY' } |
      ForEach-Object {
        [ordered]@{
          nodeId = $_.BrokerId
          ip = $_.Host
          port = $_.Port
          bridgePort = $_.BridgePort
        }
      }
  )
  $temporaryEntries = @(
    $EnvironmentProfile.Android.Nodes |
      Where-Object { $_.InitializationProfile -eq 'BASE_TEMPORARY' -and $_.NodeId -ne $Node.NodeId } |
      ForEach-Object { New-TopologyEntry -Node $_ }
  ) + @(
    $EnvironmentProfile.Docker.Brokers |
      Where-Object { $_.BrokerType -eq 'TEMPORARY' } |
      ForEach-Object {
        [ordered]@{
          nodeId = $_.BrokerId
          ip = $_.Host
          port = $_.Port
          bridgePort = $_.BridgePort
        }
      }
  )
  $observerEntries = @(
    $EnvironmentProfile.Docker.Observers | ForEach-Object {
      [ordered]@{
        nodeId = $_.NodeId
        ip = $_.Host
        port = $_.Port
      }
    }
  )

  return [ordered]@{
    schemaVersion = 1
    savedAt = [DateTime]::UtcNow.ToString('o')
    node = [ordered]@{
      nodeId = $Node.NodeId
      initializationProfile = $Node.InitializationProfile
      bridgeCapable = $Node.BridgeCapable
      localBroker = [ordered]@{
        advertisedHost = 'localhost'
        port = 9001
      }
    }
    assignedPrimaryBrokerId = $Node.AssignedPrimaryBrokerId
    messageSecurityRequired = $true
    primaryBrokers = $primaryEntries
    temporaryBrokers = $temporaryEntries
    primarySubscribers = $observerEntries
    operationalTopicFilters = @('operational/#')
  }
}

$repositoryRoot = Get-RepositoryRoot
$environmentProfile = Get-PlatformEnvironmentProfile
$sdkRoot = Get-AndroidSdkRoot -AndroidSdkRoot $AndroidSdkRoot
$adb = Get-AndroidTool -AndroidSdkRoot $sdkRoot -RelativePath 'platform-tools/adb.exe'
$resolvedTermuxApk = [System.IO.Path]::GetFullPath($TermuxApkPath)
$resolvedAppApk = if ([System.IO.Path]::IsPathRooted($AppApkPath)) {
  [System.IO.Path]::GetFullPath($AppApkPath)
} else {
  [System.IO.Path]::GetFullPath((Join-Path $repositoryRoot $AppApkPath))
}

if (-not (Test-Path $resolvedTermuxApk)) {
  throw "Termux APK was not found: $resolvedTermuxApk"
}
if (-not (Test-Path $resolvedAppApk)) {
  throw "Application APK was not found: $resolvedAppApk. Build the debug APK before preparation."
}

foreach ($node in $environmentProfile.Android.Nodes) {
  Wait-ConfiguredDeviceBoot -AdbPath $adb -Node $node -TimeoutSeconds $environmentProfile.Android.BootTimeoutSeconds

  if ($node.RequiresTermux) {
    if (-not (Test-ConfiguredPeerRedirection -AdbPath $adb -Node $node -HostPort $node.PeerWssPort -GuestPort 9001)) {
      throw "$($node.Name) does not expose its declared WSS peer endpoint. Run Start-PlatformEnvironment.ps1 first."
    }
    if (-not (Test-ConfiguredPeerRedirection -AdbPath $adb -Node $node -HostPort $node.PeerMqttsPort -GuestPort 8883)) {
      throw "$($node.Name) does not expose its declared MQTTS peer endpoint. Run Start-PlatformEnvironment.ps1 first."
    }
  }
}

foreach ($node in $environmentProfile.Android.Nodes) {
  if ($node.RequiresTermux) {
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'install', '-r', $resolvedTermuxApk) -FailureMessage "Could not install Termux on $($node.Serial)"
  }
  Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'install', '-r', $resolvedAppApk) -FailureMessage "Could not install the platform application on $($node.Serial)"

  if ($ResetApplicationData) {
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'pm', 'clear', $environmentProfile.Android.PackageId) -FailureMessage "Could not clear prior app data on $($node.Serial)"
  }

  Test-AppInstalled -AdbPath $adb -Node $node -PackageId $environmentProfile.Android.PackageId
  $topology = New-NodeTopology -Node $node -EnvironmentProfile $environmentProfile
  $topologyJson = $topology | ConvertTo-Json -Depth 8
  Write-AppPrivateFile -AdbPath $adb -Node $node -PackageId $environmentProfile.Android.PackageId -RelativePath 'files/harness_topology.json' -Content $topologyJson

  if ($node.RequiresTermux) {
    $provisioningDirectory = $environmentProfile.Security.TermuxProvisioningDirectory
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'mkdir', '-p', $provisioningDirectory) -FailureMessage "Could not create the Termux provisioning directory on $($node.Serial)"
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'push', (Join-Path $PSScriptRoot 'provision-termux.sh'), "$provisioningDirectory/provision-termux.sh") -FailureMessage "Could not deliver the Termux provisioning script to $($node.Serial)"
  }
}

Write-Host 'AVD application installation and static topology delivery completed.'
Write-Host 'Next run Prepare-PlatformSecurity.ps1, then execute the delivered Termux provisioning script once on the primary and temporary AVDs.'
