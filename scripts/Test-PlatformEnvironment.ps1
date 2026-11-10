param(
  [string]$AndroidSdkRoot = ''
)

$ErrorActionPreference = 'Stop'
Import-Module (Join-Path $PSScriptRoot 'PlatformEnvironment.psm1') -Force

$repositoryRoot = Get-RepositoryRoot
$environmentProfile = Get-PlatformEnvironmentProfile
$sdkRoot = Get-AndroidSdkRoot -AndroidSdkRoot $AndroidSdkRoot
$adb = Get-AndroidTool -AndroidSdkRoot $sdkRoot -RelativePath 'platform-tools/adb.exe'
$expectedDockerServices = @('broker-1', 'broker-2', 'broker-3', 'observer-1', 'observer-2')
$bootstrapPath = Join-Path $repositoryRoot "$($environmentProfile.Security.GeneratedDirectory)/platform-message-security-bootstrap.json"

Push-Location $repositoryRoot
try {
  $runningDockerServices = @(& docker 'compose' '-f' $environmentProfile.Docker.ComposePath 'ps' '--services' '--status' 'running')
  if ($LASTEXITCODE -ne 0) {
    throw 'Could not inspect the declared Docker services.'
  }
} finally {
  Pop-Location
}

$missingDockerServices = @($expectedDockerServices | Where-Object { $_ -notin $runningDockerServices })
$metroAvailable = [bool](Get-NetTCPConnection -LocalPort 8081 -State Listen -ErrorAction SilentlyContinue)
$offlineNodes = @()
foreach ($node in $environmentProfile.Android.Nodes) {
  $deviceState = (& $adb '-s' $node.Serial 'get-state' 2>$null).Trim()
  if ($deviceState -ne 'device') {
    $offlineNodes += $node.Name
    continue
  }
  $bootState = (& $adb '-s' $node.Serial 'shell' 'getprop' 'sys.boot_completed' 2>$null).Trim()
  if ($bootState -ne '1') {
    $offlineNodes += $node.Name
  }
}

if ($missingDockerServices.Count -gt 0 -or -not $metroAvailable -or $offlineNodes.Count -gt 0) {
  $details = @()
  if ($missingDockerServices.Count -gt 0) { $details += "Docker services missing: $($missingDockerServices -join ', ')" }
  if (-not $metroAvailable) { $details += 'Metro port 8081 is not listening' }
  if ($offlineNodes.Count -gt 0) { $details += "AVDs not booted: $($offlineNodes -join ', ')" }
  Write-Host "NOT_LIFTED: $($details -join ' | ')"
  exit 1
}

Write-Host 'LIFTED: Docker services, Metro, and all configured AVDs are active.'

$missingPreparation = @()
if (-not (Test-Path $bootstrapPath)) {
  $missingPreparation += 'host message-security bootstrap is missing'
}
foreach ($node in $environmentProfile.Android.Nodes) {
  try {
    Test-AppInstalled -AdbPath $adb -Node $node -PackageId $environmentProfile.Android.PackageId
  } catch {
    $missingPreparation += "$($node.Name): platform application is not installed"
    continue
  }

  & $adb '-s' $node.Serial 'shell' 'run-as' $environmentProfile.Android.PackageId 'test' '-s' 'files/harness_topology.json'
  if ($LASTEXITCODE -ne 0) {
    $missingPreparation += "$($node.Name): topology file is missing"
  }

  if ($node.RequiresTermux) {
    & $adb '-s' $node.Serial 'shell' 'test' '-s' "$($environmentProfile.Security.TermuxProvisioningDirectory)/termux-ready"
    if ($LASTEXITCODE -ne 0) {
      $missingPreparation += "$($node.Name): Termux provisioning has not completed"
    }

    $packageDetails = (& $adb '-s' $node.Serial 'shell' 'dumpsys' 'package' $environmentProfile.Android.PackageId) -join "`n"
    if ($packageDetails -notmatch 'com\.termux\.permission\.RUN_COMMAND:\s+granted=true') {
      $missingPreparation += "$($node.Name): Termux:RunCommand permission is not granted"
    }
  }
}

if ($missingPreparation.Count -gt 0) {
  Write-Host "LIFTED_NOT_READY: $($missingPreparation -join ' | ')"
  exit 1
}

Write-Host 'PREPARED_FOR_MANUAL_INITIALIZATION: all verifiable prerequisites are present. Initialize each declared profile in the application to make its managers operational.'
