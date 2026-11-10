param(
  [string]$AndroidSdkRoot = ''
)

$ErrorActionPreference = 'Stop'
Import-Module (Join-Path $PSScriptRoot 'PlatformEnvironment.psm1') -Force

$repositoryRoot = Get-RepositoryRoot
$environmentProfile = Get-PlatformEnvironmentProfile
$sdkRoot = Get-AndroidSdkRoot -AndroidSdkRoot $AndroidSdkRoot
$emulator = Get-AndroidTool -AndroidSdkRoot $sdkRoot -RelativePath 'emulator/emulator.exe'
$adb = Get-AndroidTool -AndroidSdkRoot $sdkRoot -RelativePath 'platform-tools/adb.exe'
$generatedDirectory = Join-Path $repositoryRoot $environmentProfile.Security.GeneratedDirectory
$bootstrapPath = Join-Path $generatedDirectory 'platform-message-security-bootstrap.json'

foreach ($node in $environmentProfile.Android.Nodes) {
  if (-not (Test-ConfiguredAvd -EmulatorPath $emulator -EnvironmentProfile $environmentProfile -Name $node.Name)) {
    throw "AVD $($node.Name) is missing. Run scripts/New-PlatformAvds.ps1 first."
  }
}

if (-not (Test-Path $bootstrapPath)) {
  & (Join-Path $repositoryRoot 'docker/prepare-security.ps1')
  if ($LASTEXITCODE -ne 0) {
    throw 'Initial Docker security preparation failed.'
  }
}

Push-Location $repositoryRoot
try {
  & docker 'compose' '-f' $environmentProfile.Docker.ComposePath 'up' '--detach' '--build'
  if ($LASTEXITCODE -ne 0) {
    throw 'Docker Compose could not start the declared platform services.'
  }
} finally {
  Pop-Location
}

$metroOwner = Get-CimInstance Win32_Process | Where-Object {
  $_.CommandLine -match [regex]::Escape($repositoryRoot) -and
  $_.CommandLine -match '(?i)react-native|metro'
}
if ($metroOwner) {
  Write-Host 'A project Metro process is already running; it was left unchanged.'
} else {
  $portOwner = Get-NetTCPConnection -LocalPort 8081 -State Listen -ErrorAction SilentlyContinue
  if ($portOwner) {
    throw 'Port 8081 is already occupied by a process outside this project. Stop it before starting Metro.'
  }

  Start-Process -FilePath 'cmd.exe' -ArgumentList @('/k', "cd /d `"$repositoryRoot`" && npm start")
  Write-Host 'Started Metro in a visible CMD window on port 8081.'
}

foreach ($node in $environmentProfile.Android.Nodes) {
  # `adb get-state` deliberately exits non-zero until an emulator has been
  # started. Treat only that expected native-command exception as offline.
  $deviceState = ''
  try {
    $deviceState = (& $adb '-s' $node.Serial 'get-state' 2>$null).Trim()
  } catch [System.Management.Automation.RemoteException] {
    $deviceState = ''
  }

  if ($deviceState -eq 'device') {
    Write-Host "$($node.Name) is already online."
  } else {
    Start-Process -FilePath $emulator -ArgumentList @('-avd', $node.Name, '-port', $node.ConsolePort)
    Write-Host "Started $($node.Name) on $($node.Serial)."
  }
}

foreach ($node in $environmentProfile.Android.Nodes) {
  Wait-ConfiguredDeviceBoot -AdbPath $adb -Node $node -TimeoutSeconds $environmentProfile.Android.BootTimeoutSeconds
}

Write-Host 'LIFTED: Docker services, Metro, and all configured AVDs are active.'
Write-Host 'Run Prepare-PlatformAvds.ps1 and Prepare-PlatformSecurity.ps1 before treating the environment as READY.'
