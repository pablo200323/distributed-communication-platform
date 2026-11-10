param(
  [string]$AndroidSdkRoot = ''
)

$ErrorActionPreference = 'Stop'
Import-Module (Join-Path $PSScriptRoot 'PlatformEnvironment.psm1') -Force

$repositoryRoot = Get-RepositoryRoot
$environmentProfile = Get-PlatformEnvironmentProfile
$sdkRoot = Get-AndroidSdkRoot -AndroidSdkRoot $AndroidSdkRoot
$adb = Get-AndroidTool -AndroidSdkRoot $sdkRoot -RelativePath 'platform-tools/adb.exe'

foreach ($node in $environmentProfile.Android.Nodes) {
  $deviceState = (& $adb '-s' $node.Serial 'get-state' 2>$null).Trim()
  if ($deviceState -eq 'device') {
    & $adb '-s' $node.Serial 'emu' 'kill'
    if ($LASTEXITCODE -ne 0) {
      throw "Could not stop $($node.Serial)."
    }
  }
}

Push-Location $repositoryRoot
try {
  & docker 'compose' '-f' $environmentProfile.Docker.ComposePath 'down'
  if ($LASTEXITCODE -ne 0) {
    throw 'Docker Compose could not stop the declared platform services.'
  }
} finally {
  Pop-Location
}

$metroProcesses = Get-CimInstance Win32_Process | Where-Object {
  $_.CommandLine -match [regex]::Escape($repositoryRoot) -and
  $_.CommandLine -match '(?i)react-native|metro'
}
foreach ($process in $metroProcesses) {
  Stop-Process -Id $process.ProcessId -Force
}

Write-Host 'STOPPED: configured AVDs, declared Docker services, and project Metro were stopped. Docker volumes were preserved.'
