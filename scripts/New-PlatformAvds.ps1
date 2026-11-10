param(
  [string]$AndroidSdkRoot = ''
)

$ErrorActionPreference = 'Stop'
Import-Module (Join-Path $PSScriptRoot 'PlatformEnvironment.psm1') -Force

$environmentProfile = Get-PlatformEnvironmentProfile
$sdkRoot = Get-AndroidSdkRoot -AndroidSdkRoot $AndroidSdkRoot
$avdHome = Get-AndroidAvdHome
$emulator = Get-AndroidTool -AndroidSdkRoot $sdkRoot -RelativePath 'emulator/emulator.exe'
$avdManager = Get-AndroidTool -AndroidSdkRoot $sdkRoot -RelativePath 'cmdline-tools/latest/bin/avdmanager.bat'
$existingNames = @(& $emulator '-list-avds')
Set-AndroidSdkLocalProperties -AndroidSdkRoot $sdkRoot

if ($LASTEXITCODE -ne 0) {
  throw 'Unable to list Android Virtual Devices.'
}

foreach ($node in $environmentProfile.Android.Nodes) {
  if ($node.Name -in $existingNames) {
    Write-Host "AVD $($node.Name) already exists; applying the declared profile settings."
  } else {
    Write-Host "Creating $($node.Name) with $($environmentProfile.Android.SystemImage)."
    'no' | & $avdManager 'create' 'avd' '-n' $node.Name '-k' $environmentProfile.Android.SystemImage '-d' $environmentProfile.Android.DeviceProfile
    if ($LASTEXITCODE -ne 0) {
      throw "Could not create AVD $($node.Name). Confirm that the declared system image and device profile are installed."
    }
  }

  $avdIniPath = Join-Path $avdHome "$($node.Name).ini"
  if (-not (Test-Path $avdIniPath)) {
    throw "AVD $($node.Name) was created without its expected definition file."
  }
  $avdPath = (Get-Content $avdIniPath | Where-Object { $_ -like 'path=*' } | Select-Object -First 1) -replace '^path=', ''
  if (-not $avdPath) {
    throw "AVD $($node.Name) definition does not declare its storage path."
  }
  $configPath = Join-Path $avdPath 'config.ini'
  if (-not (Test-Path $configPath)) {
    throw "AVD $($node.Name) was created without its expected config.ini."
  }

  $settings = @{
    'hw.cpu.ncore' = '2'
    'hw.ramSize' = '2048'
    'disk.dataPartition.size' = '10G'
    'sdcard.size' = '512M'
    'vm.heapSize' = '228'
    'hw.gpu.mode' = 'auto'
    'runtime.network.latency' = 'none'
    'runtime.network.speed' = 'full'
    'showDeviceFrame' = 'yes'
    'hw.keyboard' = 'yes'
    'hw.keyboard.lid' = 'no'
  }
  $lines = Get-Content $configPath
  foreach ($key in $settings.Keys) {
    $replacement = "$key=$($settings[$key])"
    $matches = @($lines | Select-String -SimpleMatch "$key=")
    if ($matches.Count -eq 0) {
      $lines += $replacement
    } else {
      $lines[$matches[0].LineNumber - 1] = $replacement
    }
  }
  Set-Content -Path $configPath -Value $lines -Encoding ascii
}

Write-Host 'Configured AVDs are present. No AVD was deleted or recreated.'
