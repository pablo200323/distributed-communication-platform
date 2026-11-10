Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-RepositoryRoot {
  return Split-Path $PSScriptRoot -Parent
}

function Get-PlatformEnvironmentProfile {
  return Import-PowerShellDataFile (Join-Path $PSScriptRoot 'platform-environment.psd1')
}

function Get-AndroidSdkRoot {
  param([string]$AndroidSdkRoot = '')

  $candidates = @(
    $AndroidSdkRoot,
    $env:ANDROID_HOME,
    $env:ANDROID_SDK_ROOT
  ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

  foreach ($candidate in $candidates) {
    if (Test-Path (Join-Path $candidate 'platform-tools/adb.exe')) {
      return [System.IO.Path]::GetFullPath($candidate)
    }
  }

  $adb = Get-Command adb.exe -ErrorAction SilentlyContinue
  if ($adb) {
    return Split-Path (Split-Path $adb.Source -Parent) -Parent
  }

  throw 'Android SDK was not found. Set ANDROID_HOME, ANDROID_SDK_ROOT, or pass -AndroidSdkRoot.'
}

function Get-AndroidAvdHome {
  $avdHome = if ($env:ANDROID_AVD_HOME) {
    $env:ANDROID_AVD_HOME
  } else {
    Join-Path $env:USERPROFILE '.android/avd'
  }

  if (-not (Test-Path $avdHome)) {
    throw "Android AVD home does not exist: $avdHome"
  }

  return [System.IO.Path]::GetFullPath($avdHome)
}

function Get-AndroidTool {
  param(
    [string]$AndroidSdkRoot,
    [string]$RelativePath
  )

  $path = Join-Path $AndroidSdkRoot $RelativePath
  if (-not (Test-Path $path)) {
    throw "Required Android SDK tool is unavailable: $path"
  }

  return $path
}

function Set-AndroidSdkLocalProperties {
  param([string]$AndroidSdkRoot)

  $propertiesPath = Join-Path (Get-RepositoryRoot) 'android/local.properties'
  $escapedSdkRoot = $AndroidSdkRoot.Replace('\', '\\').Replace(':', '\:')
  Set-Content -Path $propertiesPath -Value "sdk.dir=$escapedSdkRoot" -Encoding ascii
}

function Invoke-CheckedCommand {
  param(
    [string]$Command,
    [string[]]$Arguments,
    [string]$FailureMessage
  )

  & $Command @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "$FailureMessage (exit code $LASTEXITCODE)."
  }
}

function Get-ConfiguredAvdNames {
  param([hashtable]$EnvironmentProfile)

  return @($EnvironmentProfile.Android.Nodes | ForEach-Object { $_.Name })
}

function Test-ConfiguredAvd {
  param(
    [string]$EmulatorPath,
    [hashtable]$EnvironmentProfile,
    [string]$Name
  )

  if ($Name -notin (Get-ConfiguredAvdNames $EnvironmentProfile)) {
    throw "AVD '$Name' is not part of the fixed platform profile."
  }

  $available = & $EmulatorPath '-list-avds'
  if ($LASTEXITCODE -ne 0) {
    throw 'Unable to list Android Virtual Devices.'
  }

  return $available -contains $Name
}

function Wait-ConfiguredDeviceBoot {
  param(
    [string]$AdbPath,
    [hashtable]$Node,
    [int]$TimeoutSeconds
  )

  & $AdbPath '-s' $Node.Serial 'wait-for-device'
  if ($LASTEXITCODE -ne 0) {
    throw "ADB could not reach $($Node.Serial)."
  }

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    $bootState = (& $AdbPath '-s' $Node.Serial 'shell' 'getprop' 'sys.boot_completed' 2>$null).Trim()
    if ($bootState -eq '1') {
      return
    }

    Start-Sleep -Seconds 2
  }

  throw "Timed out waiting for $($Node.Serial) to boot after $TimeoutSeconds seconds."
}

function Get-ConfiguredWlan0Address {
  param(
    [string]$AdbPath,
    [hashtable]$Node
  )

  $output = & $AdbPath '-s' $Node.Serial 'shell' 'ip' '-f' 'inet' 'addr' 'show' 'wlan0'
  if ($LASTEXITCODE -ne 0) {
    throw "Could not read wlan0 from $($Node.Serial)."
  }

  $addresses = @(
    [regex]::Matches(($output -join "`n"), '\binet\s+(\d{1,3}(?:\.\d{1,3}){3})/\d{1,2}\b') |
      ForEach-Object { $_.Groups[1].Value } |
      Where-Object { $_ -notmatch '^(0|127)\.' } |
      Select-Object -Unique
  )

  if ($addresses.Count -ne 1) {
    throw "$($Node.Serial) must expose exactly one peer-reachable wlan0 IPv4 address."
  }

  return $addresses[0]
}

function Assert-AdbRoot {
  param(
    [string]$AdbPath,
    [hashtable]$Node
  )

  & $AdbPath '-s' $Node.Serial 'root'
  if ($LASTEXITCODE -ne 0) {
    throw "$($Node.Serial) does not permit adb root, required to install the test CA and Termux TLS files."
  }

  & $AdbPath '-s' $Node.Serial 'wait-for-device'
  if ($LASTEXITCODE -ne 0) {
    throw "$($Node.Serial) did not reconnect after adb root."
  }
}

function Write-AppPrivateFile {
  param(
    [string]$AdbPath,
    [hashtable]$Node,
    [string]$PackageId,
    [string]$RelativePath,
    [string]$Content
  )

  $incomingPath = "$RelativePath.incoming"
  $parentDirectory = Split-Path -Path $RelativePath -Parent
  if ($parentDirectory) {
    & $AdbPath '-s' $Node.Serial 'shell' 'run-as' $PackageId 'mkdir' '-p' $parentDirectory
    if ($LASTEXITCODE -ne 0) {
      throw "Could not create $parentDirectory in the private files directory of $($Node.Serial)."
    }
  }

  $Content | & $AdbPath '-s' $Node.Serial 'exec-in' 'run-as' $PackageId 'sh' '-c' "cat > '$incomingPath'"
  if ($LASTEXITCODE -ne 0) {
    throw "Could not write $RelativePath in the private files directory of $($Node.Serial)."
  }

  & $AdbPath '-s' $Node.Serial 'shell' 'run-as' $PackageId 'mv' '-f' $incomingPath $RelativePath
  if ($LASTEXITCODE -ne 0) {
    throw "Could not finalize $RelativePath in the private files directory of $($Node.Serial)."
  }
}

function Test-AppInstalled {
  param(
    [string]$AdbPath,
    [hashtable]$Node,
    [string]$PackageId
  )

  $packages = & $AdbPath '-s' $Node.Serial 'shell' 'pm' 'list' 'packages' $PackageId
  if ($LASTEXITCODE -ne 0 -or "package:$PackageId" -notin $packages) {
    throw "$PackageId is not installed on $($Node.Serial)."
  }
}

Export-ModuleMember -Function @(
  'Assert-AdbRoot',
  'Get-AndroidSdkRoot',
  'Get-AndroidAvdHome',
  'Get-AndroidTool',
  'Get-ConfiguredAvdNames',
  'Get-ConfiguredWlan0Address',
  'Get-PlatformEnvironmentProfile',
  'Get-RepositoryRoot',
  'Invoke-CheckedCommand',
  'Set-AndroidSdkLocalProperties',
  'Test-AppInstalled',
  'Test-ConfiguredAvd',
  'Wait-ConfiguredDeviceBoot',
  'Write-AppPrivateFile'
)
