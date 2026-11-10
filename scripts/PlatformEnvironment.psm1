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

function Get-ConfiguredEmulatorRedirections {
  param(
    [string]$AdbPath,
    [hashtable]$Node
  )

  $output = & $AdbPath '-s' $Node.Serial 'emu' 'redir' 'list'
  if ($LASTEXITCODE -ne 0) {
    throw "Could not inspect emulator port redirections for $($Node.Serial)."
  }

  return @(
    [regex]::Matches(($output -join "`n"), '(?m)^\s*(?:ipv4\s+)?tcp:(\d+)\s*=>\s*(\d+)\s*$') |
      ForEach-Object {
        [ordered]@{
          HostPort = [int]$_.Groups[1].Value
          GuestPort = [int]$_.Groups[2].Value
        }
      }
  )
}

function Test-ConfiguredPeerRedirection {
  param(
    [string]$AdbPath,
    [hashtable]$Node,
    [int]$HostPort,
    [int]$GuestPort
  )

  $redirections = Get-ConfiguredEmulatorRedirections -AdbPath $AdbPath -Node $Node
  return [bool]($redirections | Where-Object {
    $_.HostPort -eq $HostPort -and $_.GuestPort -eq $GuestPort
  })
}

function Ensure-ConfiguredPeerRedirection {
  param(
    [string]$AdbPath,
    [hashtable]$Node,
    [int]$HostPort,
    [int]$GuestPort
  )

  $redirections = Get-ConfiguredEmulatorRedirections -AdbPath $AdbPath -Node $Node
  $existing = @($redirections | Where-Object { $_.HostPort -eq $HostPort })
  if ($existing.Count -gt 0) {
    if ($existing[0].GuestPort -eq $GuestPort) {
      return
    }

    throw "Host port $HostPort on $($Node.Serial) is already redirected to guest port $($existing[0].GuestPort), not $GuestPort."
  }

  Invoke-CheckedCommand -Command $AdbPath -Arguments @(
    '-s', $Node.Serial, 'emu', 'redir', 'add', "tcp:$HostPort`:$GuestPort"
  ) -FailureMessage "Could not create port redirection tcp:$HostPort=>$GuestPort for $($Node.Serial)"

  if (-not (Test-ConfiguredPeerRedirection -AdbPath $AdbPath -Node $Node -HostPort $HostPort -GuestPort $GuestPort)) {
    throw "Port redirection tcp:$HostPort=>$GuestPort for $($Node.Serial) was not reported after creation."
  }
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

  $Content | & $AdbPath '-s' $Node.Serial 'exec-in' 'run-as' $PackageId 'sh' '-c' "cd '/data/user/0/$PackageId' && cat > '$incomingPath'"
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
  'Get-ConfiguredEmulatorRedirections',
  'Get-ConfiguredWlan0Address',
  'Get-PlatformEnvironmentProfile',
  'Get-RepositoryRoot',
  'Invoke-CheckedCommand',
  'Set-AndroidSdkLocalProperties',
  'Test-AppInstalled',
  'Test-ConfiguredAvd',
  'Test-ConfiguredPeerRedirection',
  'Ensure-ConfiguredPeerRedirection',
  'Wait-ConfiguredDeviceBoot',
  'Write-AppPrivateFile'
)
