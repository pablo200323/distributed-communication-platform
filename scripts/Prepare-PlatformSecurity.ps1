param(
  [string]$AndroidSdkRoot = '',
  [string]$OpenSslPath = ''
)

$ErrorActionPreference = 'Stop'
Import-Module (Join-Path $PSScriptRoot 'PlatformEnvironment.psm1') -Force

function Resolve-OpenSsl {
  param([string]$RequestedPath)

  if ($RequestedPath) {
    if (-not (Test-Path $RequestedPath)) {
      throw "OpenSSL path does not exist: $RequestedPath"
    }
    return [System.IO.Path]::GetFullPath($RequestedPath)
  }

  $command = Get-Command openssl.exe -ErrorAction SilentlyContinue
  if ($command) {
    return $command.Source
  }

  $gitOpenSsl = 'C:\Program Files\Git\usr\bin\openssl.exe'
  if (Test-Path $gitOpenSsl) {
    return $gitOpenSsl
  }

  throw 'OpenSSL was not found in PATH or in the Git for Windows default location.'
}

function New-AndroidBrokerCertificate {
  param(
    [string]$OpenSsl,
    [string]$CertificateDirectory,
    [string]$CertificateAuthorityPath,
    [string]$CertificateAuthorityKeyPath,
    [string]$NodeId,
    [string]$WlanAddress
  )

  New-Item -ItemType Directory -Force -Path $CertificateDirectory | Out-Null
  $configurationPath = Join-Path $CertificateDirectory 'openssl.cnf'
  $keyPath = Join-Path $CertificateDirectory 'broker.key'
  $requestPath = Join-Path $CertificateDirectory 'broker.csr'
  $certificatePath = Join-Path $CertificateDirectory 'broker.crt'
  @"
[req]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
req_extensions = req_ext

[dn]
CN = $NodeId

[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = host.docker.internal
IP.1 = $WlanAddress
"@ | Set-Content -Path $configurationPath -Encoding ascii

  Invoke-CheckedCommand -Command $OpenSsl -Arguments @('req', '-new', '-newkey', 'rsa:2048', '-nodes', '-keyout', $keyPath, '-out', $requestPath, '-config', $configurationPath) -FailureMessage "Could not create TLS request for $NodeId"
  Invoke-CheckedCommand -Command $OpenSsl -Arguments @('x509', '-req', '-in', $requestPath, '-CA', $CertificateAuthorityPath, '-CAkey', $CertificateAuthorityKeyPath, '-CAcreateserial', '-out', $certificatePath, '-days', '365', '-sha256', '-extensions', 'req_ext', '-extfile', $configurationPath) -FailureMessage "Could not sign TLS certificate for $NodeId"
  Copy-Item -LiteralPath $CertificateAuthorityPath -Destination (Join-Path $CertificateDirectory 'ca.crt') -Force
  Remove-Item -LiteralPath $requestPath -Force

  return @{
    CaPath = Join-Path $CertificateDirectory 'ca.crt'
    CertificatePath = $certificatePath
    KeyPath = $keyPath
  }
}

$repositoryRoot = Get-RepositoryRoot
$environmentProfile = Get-PlatformEnvironmentProfile
$sdkRoot = Get-AndroidSdkRoot -AndroidSdkRoot $AndroidSdkRoot
$adb = Get-AndroidTool -AndroidSdkRoot $sdkRoot -RelativePath 'platform-tools/adb.exe'
$generatedDirectory = Join-Path $repositoryRoot $environmentProfile.Security.GeneratedDirectory
$bootstrapPath = Join-Path $generatedDirectory 'platform-message-security-bootstrap.json'
$tlsDirectory = Join-Path $generatedDirectory 'platform-tls'
$caPath = Join-Path $tlsDirectory 'ca.crt'
$caKeyPath = Join-Path $tlsDirectory 'ca.key'

if (-not (Test-Path $bootstrapPath) -or -not (Test-Path $caPath) -or -not (Test-Path $caKeyPath)) {
  & (Join-Path $repositoryRoot 'docker/prepare-security.ps1')
  if ($LASTEXITCODE -ne 0) {
    throw 'Could not prepare shared Docker message-security material.'
  }
}

$openSsl = Resolve-OpenSsl -RequestedPath $OpenSslPath
$addresses = @{}
foreach ($node in $environmentProfile.Android.Nodes) {
  Wait-ConfiguredDeviceBoot -AdbPath $adb -Node $node -TimeoutSeconds $environmentProfile.Android.BootTimeoutSeconds
  $addresses[$node.NodeId] = Get-ConfiguredWlan0Address -AdbPath $adb -Node $node
}
if ((@($addresses.Values | Select-Object -Unique)).Count -ne $environmentProfile.Android.Nodes.Count) {
  throw 'Configured AVDs reported duplicate wlan0 addresses; TLS preparation stopped.'
}

$certificateByNodeId = @{}
foreach ($node in $environmentProfile.Android.Nodes | Where-Object { $_.RequiresTermux }) {
  $certificateDirectory = Join-Path $tlsDirectory "android-brokers/$($node.NodeId)"
  $certificateByNodeId[$node.NodeId] = New-AndroidBrokerCertificate -OpenSsl $openSsl -CertificateDirectory $certificateDirectory -CertificateAuthorityPath $caPath -CertificateAuthorityKeyPath $caKeyPath -NodeId $node.NodeId -WlanAddress $addresses[$node.NodeId]
}

$caHash = (& $openSsl 'x509' '-inform' 'PEM' '-subject_hash_old' '-in' $caPath '-noout').Trim()
if ($LASTEXITCODE -ne 0 -or -not $caHash) {
  throw 'Could not compute the Android CA file name.'
}
$androidCaPath = Join-Path $tlsDirectory "$caHash.0"
Copy-Item -LiteralPath $caPath -Destination $androidCaPath -Force
$bootstrapContent = [System.IO.File]::ReadAllText($bootstrapPath)

foreach ($node in $environmentProfile.Android.Nodes) {
  Test-AppInstalled -AdbPath $adb -Node $node -PackageId $environmentProfile.Android.PackageId
  Assert-AdbRoot -AdbPath $adb -Node $node
  Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'mkdir', '-p', '/data/misc/user/0/cacerts-added') -FailureMessage "Could not create the Android user CA store on $($node.Serial)"
  Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'push', $androidCaPath, "/data/misc/user/0/cacerts-added/$caHash.0") -FailureMessage "Could not install the Android test CA on $($node.Serial)"
  Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'chown', 'system:system', "/data/misc/user/0/cacerts-added/$caHash.0") -FailureMessage "Could not apply Android CA ownership on $($node.Serial)"
  Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'chmod', '644', "/data/misc/user/0/cacerts-added/$caHash.0") -FailureMessage "Could not apply Android CA permissions on $($node.Serial)"
  try {
    & $adb '-s' $node.Serial 'shell' 'restorecon' "/data/misc/user/0/cacerts-added/$caHash.0" 2>$null
  } catch [System.Management.Automation.RemoteException] {
    if ($LASTEXITCODE -ne 0) {
      throw "Could not restore the Android CA security context on $($node.Serial)."
    }
  }

  if ($node.RequiresTermux) {
    $termuxPackage = & $adb '-s' $node.Serial 'shell' 'pm' 'list' 'packages' $environmentProfile.Android.TermuxPackageId
    if ($LASTEXITCODE -ne 0 -or "package:$($environmentProfile.Android.TermuxPackageId)" -notin $termuxPackage) {
      throw "Termux is not installed on $($node.Serial). Run Prepare-PlatformAvds.ps1 first."
    }

    $material = $certificateByNodeId[$node.NodeId]
    $termuxTlsDirectory = $environmentProfile.Security.TermuxTlsDirectory
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'mkdir', '-p', $termuxTlsDirectory) -FailureMessage "Could not create the Termux TLS directory on $($node.Serial)"
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'push', $material.CaPath, "$termuxTlsDirectory/ca.crt") -FailureMessage "Could not deploy the Termux CA on $($node.Serial)"
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'push', $material.CertificatePath, "$termuxTlsDirectory/broker.crt") -FailureMessage "Could not deploy the Termux certificate on $($node.Serial)"
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'push', $material.KeyPath, "$termuxTlsDirectory/broker.key") -FailureMessage "Could not deploy the Termux private key on $($node.Serial)"
    $termuxPackageDetails = (& $adb '-s' $node.Serial 'shell' 'cmd' 'package' 'list' 'packages' '-U' $environmentProfile.Android.TermuxPackageId) -join "`n"
    $termuxUidMatch = [regex]::Match($termuxPackageDetails, 'uid:(\d+)')
    if ($LASTEXITCODE -ne 0 -or -not $termuxUidMatch.Success) {
      throw "Could not determine the Termux package UID on $($node.Serial)."
    }
    $termuxOwner = "$($termuxUidMatch.Groups[1].Value):$($termuxUidMatch.Groups[1].Value)"
    $termuxMosquittoDirectory = $termuxTlsDirectory -replace '/tls$', ''
    $termuxPlatformDirectory = $termuxMosquittoDirectory -replace '/mosquitto$', ''
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'chown', $termuxOwner, $termuxPlatformDirectory, $termuxMosquittoDirectory, $termuxTlsDirectory, "$termuxTlsDirectory/ca.crt", "$termuxTlsDirectory/broker.crt", "$termuxTlsDirectory/broker.key") -FailureMessage "Could not set Termux TLS ownership on $($node.Serial)"
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'chmod', '755', $termuxPlatformDirectory, $termuxMosquittoDirectory) -FailureMessage "Could not apply Termux middleware directory permissions on $($node.Serial)"
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'chmod', '755', $termuxTlsDirectory) -FailureMessage "Could not apply Termux TLS directory permissions on $($node.Serial)"
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'chmod', '644', "$termuxTlsDirectory/ca.crt", "$termuxTlsDirectory/broker.crt") -FailureMessage "Could not apply Termux certificate permissions on $($node.Serial)"
    Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'chmod', '600', "$termuxTlsDirectory/broker.key") -FailureMessage "Could not apply Termux key permissions on $($node.Serial)"
  }

  Write-AppPrivateFile -AdbPath $adb -Node $node -PackageId $environmentProfile.Android.PackageId -RelativePath 'files/platform-message-security-bootstrap.json' -Content $bootstrapContent
  Invoke-CheckedCommand -Command $adb -Arguments @('-s', $node.Serial, 'shell', 'monkey', '-p', $environmentProfile.Android.PackageId, '1') -FailureMessage "Could not launch the platform application on $($node.Serial)"
}

Write-Host 'Android CA, Termux TLS files, and the shared message-key bootstrap were prepared from one generated CA/key set.'
Write-Host 'Run the delivered provision-termux.sh once on Platform_Primary and Platform_Temporary, then approve Termux:RunCommand from the platform application.'
