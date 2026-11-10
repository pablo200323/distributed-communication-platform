param(
  [string]$OutputDir = ".generated",
  [string]$OpenSslPath = ""
)

$ErrorActionPreference = "Stop"

function Resolve-OpenSsl {
  if (-not [string]::IsNullOrWhiteSpace($OpenSslPath)) {
    if (Test-Path $OpenSslPath) {
      return [System.IO.Path]::GetFullPath($OpenSslPath)
    }

    throw "OpenSslPath does not exist: $OpenSslPath"
  }

  $command = Get-Command "openssl" -ErrorAction SilentlyContinue
  if ($command) {
    return $command.Source
  }

  $gitOpenSslPath = "C:\Program Files\Git\usr\bin\openssl.exe"
  if (Test-Path $gitOpenSslPath) {
    return $gitOpenSslPath
  }

  throw "openssl was not found in PATH or in the Git for Windows default location."
}

function Invoke-OpenSsl {
  param(
    [string]$Command,
    [string[]]$Arguments
  )

  & $Command @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "openssl failed: $($Arguments -join ' ')"
  }
}

$openSsl = Resolve-OpenSsl
$resolvedOutputDir = [System.IO.Path]::GetFullPath($OutputDir)
$tlsRoot = Join-Path $resolvedOutputDir "platform-tls"
$brokerRoot = Join-Path $tlsRoot "docker-brokers"
$caKeyPath = Join-Path $tlsRoot "ca.key"
$caCertPath = Join-Path $tlsRoot "ca.crt"
$groupKeyId = "platform-group-v1"
$dockerSecurityPath = Join-Path $resolvedOutputDir "platform-security.env"
$bootstrapPath = Join-Path $resolvedOutputDir "platform-message-security-bootstrap.json"

New-Item -ItemType Directory -Force -Path $brokerRoot | Out-Null

if (!(Test-Path $caKeyPath) -or !(Test-Path $caCertPath)) {
  Invoke-OpenSsl $openSsl @(
    "req", "-x509", "-newkey", "rsa:2048", "-days", "365", "-nodes",
    "-keyout", $caKeyPath, "-out", $caCertPath,
    "-subj", "/CN=Distributed Communication Platform Test CA"
  )
}

foreach ($brokerId in @("broker-1", "broker-2", "broker-3")) {
  $brokerDirectory = Join-Path $brokerRoot $brokerId
  $configPath = Join-Path $brokerDirectory "openssl.cnf"
  $keyPath = Join-Path $brokerDirectory "broker.key"
  $csrPath = Join-Path $brokerDirectory "broker.csr"
  $certificatePath = Join-Path $brokerDirectory "broker.crt"

  New-Item -ItemType Directory -Force -Path $brokerDirectory | Out-Null
  @"
[req]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
req_extensions = req_ext

[dn]
CN = $brokerId

[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1 = $brokerId
DNS.2 = localhost
DNS.3 = host.docker.internal
IP.1 = 10.0.2.2
"@ | Set-Content -Path $configPath -Encoding ascii

  Invoke-OpenSsl $openSsl @(
    "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", $keyPath,
    "-out", $csrPath, "-config", $configPath
  )
  Invoke-OpenSsl $openSsl @(
    "x509", "-req", "-in", $csrPath, "-CA", $caCertPath, "-CAkey", $caKeyPath,
    "-CAcreateserial", "-out", $certificatePath, "-days", "365", "-sha256",
    "-extensions", "req_ext", "-extfile", $configPath
  )
  Copy-Item -LiteralPath $caCertPath -Destination (Join-Path $brokerDirectory "ca.crt") -Force
  Remove-Item -LiteralPath $csrPath -Force
}

$securityEnvironmentExists = Test-Path $dockerSecurityPath
$bootstrapExists = Test-Path $bootstrapPath
if ($securityEnvironmentExists -xor $bootstrapExists) {
  throw 'The existing message-security material is incomplete. Restore both files or remove both files before generating a new shared key.'
}

if (-not $securityEnvironmentExists) {
  $keyBytes = New-Object byte[] 32
  [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($keyBytes)
  $base64Key = [Convert]::ToBase64String($keyBytes)

  @"
MESSAGE_SECURITY_KEY_ID=$groupKeyId
MESSAGE_SECURITY_KEY_BASE64=$base64Key
"@ | Set-Content -Path $dockerSecurityPath -Encoding utf8

  @{
    keyId = $groupKeyId
    base64Key = $base64Key
  } | ConvertTo-Json | Set-Content -Path $bootstrapPath -Encoding utf8
}

Write-Host "Generated Docker TLS material in: $tlsRoot"
Write-Host "Generated Docker security environment in: $dockerSecurityPath"
Write-Host "Generated Android bootstrap in: $bootstrapPath"
Write-Host "Do not commit files from $resolvedOutputDir."
