# Distributed Communication Platform

React Native test application and Docker environment for adaptive
broker-mediated communication with message encryption, TLS, local Mosquitto
management through Termux, broker bridging, and observer persistence.

## Repository structure

- `src/`: React Native application, message-security code, middleware, Termux
  bridge, topology harness, and automated middleware tests.
- `android/`: Android project and the native modules used by the application
  for Android Keystore and Termux command integration.
- `docker/`: Docker Compose definition, broker and observer images, Mosquitto
  configuration, and local certificate/key preparation.
- `scripts/`: PowerShell and Termux scripts for creating AVDs, preparing
  devices and security material, starting, checking, and stopping the fixed
  test environment.
- `package.json`, lockfiles, and root configuration files: JavaScript
  dependencies, Metro, TypeScript, linting, and test configuration.
- `ios/`: React Native iOS project template; Android is the supported test
  target for this repository.

## Requirements

- Windows, Node.js 22.11 or later, Docker Desktop, and OpenSSL.
- Android SDK command-line tools, Emulator, Platform Tools, and the installed
  `system-images;android-33;google_apis;x86_64` image.
- A Termux APK obtained by the operator. It is deliberately not downloaded or
  committed by this repository.

## Automated Android and Docker environment

The repository defines one fixed environment:

- `Platform_Primary` (`emulator-5600`): `PRIMARY_BROKER` with a local Termux
  broker.
- `Platform_Temporary` (`emulator-5602`): `BASE_TEMPORARY` with a local
  Termux broker.
- `Platform_Base` (`emulator-5604`): `BASE`, without a local broker.
- Three Docker brokers and two Docker observers from `docker/compose.yaml`.

The profile has no test-only reachability restrictions. Every declared
participant receives the topology required to communicate with every other
declared participant.

### First preparation

Build the debug APK and create the AVDs. Existing AVDs with those exact names
are preserved: the creation script never deletes or recreates an AVD, but it
does reapply the declared emulator profile (including physical keyboard input)
to keep an existing profile aligned with this environment.

```powershell
npm ci
.\scripts\New-PlatformAvds.ps1
.\android\gradlew.bat -p android :app:assembleDebug
```

Start the complete host environment. Metro opens in a visible CMD window on
port 8081, Docker starts its declared services, and the three AVDs boot.

```powershell
.\scripts\Start-PlatformEnvironment.ps1
```

Install the platform APK and the operator-provided Termux APK, then deliver the
three static topology documents. Use `-ResetApplicationData` only for a clean
preparation that intentionally discards prior app messages and Android Keystore
keys.

```powershell
.\scripts\Prepare-PlatformAvds.ps1 `
  -TermuxApkPath C:\path\to\termux.apk `
  -ResetApplicationData
```

Prepare one local CA, Docker certificates, Android trust, the two Termux broker
certificates, and the shared message-encryption bootstrap. The generated files
remain below `.generated/` and are ignored by Git.

```powershell
.\scripts\Prepare-PlatformSecurity.ps1
```

On `Platform_Primary` and `Platform_Temporary`, open Termux once and execute
the fixed script delivered by the preparation command:

```sh
termux-setup-storage
bash ~/storage/downloads/distributed-communication-platform/provision-termux.sh
```

Accept Android's storage permission when `termux-setup-storage` requests it.
Close and reopen Termux after the provisioning script finishes. When the
platform application requests it, approve Android's `Termux:RunCommand`
permission. Those approvals cannot be bypassed by the host scripts. Then open
or restart the platform application on each AVD and initialize its declared
profile normally.

The Docker brokers are reached from an AVD through `10.0.2.2`. The generated
broker certificates include that address as a TLS subject alternative name; do
not replace the generated certificates with ones that omit it.

Check the resulting state without modifying the environment:

```powershell
.\scripts\Test-PlatformEnvironment.ps1
```

### States

`LIFTED` means Docker, Metro, and all three configured AVDs are active.

`READY` additionally requires the application and Termux installation, static
topology delivery, Android trust, TLS files, the shared message key, the two
manual Termux provisioning runs, the Android `Termux:RunCommand` approval, and
the normal in-app initialization of each declared profile.

The host status script reports `PREPARED_FOR_MANUAL_INITIALIZATION` when it has
verified every host-visible prerequisite. It intentionally does not press the
application initialization controls or claim that a manager session is active.
It also cannot inspect the Android Keystore or prove an in-app MQTT session;
validate those with the application security and operation views.

### Normal use and shutdown

After the first preparation, use the start command to bring the same local
environment back online. It reuses the local generated material rather than
creating a new key or CA.

```powershell
.\scripts\Start-PlatformEnvironment.ps1
```

If Metro does not open port `8081`, close only stale project Metro processes
and start one visible instance from the repository root with:

```powershell
npm start -- --reset-cache
```

Stop only the declared AVDs, Docker services, and the Metro process belonging
to this repository. Docker volumes are retained.

```powershell
.\scripts\Stop-PlatformEnvironment.ps1
```

## Install and test

```powershell
npm ci
npm run test:middleware
```

## Docker environment

Generate local, untracked TLS and message-security material:

```powershell
npm run security:prepare
```

When both `.generated/platform-security.env` and
`.generated/platform-message-security-bootstrap.json` already exist, the
generator reuses their shared message key while it refreshes broker
certificates. If exactly one of those files exists, restore both together or
remove both together before generating a new environment; never replace just
one, because Docker and Android would then use different message keys.

Start the three brokers and two observers:

```powershell
docker compose -f docker/compose.yaml up --build
```

The command creates the Docker network and observer volumes automatically.

## Android security inputs

The application keeps its topology and group-key bootstrap outside version
control. Before the first app launch, provision these inputs in the app sandbox:

- `files/harness_topology.json`
- `files/platform-message-security-bootstrap.json`

`npm run security:prepare` creates the second input under `.generated/` with
the same key used by Docker. The application imports that key into Android
Keystore and removes the bootstrap from its private storage after a successful
import.

No AVD snapshot is part of this repository. A local snapshot may be used as an
operator convenience after preparation, but the reproducible path is the
scripts above.
