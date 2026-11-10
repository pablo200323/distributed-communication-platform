@{
  Android = @{
    PackageId = 'com.termuxbrokerdemo'
    TermuxPackageId = 'com.termux'
    SystemImage = 'system-images;android-33;google_apis;x86_64'
    DeviceProfile = 'pixel_6'
    BootTimeoutSeconds = 300
    Nodes = @(
      @{
        Name = 'Platform_Primary'
        ConsolePort = 5600
        Serial = 'emulator-5600'
        NodeId = 'node-1'
        InitializationProfile = 'PRIMARY_BROKER'
        AssignedPrimaryBrokerId = $null
        BridgeCapable = $true
        RequiresTermux = $true
      },
      @{
        Name = 'Platform_Temporary'
        ConsolePort = 5602
        Serial = 'emulator-5602'
        NodeId = 'node-2'
        InitializationProfile = 'BASE_TEMPORARY'
        AssignedPrimaryBrokerId = 'node-1'
        BridgeCapable = $true
        RequiresTermux = $true
      },
      @{
        Name = 'Platform_Base'
        ConsolePort = 5604
        Serial = 'emulator-5604'
        NodeId = 'node-3'
        InitializationProfile = 'BASE'
        AssignedPrimaryBrokerId = 'broker-2'
        BridgeCapable = $false
        RequiresTermux = $false
      }
    )
  }
  Docker = @{
    ComposePath = 'docker/compose.yaml'
    Brokers = @(
      @{ BrokerId = 'broker-1'; BrokerType = 'PRIMARY'; Host = '10.0.2.2'; Port = 19101; BridgePort = 19181 },
      @{ BrokerId = 'broker-2'; BrokerType = 'PRIMARY'; Host = '10.0.2.2'; Port = 19102; BridgePort = 19182 },
      @{ BrokerId = 'broker-3'; BrokerType = 'TEMPORARY'; Host = '10.0.2.2'; Port = 19103; BridgePort = 19183 }
    )
    Observers = @(
      @{ NodeId = 'observer-1'; Host = '10.0.2.2'; Port = 9001 },
      @{ NodeId = 'observer-2'; Host = '10.0.2.2'; Port = 9001 }
    )
  }
  Security = @{
    GeneratedDirectory = '.generated'
    TermuxTlsDirectory = '/data/data/com.termux/files/home/.tfg-middleware/mosquitto/tls'
    TermuxProvisioningDirectory = '/sdcard/Download/distributed-communication-platform'
  }
}
