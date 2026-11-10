import {
  BROKER_PROCESS_STATUS,
  BRIDGE_RULE_DIRECTION,
  ENVIRONMENT_STATUS,
} from './BrokerContracts.js';
import {
  checkTermuxAvailability,
  runTermuxCommand,
} from '../termux/TermuxCommandRunner.js';

export class TermuxMosquittoManager {
  /**
   * @param {string} [brokerId]
   * @param {string} [protocol]
   * @param {string} [host]
   * @param {string | null} [advertisedHost]
   * @param {number} [port]
   * @param {string | null} [homeDir]
   * @param {string | null} [baseDir]
   * @param {string | null} [configPath]
   * @param {string | null} [pidFilePath]
   * @param {string | null} [logFilePath]
   * @param {string | null} [persistenceDir]
   * @param {boolean} [allowAnonymous]
   * @param {boolean} [persistence]
   * @param {number} [commandTimeoutMs]
   * @param {boolean} [tlsEnabled]
   * @param {string | null} [tlsCaPath]
   * @param {string | null} [tlsCertPath]
   * @param {string | null} [tlsKeyPath]
   * @param {number} [mqttsPort]
   */
  constructor(
    brokerId = 'local-termux-broker',
    protocol = 'ws',
    host = '0.0.0.0',
    advertisedHost = null,
    port = 9001,
    homeDir = null,
    baseDir = null,
    configPath = null,
    pidFilePath = null,
    logFilePath = null,
    persistenceDir = null,
    allowAnonymous = true,
    persistence = false,
    commandTimeoutMs = 5000,
    tlsEnabled = false,
    tlsCaPath = null,
    tlsCertPath = null,
    tlsKeyPath = null,
    mqttsPort = 8883,
  ) {
    this.brokerId = brokerId;
    this.protocol = protocol;
    this.host = host;
    this.advertisedHost = advertisedHost;
    this.port = port;

    this.homeDir = homeDir ?? '/data/data/com.termux/files/home';
    this.baseDir =
      baseDir ?? this.joinPath(this.homeDir, '.tfg-middleware/mosquitto');
    this.configPath =
      configPath ?? this.joinPath(this.baseDir, 'mosquitto.conf');
    this.pidFilePath =
      pidFilePath ?? this.joinPath(this.baseDir, 'mosquitto.pid');
    this.logFilePath =
      logFilePath ?? this.joinPath(this.baseDir, 'mosquitto.log');
    this.persistenceDir =
      persistenceDir ?? this.joinPath(this.baseDir, 'data');

    this.allowAnonymous = allowAnonymous;
    this.persistence = persistence;
    this.commandTimeoutMs = commandTimeoutMs;
    this.tlsEnabled = tlsEnabled;
    this.tlsCaPath = tlsCaPath ?? this.joinPath(this.baseDir, 'tls/ca.crt');
    this.tlsCertPath =
      tlsCertPath ?? this.joinPath(this.baseDir, 'tls/broker.crt');
    this.tlsKeyPath =
      tlsKeyPath ?? this.joinPath(this.baseDir, 'tls/broker.key');
    this.mqttsPort = mqttsPort;
    this.bridgeConnections = {};
    this.statusObservers = new Set();
    this.lastBrokerStatus = null;
    this.statusQueryPromise = null;
    this.statusMonitorId = null;
    this.statusMonitorIntervalMs = null;
  }

  getBridgeConnections() {
    return this.bridgeConnections;
  }

  addBridgeConnection(remoteBrokerId, brokerUrl) {
    if (
      typeof remoteBrokerId !== 'string' ||
      remoteBrokerId.trim() === '' ||
      typeof brokerUrl !== 'string' ||
      brokerUrl.trim() === ''
    ) {
      return { ok: false, error: 'remoteBrokerId and brokerUrl are required.' };
    }

    const endpointValidationResult = this.validateBridgeEndpoint(brokerUrl);
    if (!endpointValidationResult.ok) return endpointValidationResult;

    if (this.bridgeConnections[remoteBrokerId]) {
      return {
        ok: false,
        error: `Bridge connection '${remoteBrokerId}' already exists.`,
      };
    }

    this.bridgeConnections[remoteBrokerId] = { brokerUrl, bridgeRules: [] };
    return { ok: true };
  }

  removeBridgeConnection(remoteBrokerId) {
    const removed = Boolean(this.bridgeConnections[remoteBrokerId]);
    delete this.bridgeConnections[remoteBrokerId];
    return { ok: removed };
  }

  addBridgeRule(remoteBrokerId, topicFilter, direction, qos = 1) {
    const validationResult = this.validateBridgeRuleInput(
      this.bridgeConnections[remoteBrokerId]?.brokerUrl,
      topicFilter,
      direction,
      qos,
    );

    if (!validationResult.ok) {
      return validationResult;
    }

    const connection = this.bridgeConnections[remoteBrokerId];
    if (!connection) {
      return {
        ok: false,
        error: `Bridge connection '${remoteBrokerId}' is not configured.`,
      };
    }

    if (
      connection.bridgeRules.some(rule => rule.topicFilter === topicFilter)
    ) {
      return {
        ok: false,
        error: `Bridge topic '${topicFilter}' is already configured.`,
      };
    }

    connection.bridgeRules.push({ topicFilter, direction, qos });
    return { ok: true };
  }

  removeBridgeRule(remoteBrokerId, topicFilter) {
    const connection = this.bridgeConnections[remoteBrokerId];
    if (!connection) {
      return { ok: false };
    }

    const previousRuleCount = connection.bridgeRules.length;
    connection.bridgeRules = connection.bridgeRules.filter(
      rule => rule.topicFilter !== topicFilter,
    );

    return { ok: connection.bridgeRules.length !== previousRuleCount };
  }

  applyBridgeConfiguration(remoteBrokerId, brokerUrl, bridgeRules) {
    if (!Array.isArray(bridgeRules)) {
      return { ok: false, error: 'bridgeRules must be an array.' };
    }

    const existingConnection = this.bridgeConnections[remoteBrokerId];
    if (existingConnection) {
      const sameConnection =
        existingConnection.brokerUrl === brokerUrl &&
        JSON.stringify(existingConnection.bridgeRules) ===
          JSON.stringify(bridgeRules);

      return sameConnection
        ? { ok: true, alreadyConfigured: true }
        : {
            ok: false,
            error: `Bridge connection '${remoteBrokerId}' already exists.`,
          };
    }

    for (const rule of bridgeRules) {
      const validationResult = this.validateBridgeRuleInput(
        brokerUrl,
        rule?.topicFilter,
        rule?.direction,
        rule?.qos,
      );
      if (!validationResult.ok) {
        return validationResult;
      }
    }

    const connectionResult = this.addBridgeConnection(
      remoteBrokerId,
      brokerUrl,
    );
    if (!connectionResult.ok) {
      return connectionResult;
    }

    for (const rule of bridgeRules) {
      const ruleResult = this.addBridgeRule(
        remoteBrokerId,
        rule.topicFilter,
        rule.direction,
        rule.qos,
      );
      if (!ruleResult.ok) {
        this.removeBridgeConnection(remoteBrokerId);
        return ruleResult;
      }
    }

    return { ok: true };
  }

  validateBridgeRuleInput(brokerUrl, topicFilter, direction, qos) {
    const endpointValidationResult = this.validateBridgeEndpoint(brokerUrl);
    if (!endpointValidationResult.ok) return endpointValidationResult;

    if (typeof topicFilter !== 'string' || topicFilter.trim() === '') {
      return { ok: false, error: 'topicFilter must be a non-empty string.' };
    }

    if (!Object.values(BRIDGE_RULE_DIRECTION).includes(direction)) {
      return { ok: false, error: 'direction must be in or out.' };
    }

    if (!Number.isInteger(qos) || qos < 0 || qos > 2) {
      return { ok: false, error: 'qos must be an integer between 0 and 2.' };
    }

    return { ok: true };
  }

  observeBrokerStatus(statusObserver) {
    if (typeof statusObserver !== 'function') {
      throw new TypeError('statusObserver must be a function');
    }

    this.statusObservers.add(statusObserver);

    if (this.lastBrokerStatus) {
      try {
        statusObserver(this.lastBrokerStatus);
      } catch {
        // A view observer must not change broker lifecycle behavior.
      }
    }

    return () => this.statusObservers.delete(statusObserver);
  }

  async startStatusMonitoring(statusMonitorIntervalMs) {
    if (
      !Number.isInteger(statusMonitorIntervalMs) ||
      statusMonitorIntervalMs <= 0
    ) {
      return {
        ok: false,
        error: {
          code: 'INVALID_STATUS_MONITOR_INTERVAL',
          message: 'statusMonitorIntervalMs must be a positive integer.',
        },
      };
    }

    if (this.statusMonitorId !== null) {
      return {
        ok: true,
        alreadyMonitoring: true,
        statusMonitorIntervalMs: this.statusMonitorIntervalMs,
        status: await this.getBrokerStatus(),
      };
    }

    this.statusMonitorIntervalMs = statusMonitorIntervalMs;
    this.statusMonitorId = setInterval(() => {
      this.getBrokerStatus().catch(() => {
        // getBrokerStatus() normally contains operational failures in its result.
      });
    }, statusMonitorIntervalMs);

    return {
      ok: true,
      alreadyMonitoring: false,
      statusMonitorIntervalMs,
      status: await this.getBrokerStatus(),
    };
  }

  stopStatusMonitoring() {
    if (this.statusMonitorId === null) {
      return { ok: true, wasMonitoring: false };
    }

    clearInterval(this.statusMonitorId);
    this.statusMonitorId = null;
    this.statusMonitorIntervalMs = null;

    return { ok: true, wasMonitoring: true };
  }

  async checkEnvironment() {
    try {
      const termuxAvailability = await checkTermuxAvailability();

      if (termuxAvailability?.available !== true) {
        return {
          ok: true,
          environmentStatus: ENVIRONMENT_STATUS.UNAVAILABLE,
          brokerProcessStatus: BROKER_PROCESS_STATUS.UNKNOWN,
          error: {
            code: termuxAvailability?.errorCode ?? 'TERMUX_UNAVAILABLE',
            message:
              termuxAvailability?.errorMessage ??
              'Termux command execution is unavailable.',
            details: termuxAvailability ?? null,
          },
          termuxAvailability,
        };
      }

      const mosquittoAvailable = await this.checkMosquittoAvailability();

      if (!mosquittoAvailable) {
        return {
          ok: true,
          environmentStatus: ENVIRONMENT_STATUS.UNAVAILABLE,
          brokerProcessStatus: BROKER_PROCESS_STATUS.UNKNOWN,
          error: {
            code: 'MOSQUITTO_UNAVAILABLE',
            message: 'Mosquitto command is not available.',
          },
          termuxAvailability,
        };
      }

      return {
        ok: true,
        environmentStatus: ENVIRONMENT_STATUS.AVAILABLE,
        brokerProcessStatus: BROKER_PROCESS_STATUS.UNKNOWN,
        termuxAvailability,
      };
    } catch (error) {
      return { ok: false, error };
    }
  }

  async startBroker() {
    try {
      const validationResult = this.validateConfiguration();

      if (!validationResult.ok) {
        return this.publishBrokerStatus(validationResult);
      }

      const environmentResult = await this.checkEnvironment();

      if (!environmentResult.ok) {
        return this.publishBrokerStatus(
          this.createUnexpectedBrokerStatus(environmentResult.error),
        );
      }

      if (
        environmentResult.environmentStatus !== ENVIRONMENT_STATUS.AVAILABLE
      ) {
        return this.publishBrokerStatus({
          ok: false,
          error: environmentResult.error,
          environmentStatus: environmentResult.environmentStatus,
          brokerProcessStatus: environmentResult.brokerProcessStatus,
          usable: false,
          endpoint: null,
        });
      }

      const currentStatusResult = await this.getBrokerStatus();

      if (!currentStatusResult.ok) {
        return currentStatusResult;
      }

      if (
        currentStatusResult.brokerProcessStatus ===
        BROKER_PROCESS_STATUS.RUNNING
      ) {
        return {
          ok: currentStatusResult.usable,
          error: currentStatusResult.usable ? null : currentStatusResult.error,
          environmentStatus: currentStatusResult.environmentStatus,
          brokerProcessStatus: currentStatusResult.brokerProcessStatus,
        };
      }

      await this.ensureBrokerDirectory();
      await this.writeMosquittoConfig();

      const command = [
        `nohup mosquitto -c ${this.pathForShell(this.configPath)} ` +
          `> ${this.pathForShell(this.logFilePath)} 2>&1 &`,
        `echo $! > ${this.pathForShell(this.pidFilePath)}`,
      ].join('\n');

      await this.runCommand(command);

      const newStatusResult = await this.getBrokerStatus();

      if (!newStatusResult.ok) {
        return newStatusResult;
      }

      if (!newStatusResult.usable) {
        return this.publishBrokerStatus({
          ok: false,
          error: {
            code: 'BROKER_START_FAILED',
            message: 'Mosquitto did not become usable after the start command.',
            statusError: newStatusResult.error ?? null,
            statusResult: newStatusResult,
            logFilePath: this.logFilePath,
          },
          environmentStatus: newStatusResult.environmentStatus,
          brokerProcessStatus: newStatusResult.brokerProcessStatus,
          usable: false,
          endpoint: null,
        });
      }

      return {
        ok: true,
        environmentStatus: newStatusResult.environmentStatus,
        brokerProcessStatus: newStatusResult.brokerProcessStatus,
      };
    } catch (error) {
      return this.publishBrokerStatus(
        this.createUnexpectedBrokerStatus(error),
      );
    }
  }

  async stopBroker(force = false) {
    try {
      const environmentResult = await this.checkEnvironment();

      if (!environmentResult.ok) {
        return this.publishBrokerStatus(
          this.createUnexpectedBrokerStatus(environmentResult.error),
        );
      }

      if (
        environmentResult.environmentStatus !== ENVIRONMENT_STATUS.AVAILABLE
      ) {
        return this.publishBrokerStatus({
          ok: false,
          error: environmentResult.error,
          environmentStatus: environmentResult.environmentStatus,
          brokerProcessStatus: environmentResult.brokerProcessStatus,
          usable: false,
          endpoint: null,
        });
      }

      const pid = await this.findMosquittoPid();

      if (!pid) {
        const statusResult = await this.getBrokerStatus();

        if (!statusResult.ok) {
          return statusResult;
        }

        return {
          ok: true,
          environmentStatus: statusResult.environmentStatus,
          brokerProcessStatus: statusResult.brokerProcessStatus,
        };
      }

      const signal = force ? '-9' : '-15';

      await this.runCommand(`kill ${signal} ${pid}`);
      await this.runCommand(`rm -f ${this.pathForShell(this.pidFilePath)}`);

      const statusResult = await this.getBrokerStatus();

      if (!statusResult.ok) {
        return statusResult;
      }

      if (
        statusResult.brokerProcessStatus !== BROKER_PROCESS_STATUS.STOPPED
      ) {
        return this.publishBrokerStatus({
          ok: false,
          error: {
            code: 'BROKER_STOP_FAILED',
            message: 'Mosquitto process is still running after the stop command.',
          },
          environmentStatus: statusResult.environmentStatus,
          brokerProcessStatus: statusResult.brokerProcessStatus,
          usable: false,
          endpoint: null,
        });
      }

      return {
        ok: true,
        environmentStatus: statusResult.environmentStatus,
        brokerProcessStatus: statusResult.brokerProcessStatus,
      };
    } catch (error) {
      return this.publishBrokerStatus(
        this.createUnexpectedBrokerStatus(error),
      );
    }
  }

  async getBrokerStatus() {
    if (this.statusQueryPromise) {
      return this.statusQueryPromise;
    }

    this.statusQueryPromise = this.collectBrokerStatus();

    try {
      return await this.statusQueryPromise;
    } finally {
      this.statusQueryPromise = null;
    }
  }

  async collectBrokerStatus() {
    try {
      const environmentResult = await this.checkEnvironment();

      if (!environmentResult.ok) {
        return this.publishBrokerStatus(
          this.createUnexpectedBrokerStatus(environmentResult.error),
        );
      }

      if (
        environmentResult.environmentStatus !== ENVIRONMENT_STATUS.AVAILABLE
      ) {
        return this.publishBrokerStatus({
          ok: true,
          environmentStatus: environmentResult.environmentStatus,
          brokerProcessStatus: BROKER_PROCESS_STATUS.UNKNOWN,
          usable: false,
          endpoint: null,
          error: environmentResult.error,
        });
      }

      const running = await this.isMosquittoRunning();

      if (!running) {
        return this.publishBrokerStatus({
          ok: true,
          environmentStatus: ENVIRONMENT_STATUS.AVAILABLE,
          brokerProcessStatus: BROKER_PROCESS_STATUS.STOPPED,
          usable: false,
          endpoint: null,
          error: {
            code: 'BROKER_NOT_RUNNING',
            message: 'Mosquitto process is not running.',
          },
        });
      }

      const endpointResult = this.getBrokerEndpoint();

      if (!endpointResult.ok) {
        return this.publishBrokerStatus({
          ok: true,
          environmentStatus: ENVIRONMENT_STATUS.AVAILABLE,
          brokerProcessStatus: BROKER_PROCESS_STATUS.RUNNING,
          usable: false,
          endpoint: null,
          error: endpointResult.error,
        });
      }

      return this.publishBrokerStatus({
        ok: true,
        environmentStatus: ENVIRONMENT_STATUS.AVAILABLE,
        brokerProcessStatus: BROKER_PROCESS_STATUS.RUNNING,
        usable: true,
        endpoint: endpointResult.endpoint,
      });
    } catch (error) {
      return this.publishBrokerStatus(
        this.createUnexpectedBrokerStatus(error),
      );
    }
  }

  getBrokerEndpoint() {
    try {
      const endpoint = this.resolveEndpoint();

      if (!endpoint) {
        return {
          ok: false,
          endpoint: null,
          error: {
            code: 'ENDPOINT_UNAVAILABLE',
            message: 'Broker endpoint could not be resolved.',
          },
        };
      }

      return { ok: true, endpoint };
    } catch (error) {
      return { ok: false, endpoint: null, error };
    }
  }

  validateConfiguration() {
    const port = Number(this.port);
    const mqttsPort = Number(this.mqttsPort);

    if (!this.protocol || typeof this.protocol !== 'string') {
      return this.createInvalidConfigurationResult(
        'INVALID_PROTOCOL',
        'Broker protocol must be a non-empty string.',
      );
    }

    if (!this.host || typeof this.host !== 'string') {
      return this.createInvalidConfigurationResult(
        'INVALID_HOST',
        'Broker host must be a non-empty string.',
      );
    }

    if (!Number.isInteger(port) || port <= 0 || port > 65535) {
      return this.createInvalidConfigurationResult(
        'INVALID_PORT',
        'Broker port must be an integer between 1 and 65535.',
      );
    }

    if (
      !Number.isInteger(mqttsPort) ||
      mqttsPort <= 0 ||
      mqttsPort > 65535
    ) {
      return this.createInvalidConfigurationResult(
        'INVALID_MQTTS_PORT',
        'MQTTS port must be an integer between 1 and 65535.',
      );
    }

    if (
      !this.homeDir ||
      !this.baseDir ||
      !this.configPath ||
      !this.pidFilePath ||
      !this.logFilePath
    ) {
      return this.createInvalidConfigurationResult(
        'INVALID_PATHS',
        'homeDir, baseDir, configPath, pidFilePath and logFilePath are required.',
      );
    }

    if (
      this.tlsEnabled &&
      (!this.tlsCaPath || !this.tlsCertPath || !this.tlsKeyPath)
    ) {
      return this.createInvalidConfigurationResult(
        'INVALID_TLS_PATHS',
        'tlsCaPath, tlsCertPath and tlsKeyPath are required when TLS is enabled.',
      );
    }

    this.port = port;
    this.mqttsPort = mqttsPort;

    return { ok: true };
  }

  async getTlsReadiness() {
    const configurationResult = this.validateConfiguration();

    if (!configurationResult.ok) {
      return {
        ok: false,
        configured: false,
        prepared: false,
        error: configurationResult.error,
      };
    }

    if (!this.tlsEnabled) {
      return {
        ok: true,
        configured: false,
        prepared: false,
        endpoint: this.resolveEndpoint(),
        mqttsEndpoint: null,
        reason: 'TLS is disabled for this local broker.',
      };
    }

    const [ca, certificate, privateKey] = await Promise.all([
      this.inspectTlsFile(this.tlsCaPath, true),
      this.inspectTlsFile(this.tlsCertPath, true),
      this.inspectTlsFile(this.tlsKeyPath, false),
    ]);
    const certificateExpiresAt = Date.parse(certificate.expiresAt ?? '');
    const certificateCurrent =
      Number.isFinite(certificateExpiresAt) && certificateExpiresAt > Date.now();
    const prepared =
      ca.available &&
      !ca.inspectionError &&
      certificate.available &&
      !certificate.inspectionError &&
      certificateCurrent &&
      privateKey.available;
    const endpointHost = this.advertisedHost ?? '127.0.0.1';

    return {
      ok: prepared,
      configured: true,
      prepared,
      endpoint: this.resolveEndpoint(),
      mqttsEndpoint: {
        protocol: 'mqtts',
        host: endpointHost,
        port: this.mqttsPort,
        brokerUrl: `mqtts://${endpointHost}:${this.mqttsPort}`,
      },
      ca,
      certificate,
      certificateCurrent,
      privateKey,
      bridges: Object.entries(this.bridgeConnections).map(
        ([brokerId, connection]) => ({
          brokerId,
          brokerUrl: connection.brokerUrl,
          tlsProtected:
            this.parseMqttBridgeUrl(connection.brokerUrl)?.protocol === 'mqtts',
        }),
      ),
    };
  }

  async inspectTlsFile(path, includeCertificateMetadata) {
    const result = { available: false, path };

    try {
      await this.runCommand(`test -r ${this.pathForShell(path)}`);
      result.available = true;
    } catch {
      return result;
    }

    if (!includeCertificateMetadata) {
      return result;
    }

    try {
      const inspection = await this.runCommand(
        `openssl x509 -in ${this.pathForShell(path)} -noout -subject -issuer -enddate -fingerprint -sha256`,
      );
      const values = Object.fromEntries(
        inspection.stdout
          .split(/\r?\n/)
          .filter(Boolean)
          .map(line => {
            const separatorIndex = line.indexOf('=');
            return separatorIndex < 0
              ? [line.trim(), '']
              : [line.slice(0, separatorIndex).trim(), line.slice(separatorIndex + 1).trim()];
          }),
      );

      return {
        ...result,
        subject: values.subject ?? null,
        issuer: values.issuer ?? null,
        expiresAt: values.notAfter ?? null,
        sha256Fingerprint: values['sha256 Fingerprint'] ?? null,
      };
    } catch (error) {
      return {
        ...result,
        inspectionError: error?.message ?? String(error),
      };
    }
  }

  createInvalidConfigurationResult(code, message) {
    return {
      ok: false,
      error: { code, message },
      environmentStatus: ENVIRONMENT_STATUS.ERROR,
      brokerProcessStatus: BROKER_PROCESS_STATUS.UNKNOWN,
      usable: false,
      endpoint: null,
    };
  }

  createUnexpectedBrokerStatus(error) {
    return {
      ok: false,
      error,
      environmentStatus: ENVIRONMENT_STATUS.ERROR,
      brokerProcessStatus: BROKER_PROCESS_STATUS.ERROR,
      usable: false,
      endpoint: null,
    };
  }

  publishBrokerStatus(status) {
    const previousStatusKey = this.buildBrokerStatusKey(this.lastBrokerStatus);
    const nextStatusKey = this.buildBrokerStatusKey(status);

    this.lastBrokerStatus = status;

    if (previousStatusKey === nextStatusKey) {
      return status;
    }

    for (const statusObserver of this.statusObservers) {
      try {
        statusObserver(status);
      } catch {
        // A view observer must not change broker lifecycle behavior.
      }
    }

    return status;
  }

  buildBrokerStatusKey(status) {
    if (!status) {
      return null;
    }

    return JSON.stringify({
      ok: status.ok,
      environmentStatus: status.environmentStatus,
      brokerProcessStatus: status.brokerProcessStatus,
      usable: status.usable,
      endpoint: status.endpoint,
      errorCode: status.error?.code ?? null,
    });
  }

  async checkMosquittoAvailability() {
    try {
      await this.runCommand('command -v mosquitto >/dev/null 2>&1');
      await this.runCommand('mosquitto -h >/dev/null 2>&1');
      return true;
    } catch {
      return false;
    }
  }

  async ensureBrokerDirectory() {
    const directories = [this.baseDir];

    if (this.persistence) {
      directories.push(this.persistenceDir);
    }

    if (this.tlsEnabled) {
      directories.push(this.joinPath(this.baseDir, 'tls'));
    }

    const command = directories
      .map((directory) => `mkdir -p ${this.pathForShell(directory)}`)
      .join('\n');

    await this.runCommand(command);
  }

  async writeMosquittoConfig() {
    const content = this.buildMosquittoConfigContent();
    const command = [
      `cat > ${this.pathForShell(this.configPath)} <<'EOF'`,
      content,
      'EOF',
    ].join('\n');

    await this.runCommand(command);
  }

  buildMosquittoConfigContent() {
    const lines = [
      `persistence ${this.persistence ? 'true' : 'false'}`,
    ];

    if (this.persistence) {
      lines.push(`persistence_location ${this.persistenceDir}`);
    }

    lines.push(
      '',
      `listener ${Number(this.port)} ${this.host}`,
      'protocol websockets',
    );

    if (this.tlsEnabled) {
      lines.push(
        `cafile ${this.tlsCaPath}`,
        `certfile ${this.tlsCertPath}`,
        `keyfile ${this.tlsKeyPath}`,
      );
    }

    lines.push(`allow_anonymous ${this.allowAnonymous ? 'true' : 'false'}`);

    if (this.tlsEnabled) {
      lines.push(
        '',
        `listener ${Number(this.mqttsPort)} ${this.host}`,
        'protocol mqtt',
        `cafile ${this.tlsCaPath}`,
        `certfile ${this.tlsCertPath}`,
        `keyfile ${this.tlsKeyPath}`,
        `allow_anonymous ${this.allowAnonymous ? 'true' : 'false'}`,
      );
    }

    if (Object.keys(this.bridgeConnections).length > 0) {
      lines.push(...this.buildBridgeConfigurationLines());
    }

    lines.push('');

    return lines.join('\n');
  }

  buildBridgeConfigurationLines() {
    const lines = [];

    for (const [remoteBrokerId, connection] of Object.entries(
      this.bridgeConnections,
    )) {
      if (!connection?.brokerUrl || !Array.isArray(connection.bridgeRules)) {
        continue;
      }

      const address = this.resolveBridgeAddress(connection.brokerUrl);
      if (!address) {
        continue;
      }

      lines.push(
        '',
        `connection bridge_${remoteBrokerId}`,
        `address ${address}`,
        'try_private false',
        'cleansession true',
        'start_type automatic',
        'bridge_reload_type immediate',
        'notifications false',
      );

      if (this.parseMqttBridgeUrl(connection.brokerUrl)?.protocol === 'mqtts') {
        lines.push(
          `bridge_cafile ${this.tlsCaPath}`,
          'bridge_insecure false',
        );
      }

      for (const rule of connection.bridgeRules) {
        lines.push(`topic ${rule.topicFilter} ${rule.direction} ${rule.qos}`);
      }
    }

    return lines;
  }

  parseMqttBridgeUrl(brokerUrl) {
    if (typeof brokerUrl !== 'string' || brokerUrl.trim() === '') {
      return null;
    }

    const match = /^(mqtt|mqtts):\/\/([^/:]+):(\d+)$/.exec(brokerUrl.trim());
    if (!match) {
      return null;
    }

    const [, protocol, host, portText] = match;
    const port = Number(portText);
    if (!Number.isInteger(port) || port <= 0 || port > 65535) {
      return null;
    }

    return { protocol, host, port: portText };
  }

  validateBridgeEndpoint(brokerUrl) {
    const parsedUrl = this.parseMqttBridgeUrl(brokerUrl);

    if (!parsedUrl) {
      return {
        ok: false,
        error: 'brokerUrl must be a valid mqtt://host:port or mqtts://host:port endpoint.',
      };
    }

    if (parsedUrl.protocol === 'mqtts' && !this.tlsEnabled) {
      return {
        ok: false,
        error: 'MQTTS bridge configuration requires TLS to be enabled.',
      };
    }

    return { ok: true, endpoint: parsedUrl };
  }

  resolveBridgeAddress(brokerUrl) {
    const parsedUrl = this.parseMqttBridgeUrl(brokerUrl);
    return parsedUrl ? `${parsedUrl.host}:${parsedUrl.port}` : null;
  }

  async reloadBrokerConfiguration() {
    const pid = await this.findMosquittoPid();
    if (!pid) {
      return { ok: false, error: 'Mosquitto is not running.' };
    }

    await this.runCommand(`kill -HUP ${pid}`);
    return { ok: true };
  }

  async updateRunningBrokerConfiguration() {
    try {
      await this.ensureBrokerDirectory();
      await this.writeMosquittoConfig();
      const reloadResult = await this.reloadBrokerConfiguration();
      if (!reloadResult.ok) {
        return reloadResult;
      }

      return { ok: true, reloaded: true };
    } catch (error) {
      return { ok: false, error };
    }
  }

  async isMosquittoRunning() {
    const pid = await this.findMosquittoPid();
    return Boolean(pid);
  }

  async findMosquittoPid() {
    const pidFromFile = await this.readPid();

    if (pidFromFile && await this.processExists(pidFromFile)) {
      return pidFromFile;
    }

    try {
      const result = await this.runCommand(
        "pgrep -f '[m]osquitto' | head -n 1",
      );
      const pid = result.stdout.trim().split(/\s+/)[0];

      return /^\d+$/.test(pid) ? pid : null;
    } catch {
      return null;
    }
  }

  async readPid() {
    try {
      const result = await this.runCommand(
        `cat ${this.pathForShell(this.pidFilePath)}`,
      );
      const pid = result.stdout.trim().split(/\s+/)[0];

      return /^\d+$/.test(pid) ? pid : null;
    } catch {
      return null;
    }
  }

  async processExists(pid) {
    try {
      await this.runCommand(`kill -0 ${pid}`);
      return true;
    } catch {
      return false;
    }
  }

  resolveEndpoint() {
    const endpointHost = this.advertisedHost ?? '127.0.0.1';
    const host = this.host ?? '0.0.0.0';
    const port = Number(this.port);
    const protocol = this.tlsEnabled ? 'wss' : this.protocol;

    if (
      !protocol ||
      !endpointHost ||
      !Number.isInteger(port) ||
      port <= 0 ||
      port > 65535
    ) {
      return null;
    }

    return {
      protocol,
      ip: endpointHost,
      port,
      brokerUrl: `${protocol}://${endpointHost}:${port}`,
      host,
    };
  }

  pathForShell(path) {
    return `"${String(path).replace(/(["\\`])/g, '\\$1')}"`;
  }

  joinPath(basePath, childPath) {
    const normalizedBasePath = String(basePath).replace(/\/+$/, '');
    const normalizedChildPath = String(childPath).replace(/^\/+/, '');

    return `${normalizedBasePath}/${normalizedChildPath}`;
  }

  async runCommand(command) {
    return runTermuxCommand(command, this.commandTimeoutMs);
  }
}

export default TermuxMosquittoManager;
