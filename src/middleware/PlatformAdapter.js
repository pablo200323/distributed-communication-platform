import { v4 as randomUUID } from 'uuid';
import {
  BROKER_PROCESS_STATUS,
  BROKER_ROLE,
  BRIDGE_RULE_DIRECTION,
} from './BrokerContracts.js';
import { ControlService } from './ControlSesion.js';
import { MESSAGE_OPERATIONS, MESSAGE_TYPES } from './Message.js';
import { PendingRequestRegistry } from './PendingRequestRegistry.js';
import {
  MAX_OBSERVER_ROUTE_HOP_COUNT,
  OBSERVER_REACHABILITY_STATUS,
  ReevaluationSnapshot,
  ROUTE_PRIORITY_TIER,
} from './ReevaluationSnapshot.js';
import {
  MIDDLEWARE_MODES,
  PlatformModeManager,
} from './PlatformModeManager.js';
import { SessionNotFoundError } from './SessionManager.js';
import { RUNTIME_PROFILE } from './RuntimeProfiles.js';

export const OPERATION_INFORMATION_TOPIC = 'operation/info';
export const BROKER_STATUS_REQUEST_TOPIC = 'check/observer/status';
export const BROKER_STATUS_CHANGED_TOPIC = 'notify/observer/status';
export const PARTITION_REPLICA_TOPIC = 'operational/partition/replica';
export const RESYNC_BATCH_REQUEST_TOPIC =
  'control/subscriber/resync/batch/request';

export const RESYNC_BATCH_STATUS = Object.freeze({
  IN_PROGRESS: 'EN_CURSO',
  COMPLETED: 'COMPLETADA',
  FAILED: 'FALLIDA',
});

const RESYNC_BATCH_MESSAGE_COUNT = 25;
const RESYNC_BATCH_RESPONSE_TIMEOUT_MS = 5000;
const LOCAL_BROKER_STATUS_MONITOR_INTERVAL_MS = 1000;
const CONTROL_SERVICE_HEALTH_REQUEST_TIMEOUT_MS = 5000;
const LOCAL_BROKER_BATTERY_PERCENTAGE = 100;
const USABLE_ROUTE_TIERS_BEFORE_TEMPORARY_ACTIVATION = Object.freeze([
  ROUTE_PRIORITY_TIER.DIRECT_PRIMARY,
]);
const USABLE_ROUTE_TIERS_AFTER_TEMPORARY_ACTIVATION = Object.freeze([
  ROUTE_PRIORITY_TIER.DIRECT_TEMPORARY,
  ROUTE_PRIORITY_TIER.BRIDGED_PRIMARY,
  ROUTE_PRIORITY_TIER.BRIDGED_TEMPORARY,
]);
const OBSERVER_UNREACHABLE_ROUTE_TIERS = Object.freeze([
  ROUTE_PRIORITY_TIER.DIRECT_PRIMARY,
  ROUTE_PRIORITY_TIER.DIRECT_TEMPORARY,
  ROUTE_PRIORITY_TIER.BRIDGED_PRIMARY,
  ROUTE_PRIORITY_TIER.BRIDGED_TEMPORARY,
  ROUTE_PRIORITY_TIER.UNKNOWN_PRIMARY,
  ROUTE_PRIORITY_TIER.UNKNOWN_TEMPORARY,
]);
const DEFAULT_BRIDGE_RULES = Object.freeze([
  Object.freeze({
    topicFilter: 'operation/#',
    direction: BRIDGE_RULE_DIRECTION.OUTBOUND,
    qos: 1,
  }),
  Object.freeze({
    topicFilter: 'broker/heartbeat',
    direction: BRIDGE_RULE_DIRECTION.OUTBOUND,
    qos: 1,
  }),
  Object.freeze({
    topicFilter: 'observer/status/response',
    direction: BRIDGE_RULE_DIRECTION.INBOUND,
    qos: 1,
  }),
  Object.freeze({
    topicFilter: 'control/node/+/inbox/#',
    direction: BRIDGE_RULE_DIRECTION.INBOUND,
    qos: 1,
  }),
  Object.freeze({
    topicFilter: 'control/subscriber/status/request',
    direction: BRIDGE_RULE_DIRECTION.OUTBOUND,
    qos: 1,
  }),
  Object.freeze({
    topicFilter: 'control/subscriber/status/resync',
    direction: BRIDGE_RULE_DIRECTION.OUTBOUND,
    qos: 1,
  }),
  Object.freeze({
    topicFilter: 'control/subscriber/resync/batch/request',
    direction: BRIDGE_RULE_DIRECTION.OUTBOUND,
    qos: 1,
  }),
]);

function buildNodeInboxTopic(nodeId, sessionId) {
  return `control/node/${nodeId}/inbox/${sessionId}`;
}

function buildManagedEntityJoinResponseTopic(nodeId, sessionId) {
  return `control/node/${nodeId}/inbox/${sessionId}/managed-entity-join`;
}

function buildResyncBatchResponseTopic(nodeId, sessionId) {
  return `control/node/${nodeId}/inbox/${sessionId}/resync-batch`;
}

function assertNonEmptyString(value, fieldName) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new TypeError(`${fieldName} must be a non-empty string`);
  }
}

function assertBroker(brokerId, brokerUrl, brokerType, ip, bridgePort) {
  assertNonEmptyString(brokerId, 'brokerId');

  if (typeof brokerUrl !== 'object' || brokerUrl === null) {
    assertNonEmptyString(brokerUrl, 'brokerUrl');
  }

  if (!Object.values(BROKER_ROLE).includes(brokerType)) {
    throw new TypeError(`Invalid brokerType '${String(brokerType)}'`);
  }

  if (ip !== null) {
    assertNonEmptyString(ip, 'ip');
  }

  if (
    bridgePort !== null &&
    (!Number.isInteger(bridgePort) || bridgePort <= 0 || bridgePort > 65535)
  ) {
    throw new TypeError(
      'bridgePort must be null or an integer between 1 and 65535',
    );
  }
}

function normalizeHopCount(value) {
  return Number.isInteger(value) && value > 0 ? value : null;
}

function buildBridgeBrokerUrl(ip, bridgePort, bridgeUsesTls = false) {
  if (
    typeof ip !== 'string' ||
    ip.trim() === '' ||
    !Number.isInteger(bridgePort) ||
    bridgePort <= 0 ||
    bridgePort > 65535
  ) {
    return null;
  }

  return `${bridgeUsesTls ? 'mqtts' : 'mqtt'}://${ip}:${bridgePort}`;
}

function summarizeLocalBrokerError(error) {
  if (typeof error === 'string') {
    return { code: null, message: error };
  }

  if (!error || typeof error !== 'object') {
    return { code: null, message: error == null ? null : String(error) };
  }

  return {
    code: typeof error.code === 'string' ? error.code : null,
    message: typeof error.message === 'string' ? error.message : null,
    exitCode: Number.isInteger(error.exitCode) ? error.exitCode : null,
    errCode: Number.isInteger(error.errCode) ? error.errCode : null,
  };
}

function localBrokerFailureMessage(fallback, result) {
  const error = summarizeLocalBrokerError(result?.error);

  return error.message ? `${fallback}: ${error.message}` : fallback;
}

function assertStatusReport(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new TypeError('Broker report payload must be an object');
  }

  assertNonEmptyString(payload.brokerId, 'payload.brokerId');

  if (!Object.values(BROKER_ROLE).includes(payload.brokerType)) {
    throw new TypeError(
      `Invalid payload.brokerType '${String(payload.brokerType)}'`,
    );
  }

  if (!Object.values(OBSERVER_REACHABILITY_STATUS).includes(payload.status)) {
    throw new TypeError(`Invalid payload.status '${String(payload.status)}'`);
  }

  if (
    payload.status === OBSERVER_REACHABILITY_STATUS.REACHABLE &&
    normalizeHopCount(payload.hopCount) === null
  ) {
    throw new TypeError(
      'Reachable broker report payloads must define a positive integer hopCount',
    );
  }

  if (
    payload.lastKnownHopCount !== undefined &&
    payload.lastKnownHopCount !== null &&
    normalizeHopCount(payload.lastKnownHopCount) === null
  ) {
    throw new TypeError(
      'payload.lastKnownHopCount must be null or a positive integer',
    );
  }

  assertNonEmptyString(payload.observedAt, 'payload.observedAt');

  if (typeof payload.admissionAccepted !== 'boolean') {
    throw new TypeError('payload.admissionAccepted must be a boolean');
  }

  if (!Number.isInteger(payload.managedEntityCount) || payload.managedEntityCount < 0) {
    throw new TypeError('payload.managedEntityCount must be a non-negative integer');
  }

  if (!Number.isInteger(payload.maximumManagedEntityCount) ||
    payload.maximumManagedEntityCount <= 0) {
    throw new TypeError('payload.maximumManagedEntityCount must be a positive integer');
  }
}

function resolveCandidateMode(candidate) {
  if (candidate.status === OBSERVER_REACHABILITY_STATUS.UNREACHABLE) {
    return MIDDLEWARE_MODES.OFFLINE;
  }

  if (normalizeHopCount(candidate.hopCount) > 1) {
    return MIDDLEWARE_MODES.FALLBACK;
  }

  return candidate.brokerType === BROKER_ROLE.PRIMARY
    ? MIDDLEWARE_MODES.NORMAL
    : MIDDLEWARE_MODES.FALLBACK;
}

function findNextUsableRouteCandidate(
  snapshot,
  routePriorityTier,
  attemptedBrokerIds,
) {
  return (
    snapshot
      .getUsableBrokersInTier(routePriorityTier)
      .find(
        candidate => !attemptedBrokerIds.has(candidate.brokerId),
      ) ?? null
  );
}

function findNextObserverUnreachableRouteCandidate(
  snapshot,
  routePriorityTier,
  attemptedBrokerIds,
) {
  return (
    snapshot
      .getObserverUnreachableBrokersInTier(routePriorityTier)
      .find(candidate => !attemptedBrokerIds.has(candidate.brokerId)) ?? null
  );
}

class PlatformAdapter {
  /**
   * @param {unknown} sessionManager
   * @param {string} nodeId
   * @param {string | null} assignedPrimaryBrokerId
   * @param {number} reevaluationIntervalMs
   * @param {number} reportTimeoutMs
   * @param {boolean} bridgeUsesTls
   */
  constructor(
    sessionManager,
    nodeId,
    assignedPrimaryBrokerId = null,
    reevaluationIntervalMs = 5000,
    reportTimeoutMs = 3000,
    bridgeCapable = false,
    bridgeUsesTls = false,
  ) {
    if (
      !sessionManager ||
      typeof sessionManager.initialize !== 'function' ||
      typeof sessionManager.createSession !== 'function' ||
      typeof sessionManager.connectSession !== 'function' ||
      typeof sessionManager.getSessionState !== 'function' ||
      typeof sessionManager.subscribe !== 'function' ||
      typeof sessionManager.publish !== 'function' ||
      typeof sessionManager.persistOperationalMessage !== 'function' ||
      typeof sessionManager.getPendingMessages !== 'function' ||
      typeof sessionManager.getMessageEntry !== 'function' ||
      typeof sessionManager.markMessageAsSynced !== 'function' ||
      typeof sessionManager.closeSession !== 'function' ||
      typeof sessionManager.setUnexpectedSessionCloseHandler !== 'function'
    ) {
      throw new TypeError(
        'sessionManager must support the platform session contract',
      );
    }

    assertNonEmptyString(nodeId, 'nodeId');

    if (assignedPrimaryBrokerId !== null) {
      assertNonEmptyString(assignedPrimaryBrokerId, 'assignedPrimaryBrokerId');
    }

    if (
      !Number.isInteger(reevaluationIntervalMs) ||
      reevaluationIntervalMs <= 0
    ) {
      throw new TypeError('reevaluationIntervalMs must be a positive integer');
    }

    if (!Number.isInteger(reportTimeoutMs) || reportTimeoutMs <= 0) {
      throw new TypeError('reportTimeoutMs must be a positive integer');
    }

    this.sessionManager = sessionManager;
    this.nodeId = nodeId;
    this.assignedPrimaryBrokerId = assignedPrimaryBrokerId;
    this.reevaluationIntervalMs = reevaluationIntervalMs;
    this.reportTimeoutMs = reportTimeoutMs;
    this.bridgeCapable = bridgeCapable === true;
    this.bridgeUsesTls = bridgeUsesTls === true;
    this.knownBrokers = new Map();
    this.pendingRequestRegistry = new PendingRequestRegistry(reportTimeoutMs);
    this.periodicReevaluationTimer = null;
    this.periodicPendingSynchronizationTimer = null;
    this.reevaluationInFlight = false;
    this.pendingSynchronizationInFlight = false;
    this.currentPendingSynchronizationBatch = null;
    this.lastPendingSynchronizationResult = null;
    this.lastSnapshot = null;
    this.lastReevaluationResult = null;
    this.lastAdaptationAction = null;
    this.lastAdaptationSummary = null;
    this.reevaluationStateHandler = null;
    this.operationalSessionId = null;
    this.localBrokerActive = false;
    this.runtimeProfile = null;
    this.localBrokerManager = null;
    this.localBrokerType = null;
    this.controlService = null;
    this.localBrokerActivationPromise = null;
    this.localBrokerDeactivationPromise = null;
    this.managedBridgeState = null;
    this.initialized = false;
    this.platformModeManager = new PlatformModeManager({
      assignedPrimaryBrokerId,
    });
    this.handleBrokerStatusReport = this.handleBrokerStatusReport.bind(this);
    this.handleBrokerStatusChanged = this.handleBrokerStatusChanged.bind(this);
    this.handleManagedEntityJoinResponse = this.handleManagedEntityJoinResponse.bind(this);
    this.handleBrokerGroupCheckRequest = this.handleBrokerGroupCheckRequest.bind(this);
    this.handlePartitionReplica = this.handlePartitionReplica.bind(this);
    this.handleReceiptConfirmation = this.handleReceiptConfirmation.bind(this);
    this.handleResyncBatchResponse = this.handleResyncBatchResponse.bind(this);
    this.handleLocalBrokerObserverReachabilityChanged =
      this.handleLocalBrokerObserverReachabilityChanged.bind(this);

    this.sessionManager.setLocalNodeId(this.nodeId);
    this.sessionManager.setUnexpectedSessionCloseHandler(
      this.handleUnexpectedSessionClose.bind(this),
    );
  }

  setAssignedPrimaryBrokerId(brokerId) {
    if (brokerId !== null) {
      assertNonEmptyString(brokerId, 'brokerId');
    }

    this.assignedPrimaryBrokerId = brokerId;
    const result = this.platformModeManager.setAssignedPrimaryBrokerId(brokerId);

    return {
      ...result,
    };
  }

  setRuntimeProfile(runtimeProfile) {
    if (this.initialized) {
      throw new Error('Runtime profile cannot be changed after initialization');
    }

    if (!Object.values(RUNTIME_PROFILE).includes(runtimeProfile)) {
      throw new TypeError(
        `Invalid runtimeProfile '${String(runtimeProfile)}'`,
      );
    }

    this.runtimeProfile = runtimeProfile;
    return { ok: true, runtimeProfile };
  }

  getRuntimeProfile() {
    return this.runtimeProfile;
  }

  setLocalBrokerManager(localBrokerManager, localBrokerType) {
    if (this.initialized) {
      throw new Error(
        'Local broker manager cannot be changed after initialization',
      );
    }

    const requiredMethods = [
      'getBrokerStatus',
      'startBroker',
      'stopBroker',
      'getBrokerEndpoint',
      'startStatusMonitoring',
      'stopStatusMonitoring',
      'getBridgeConnections',
      'removeBridgeConnection',
      'applyBridgeConfiguration',
      'updateRunningBrokerConfiguration',
    ];

    if (
      !localBrokerManager ||
      typeof localBrokerManager !== 'object' ||
      requiredMethods.some(
        methodName => typeof localBrokerManager[methodName] !== 'function',
      )
    ) {
      throw new TypeError(
        'localBrokerManager must support the local broker lifecycle contract',
      );
    }

    if (!Object.values(BROKER_ROLE).includes(localBrokerType)) {
      throw new TypeError(
        `Invalid localBrokerType '${String(localBrokerType)}'`,
      );
    }

    this.localBrokerManager = localBrokerManager;
    this.localBrokerType = localBrokerType;

    return {
      ok: true,
      localBrokerType,
    };
  }

  hasLocalBrokerManager() {
    return this.localBrokerManager !== null;
  }

  getLocalBrokerType() {
    return this.localBrokerType;
  }

  getManagedBrokerBridgeConnections() {
    if (!this.hasLocalBrokerManager()) {
      return { ok: false, error: 'No local broker manager is registered.' };
    }

    return {
      ok: true,
      bridgeCapable: this.bridgeCapable,
      bridgeConnections: this.localBrokerManager.getBridgeConnections(),
    };
  }

  applyManagedBridgeActivation(
    activationResult,
    upstreamBrokerId,
    upstreamHopCount,
    upstreamEndpoint,
  ) {
    if (activationResult?.ok === true) {
      this.managedBridgeState = {
        configured: true,
        upstreamBrokerId,
        upstreamHopCount: normalizeHopCount(upstreamHopCount) ?? 1,
        upstreamEndpoint,
        configuredAt: new Date().toISOString(),
      };
    } else {
      this.managedBridgeState = null;
    }

    return { ok: true };
  }

  clearManagedBridgeState() {
    this.managedBridgeState = null;
    return { ok: true };
  }

  async activateManagedBrokerBridge(remoteBrokerId, brokerUrl, upstreamHopCount) {
    if (!this.hasLocalBrokerManager()) {
      return { ok: false, error: 'No local broker manager is registered.' };
    }

    if (!this.bridgeCapable) {
      return { ok: false, error: 'This node is not bridge-capable.' };
    }

    for (const existingRemoteBrokerId of Object.keys(
      this.localBrokerManager.getBridgeConnections(),
    )) {
      this.localBrokerManager.removeBridgeConnection(existingRemoteBrokerId);
    }

    const configurationResult = this.localBrokerManager.applyBridgeConfiguration(
      remoteBrokerId,
      brokerUrl,
      DEFAULT_BRIDGE_RULES,
    );
    if (!configurationResult?.ok) {
      const bridgeCleanupResult = await this.disableManagedBrokerBridge();
      this.applyManagedBridgeActivation(
        configurationResult,
        remoteBrokerId,
        upstreamHopCount,
        brokerUrl,
      );
      return { ...configurationResult, bridgeCleanupResult };
    }

    const brokerStatusResult = await this.localBrokerManager.getBrokerStatus();
    if (!brokerStatusResult?.ok) {
      return brokerStatusResult;
    }

    const brokerWasRunning =
      brokerStatusResult.brokerProcessStatus === BROKER_PROCESS_STATUS.RUNNING;
    const runningBrokerConfigurationResult = brokerWasRunning
      ? await this.localBrokerManager.updateRunningBrokerConfiguration()
      : { ok: true, skipped: true };
    if (!runningBrokerConfigurationResult?.ok) {
      return runningBrokerConfigurationResult;
    }

    const activationResult = await this.activateLocalBroker();
    const managedBridgeStateResult = this.applyManagedBridgeActivation(
      activationResult,
      remoteBrokerId,
      upstreamHopCount,
      brokerUrl,
    );
    let controlServiceResult = { ok: true, skipped: true };

    if (activationResult?.ok === true && this.controlService) {
      const upstreamHopCountResult =
        await this.controlService.setBridgeUpstreamHopCount(
          normalizeHopCount(upstreamHopCount) ?? 1,
        );
      const invalidationResult =
        await this.controlService.invalidateObserverReachability();
      const healthCheckResult =
        await this.controlService.checkObserverReachability();

      controlServiceResult = {
        ok:
          upstreamHopCountResult?.ok === true &&
          invalidationResult?.ok === true &&
          healthCheckResult?.ok === true,
        upstreamHopCountResult,
        invalidationResult,
        healthCheckResult,
      };
    }

    return {
      ok:
        activationResult?.ok === true &&
        managedBridgeStateResult?.ok === true &&
        controlServiceResult?.ok === true,
      configurationResult,
      runningBrokerConfigurationResult,
      activationResult,
      managedBridgeStateResult,
      controlServiceResult,
      error:
        activationResult?.error ??
        managedBridgeStateResult?.error ??
        controlServiceResult?.error ??
        null,
    };
  }

  async disableManagedBrokerBridge() {
    if (!this.hasLocalBrokerManager()) {
      return { ok: false, error: 'No local broker manager is registered.' };
    }

    const bridgeConnections = this.localBrokerManager.getBridgeConnections();
    const activeBridgeIds = Object.keys(bridgeConnections);
    const hadBridgeService =
      activeBridgeIds.length > 0 ||
      this.managedBridgeState?.configured === true;

    for (const remoteBrokerId of activeBridgeIds) {
      this.localBrokerManager.removeBridgeConnection(remoteBrokerId);
    }

    const brokerStatusResult = await this.localBrokerManager.getBrokerStatus();
    const brokerConfigurationResult =
      activeBridgeIds.length > 0 &&
      brokerStatusResult?.brokerProcessStatus === BROKER_PROCESS_STATUS.RUNNING
        ? await this.localBrokerManager.updateRunningBrokerConfiguration()
        : { ok: true, skipped: true };
    let controlServiceResult = { ok: true, skipped: true };

    if (this.controlService) {
      const upstreamHopCountResult =
        await this.controlService.setBridgeUpstreamHopCount(null);
      const invalidationResult =
        await this.controlService.invalidateObserverReachability();
      const healthCheckResult =
        await this.controlService.checkObserverReachability();

      controlServiceResult = {
        ok:
          upstreamHopCountResult?.ok === true &&
          invalidationResult?.ok === true &&
          healthCheckResult?.ok === true,
        upstreamHopCountResult,
        invalidationResult,
        healthCheckResult,
      };
    }
    const managedBridgeStateResult = this.clearManagedBridgeState();

    return {
      ok:
        brokerConfigurationResult?.ok === true &&
        controlServiceResult?.ok === true &&
        managedBridgeStateResult?.ok === true,
      bridgeConfigured: false,
      changed: hadBridgeService,
      brokerConfigurationResult,
      controlServiceResult,
      managedBridgeStateResult,
    };
  }

  resolveManagedBrokerLifecycle(selectedCandidate) {
    if (!this.hasLocalBrokerManager()) {
      return {};
    }

    const selectedBrokerEntry = selectedCandidate
      ? this.knownBrokers.get(selectedCandidate.brokerId)
      : null;
    const bridgeUrl = buildBridgeBrokerUrl(
      selectedBrokerEntry?.ip,
      selectedBrokerEntry?.bridgePort,
      this.bridgeUsesTls,
    );
    const selectedHopCount = normalizeHopCount(selectedCandidate?.hopCount);
    const canOfferBridgeNow =
      this.bridgeCapable &&
      selectedCandidate?.brokerId !== this.nodeId &&
      selectedCandidate?.status === OBSERVER_REACHABILITY_STATUS.REACHABLE &&
      selectedHopCount !== null &&
      selectedHopCount + 1 <= MAX_OBSERVER_ROUTE_HOP_COUNT &&
      Boolean(bridgeUrl);
    const bridgeConnections = this.localBrokerManager.getBridgeConnections();
    const activeBridgeIds = Object.keys(bridgeConnections);
    const existingSelectedBridge = canOfferBridgeNow
      ? bridgeConnections[selectedCandidate.brokerId]
      : null;
    const bridgeIsAlreadyOperational =
      activeBridgeIds.length === 1 &&
      existingSelectedBridge?.brokerUrl === bridgeUrl &&
      this.managedBridgeState?.configured === true &&
      this.managedBridgeState.upstreamBrokerId === selectedCandidate?.brokerId &&
      this.managedBridgeState.upstreamHopCount === selectedHopCount;

    if (canOfferBridgeNow && !bridgeIsAlreadyOperational) {
      return { activateBridge: true };
    }

    if (
      !canOfferBridgeNow &&
      (activeBridgeIds.length > 0 || this.managedBridgeState?.configured === true)
    ) {
      return { removeBridge: true };
    }

    return {};
  }

  addBroker(brokerId, brokerUrl, brokerType, ip = null, bridgePort = null) {
    assertBroker(brokerId, brokerUrl, brokerType, ip, bridgePort);

    const broker = Object.freeze({
      brokerId,
      brokerUrl,
      brokerType,
      ip,
      bridgePort,
    });
    const existingBroker = this.knownBrokers.get(brokerId);
    this.knownBrokers.set(brokerId, broker);

    return {
      ok: true,
      broker,
      alreadyRegistered: Boolean(existingBroker),
      count: this.knownBrokers.size,
    };
  }

  async removeBroker(brokerId) {
    assertNonEmptyString(brokerId, 'brokerId');
    const existingBroker = this.knownBrokers.get(brokerId);

    if (!existingBroker) {
      return { ok: true, removed: false, brokerId };
    }

    const sessionId = brokerId;
    const closeResult = await this.sessionManager.closeSession(sessionId);
    this.knownBrokers.delete(brokerId);

    return {
      ok: closeResult.ok === true,
      removed: true,
      brokerId,
      closeResult,
      count: this.knownBrokers.size,
    };
  }

  loadKnownBrokers(brokersJson) {
    if (this.initialized) {
      throw new Error('Known brokers cannot be replaced after initialization');
    }

    if (!brokersJson || !Array.isArray(brokersJson.brokers)) {
      throw new TypeError('brokersJson.brokers must be an array');
    }

    this.knownBrokers.clear();

    for (const broker of brokersJson.brokers) {
      if (!broker || typeof broker !== 'object' || Array.isArray(broker)) {
        throw new TypeError('Each broker must be an object');
      }

      this.addBroker(
        broker.brokerId,
        broker.brokerUrl,
        broker.brokerType,
        broker.ip ?? null,
        broker.bridgePort ?? null,
      );
    }

    return {
      ok: true,
      count: this.knownBrokers.size,
      brokers: this.getKnownBrokers(),
    };
  }

  getKnownBrokers() {
    return Array.from(this.knownBrokers.values());
  }

  getLastSnapshot() {
    return this.lastSnapshot;
  }

  getReevaluationState() {
    return Object.freeze({
      lastResult: this.lastReevaluationResult,
      lastAdaptationAction: this.lastAdaptationAction,
      modeState: this.platformModeManager.getState(),
      currentOperationalSessionId: this.operationalSessionId,
      reevaluationInFlight: this.reevaluationInFlight,
      periodicReevaluationActive: this.periodicReevaluationTimer !== null,
      pendingSynchronizationInFlight: this.pendingSynchronizationInFlight,
      periodicPendingSynchronizationActive:
        this.periodicPendingSynchronizationTimer !== null,
      currentPendingSynchronizationBatch: this.currentPendingSynchronizationBatch,
      lastPendingSynchronizationResult: this.lastPendingSynchronizationResult,
      bridgeCapable: this.bridgeCapable,
      managedBridgeState: this.managedBridgeState,
      localBrokerControlReport: this.controlService?.getStatusReport() ?? null,
    });
  }

  handleLocalBrokerObserverReachabilityChanged() {
    return this.notifyReevaluationState();
  }

  setReevaluationStateHandler(handler) {
    if (handler !== null && typeof handler !== 'function') {
      throw new TypeError('handler must be a function or null');
    }

    this.reevaluationStateHandler = handler;
    return this.getReevaluationState();
  }

  notifyReevaluationState() {
    if (typeof this.reevaluationStateHandler !== 'function') {
      return { ok: true, notified: false };
    }

    try {
      this.reevaluationStateHandler(this.getReevaluationState());
      return { ok: true, notified: true };
    } catch (error) {
      return { ok: false, notified: false, error };
    }
  }

  async initialize() {
    const storageResult = await this.initializeBaseRuntime();

    if (!storageResult?.ok) {
      return { ok: false, storageResult, primarySessionResult: null };
    }

    const profileInitializationResult =
      await this.initializeRuntimeProfile();

    if (!profileInitializationResult.ok) {
      return {
        ok: false,
        storageResult,
        profileInitializationResult,
      };
    }

    const reevaluationActivationResult =
      await this.ensurePeriodicReevaluationAndReevaluate();

    return {
      ok: reevaluationActivationResult.ok === true,
      storageResult,
      profileInitializationResult,
      reevaluationActivationResult,
    };
  }

  async initializeBaseRuntime() {
    const storageResult = await this.sessionManager.initialize();

    if (!storageResult?.ok) {
      return storageResult;
    }

    this.initialized = true;
    return storageResult;
  }

  async initializeRuntimeProfile() {
    if (this.runtimeProfile === null) {
      return { ok: true, profileApplied: false };
    }

    if (this.runtimeProfile === RUNTIME_PROFILE.PRIMARY_BROKER) {
      if (
        !this.hasLocalBrokerManager() ||
        this.localBrokerType !== BROKER_ROLE.PRIMARY
      ) {
        return {
          ok: false,
          error:
            'PRIMARY_BROKER requires a local broker manager of type PRIMARY',
        };
      }

      const assignmentResult = this.setAssignedPrimaryBrokerId(this.nodeId);
      const activationResult = await this.activateLocalBroker();

      return {
        ok: assignmentResult.ok === true && activationResult.ok === true,
        profileApplied: true,
        assignmentResult,
        activationResult,
      };
    }

    if (this.runtimeProfile === RUNTIME_PROFILE.BASE_TEMPORARY) {
      if (
        !this.hasLocalBrokerManager() ||
        this.localBrokerType !== BROKER_ROLE.TEMPORARY
      ) {
        return {
          ok: false,
          error:
            'BASE_TEMPORARY requires a local broker manager of type TEMPORARY',
        };
      }

      if (this.assignedPrimaryBrokerId === null) {
        return {
          ok: false,
          error: 'BASE_TEMPORARY requires an assigned primary broker',
        };
      }

      const statusMonitoringResult =
        await this.localBrokerManager.startStatusMonitoring(
          LOCAL_BROKER_STATUS_MONITOR_INTERVAL_MS,
        );

      return {
        ok: statusMonitoringResult?.ok === true,
        profileApplied: true,
        statusMonitoringResult,
      };
    }

    if (this.runtimeProfile === RUNTIME_PROFILE.BASE) {
      if (this.hasLocalBrokerManager()) {
        return {
          ok: false,
          error: 'BASE must not declare a local broker manager',
        };
      }

      if (this.assignedPrimaryBrokerId === null) {
        return {
          ok: false,
          error: 'BASE requires an assigned primary broker',
        };
      }

      return { ok: true, profileApplied: true };
    }

    return {
      ok: false,
      error: `Unsupported runtime profile '${String(this.runtimeProfile)}'`,
    };
  }

  async initializeBrokerSession(brokerId) {
    assertNonEmptyString(brokerId, 'brokerId');
    const broker = this.knownBrokers.get(brokerId);

    if (!broker) {
      return { ok: false, brokerId, error: `Unknown broker '${brokerId}'` };
    }

    const sessionId = brokerId;
    const sessionState = this.sessionManager.getSessionState(sessionId);
    const creationResult = sessionState
      ? { ok: true, sessionId, alreadyCreated: true }
      : this.sessionManager.createSession(
          broker.brokerUrl,
          sessionId,
          this.nodeId,
        );

    if (!creationResult.ok) {
      return { ok: false, brokerId, creationResult };
    }

    const connectionResult = await this.sessionManager.connectSession(
      sessionId,
      this.reportTimeoutMs,
    );

    if (!connectionResult.ok) {
      return { ok: false, brokerId, creationResult, connectionResult };
    }

    const responseSubscriptionResult = await this.sessionManager.subscribe(
      sessionId,
      `${this.nodeId}/response`,
      this.handleBrokerStatusReport,
    );
    const statusSubscriptionResult = await this.sessionManager.subscribe(
      sessionId,
      BROKER_STATUS_CHANGED_TOPIC,
      this.handleBrokerStatusChanged,
    );
    const resyncResponseSubscriptionResult = await this.sessionManager.subscribe(
      sessionId,
      this.getResyncResponseTopic(sessionId),
      this.handleResyncBatchResponse,
      1,
    );
    const managedEntityResponseSubscriptionResult = await this.sessionManager.subscribe(
      sessionId,
      buildManagedEntityJoinResponseTopic(this.nodeId, sessionId),
      this.handleManagedEntityJoinResponse,
      1,
    );
    const managedEntityGroupCheckSubscriptionResult = await this.sessionManager.subscribe(
      sessionId,
      'control/manager/+/group/check/request',
      this.handleBrokerGroupCheckRequest,
      1,
    );

    return {
      ok:
        responseSubscriptionResult.ok === true &&
        statusSubscriptionResult.ok === true &&
        resyncResponseSubscriptionResult.ok === true &&
        managedEntityResponseSubscriptionResult.ok === true &&
        managedEntityGroupCheckSubscriptionResult.ok === true,
      brokerId,
      sessionId,
      creationResult,
      connectionResult,
      responseSubscriptionResult,
      statusSubscriptionResult,
      resyncResponseSubscriptionResult,
      managedEntityResponseSubscriptionResult,
      managedEntityGroupCheckSubscriptionResult,
    };
  }

  async handlePartitionReplica(message, context = {}) {
    const messageId = message.getMessageId?.() ?? message.messageId ?? null;
    const sourceNodeId = message.source?.nodeId ?? null;
    const payload = message.getPayload?.() ?? message.payload ?? null;
    const sessionId = context.sessionId ?? null;

    console.log('[PARTITION_REPLICA] Received operational replica.', {
      messageId,
      sourceNodeId,
      payload,
      sessionId,
    });

    return { ok: true, messageId, sessionId };
  }

  async handleReceiptConfirmation(message) {
    if (message.operation !== MESSAGE_OPERATIONS.EVENT_RECEIPT_CONFIRMATION) {
      return { ok: true, ignored: true };
    }

    const payload = message.getPayload?.() ?? message.payload ?? null;
    const messageId = payload?.messageId;

    if (typeof messageId !== 'string' || messageId.trim() === '') {
      return {
        ok: false,
        error: 'Receipt confirmation requires a messageId',
      };
    }

    try {
      const synchronizationResult = await this.sessionManager.markMessageAsSynced(
        messageId,
      );

      return {
        ok: synchronizationResult?.ok === true,
        messageId,
        synchronizationResult,
      };
    } catch (error) {
      return {
        ok: false,
        messageId,
        error,
      };
    }
  }

  async installOperationalSubscriptions(sessionId) {
    assertNonEmptyString(sessionId, 'sessionId');

    const partitionSubscriptionResult = await this.sessionManager.subscribe(
      sessionId,
      PARTITION_REPLICA_TOPIC,
      this.handlePartitionReplica,
      1,
    );
    const confirmationSubscriptionResult = await this.sessionManager.subscribe(
      sessionId,
      `confirmation/${this.nodeId}/message`,
      this.handleReceiptConfirmation,
      1,
    );

    return {
      ok:
        partitionSubscriptionResult?.ok === true &&
        confirmationSubscriptionResult?.ok === true,
      sessionId,
      partitionSubscriptionResult,
      confirmationSubscriptionResult,
    };
  }

  async prepareOperationalSession(brokerId) {
    const connectionResult = await this.initializeBrokerSession(brokerId);

    if (!connectionResult?.ok) {
      return { ok: false, connectionResult };
    }

    const sessionState = this.sessionManager.getSessionState(
      connectionResult.sessionId,
    );

    if (!sessionState?.connected) {
      if (connectionResult.creationResult?.alreadyCreated === false) {
        await this.sessionManager.closeSession(connectionResult.sessionId);
      }

      return {
        ok: false,
        connectionResult,
        sessionState,
      };
    }

    return {
      ok: true,
      sessionId: connectionResult.sessionId,
      connectionResult,
      sessionState,
    };
  }

  async requestManagedEntityJoin(sessionId, targetBrokerNodeId) {
    assertNonEmptyString(sessionId, 'sessionId');
    assertNonEmptyString(targetBrokerNodeId, 'targetBrokerNodeId');

    const correlationId = randomUUID();
    let joinResponsePromise;

    try {
      joinResponsePromise = this.pendingRequestRegistry.register({
        correlationId,
        operationType: MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_REQUEST,
        timeoutMs: this.reportTimeoutMs,
        metadata: { sessionId, targetBrokerNodeId },
      });
    } catch (error) {
      return { ok: false, error, correlationId };
    }

    const publishResult = await this.sendThroughSession(
      sessionId,
      buildNodeInboxTopic(targetBrokerNodeId, targetBrokerNodeId),
      { replyTo: buildManagedEntityJoinResponseTopic(this.nodeId, sessionId) },
      MESSAGE_TYPES.EVENT,
      MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_REQUEST,
      true,
      correlationId,
    );

    if (!publishResult.ok) {
      this.pendingRequestRegistry.clear(correlationId, {
        ok: false,
        error: publishResult.error ?? 'Managed entity join request could not be published',
      });
    }

    const joinResponse = await joinResponsePromise;

    return {
      ok: joinResponse?.ok === true && joinResponse.accepted === true,
      accepted: joinResponse?.accepted === true,
      correlationId,
      publishResult,
      joinResponse,
    };
  }

  async rollbackBrokerPreparation(currentBrokerNodeId, preparedSessionId) {
    if (currentBrokerNodeId === preparedSessionId) {
      return { ok: true, skipped: true, reason: 'CURRENT_BROKER' };
    }

    return this.sessionManager.closeSession(preparedSessionId);
  }

  async switchBroker(currentBrokerNodeId, targetBrokerNodeId) {
    const preparationResult = await this.prepareOperationalSession(
      targetBrokerNodeId,
    );

    if (!preparationResult?.ok) {
      return {
        ok: false,
        previousBrokerNodeId: currentBrokerNodeId,
        targetBrokerNodeId,
        preparationResult,
      };
    }

    const joinResult = await this.requestManagedEntityJoin(
      preparationResult.sessionId,
      targetBrokerNodeId,
    );

    if (!joinResult.ok) {
      const rollbackResult = await this.rollbackBrokerPreparation(
        currentBrokerNodeId,
        preparationResult.sessionId,
      );

      return {
        ok: false,
        error: joinResult.accepted === false
          ? 'Managed entity join was rejected'
          : joinResult.joinResponse?.error ?? 'Managed entity join request failed',
        previousBrokerNodeId: currentBrokerNodeId,
        targetBrokerNodeId,
        preparationResult,
        joinResult,
        rollbackResult,
      };
    }

    const operationalSubscriptionsResult =
      await this.installOperationalSubscriptions(preparationResult.sessionId);

    if (!operationalSubscriptionsResult.ok) {
      const cleanupResult = await this.rollbackBrokerPreparation(
        currentBrokerNodeId,
        preparationResult.sessionId,
      );

      return {
        ok: false,
        previousBrokerNodeId: currentBrokerNodeId,
        targetBrokerNodeId,
        preparationResult,
        joinResult,
        operationalSubscriptionsResult,
        cleanupResult,
      };
    }

    const cleanupResult =
      currentBrokerNodeId && currentBrokerNodeId !== targetBrokerNodeId
        ? await this.sessionManager.closeSession(currentBrokerNodeId)
        : { ok: true, skipped: true };

    return {
      ok: true,
      changed: currentBrokerNodeId !== targetBrokerNodeId,
      previousBrokerNodeId: currentBrokerNodeId,
      targetBrokerNodeId,
      sessionId: preparationResult.sessionId,
      preparationResult,
      joinResult,
      operationalSubscriptionsResult,
      cleanupResult,
    };
  }

  async activateLocalBroker() {
    if (this.localBrokerActivationPromise) {
      return this.localBrokerActivationPromise;
    }

    const pendingDeactivationPromise = this.localBrokerDeactivationPromise;
    const activationPromise = Promise.resolve().then(async () => {
      if (!this.hasLocalBrokerManager()) {
        if (this.runtimeProfile !== null) {
          return {
            ok: false,
            error: 'No local broker manager is declared for this runtime profile',
          };
        }

        if (this.localBrokerActive) {
          return { ok: true, alreadyActive: true, simulated: true };
        }

        console.log('[PLATFORM_BROKER] Local broker activated.');
        this.localBrokerActive = true;
        return { ok: true, activated: true, simulated: true };
      }

      if (pendingDeactivationPromise) {
        await pendingDeactivationPromise;
      }

      let brokerStatusResult;
      let startStatusMonitoringResult;
      let startBrokerResult = null;
      let usableBrokerStatusResult = null;
      let controlService = null;
      const controlServiceWasActive = this.controlService !== null;
      let activationPhase = 'START_STATUS_MONITORING';

      try {
        startStatusMonitoringResult =
          await this.localBrokerManager.startStatusMonitoring(
            LOCAL_BROKER_STATUS_MONITOR_INTERVAL_MS,
          );
        if (startStatusMonitoringResult?.ok !== true) {
          throw new Error(
            localBrokerFailureMessage(
              'Unable to start local broker status monitoring',
              startStatusMonitoringResult,
            ),
          );
        }

        activationPhase = 'READ_INITIAL_STATUS';
        brokerStatusResult = await this.localBrokerManager.getBrokerStatus();
        if (brokerStatusResult?.ok !== true) {
          throw new Error(
            localBrokerFailureMessage(
              'Unable to determine local broker status',
              brokerStatusResult,
            ),
          );
        }

        if (brokerStatusResult.usable !== true) {
          activationPhase = 'START_BROKER';
          startBrokerResult = await this.localBrokerManager.startBroker();
          if (startBrokerResult?.ok !== true) {
            throw new Error(
              localBrokerFailureMessage(
                'Unable to start the local broker',
                startBrokerResult,
              ),
            );
          }
        }

        activationPhase = 'READ_USABLE_STATUS';
        usableBrokerStatusResult =
          await this.localBrokerManager.getBrokerStatus();
        if (
          usableBrokerStatusResult?.ok !== true ||
          usableBrokerStatusResult.usable !== true
        ) {
          throw new Error(
            localBrokerFailureMessage(
              'Local broker is not usable after activation',
              usableBrokerStatusResult,
            ),
          );
        }

        activationPhase = 'RESOLVE_ENDPOINT';
        const endpointResult = this.localBrokerManager.getBrokerEndpoint();
        if (endpointResult?.ok !== true) {
          throw new Error(
            localBrokerFailureMessage(
              'Local broker endpoint is unavailable',
              endpointResult,
            ),
          );
        }

        controlService = this.controlService;
        if (!controlService) {
          activationPhase = 'INITIALIZE_CONTROL_SERVICE';
          controlService = new ControlService(
            endpointResult.endpoint.brokerUrl,
            `control-${this.nodeId}`,
            `control-client-${this.nodeId}`,
            this.nodeId,
            CONTROL_SERVICE_HEALTH_REQUEST_TIMEOUT_MS,
            this.localBrokerType,
            LOCAL_BROKER_BATTERY_PERCENTAGE,
            this.sessionManager.securityManager,
            this.sessionManager.messageSecurityKeyId,
          );
          await controlService.initializationPromise;
        }

        controlService.setObserverReachabilityChangeHandler(
          this.handleLocalBrokerObserverReachabilityChanged,
        );

        activationPhase = 'REGISTER_LOCAL_BROKER';
        const registrationResult = this.addBroker(
          this.getLocalBrokerId(),
          endpointResult.endpoint.brokerUrl,
          this.localBrokerType,
        );
        if (registrationResult.ok !== true) {
          throw new Error(
            localBrokerFailureMessage(
              'Unable to register the local broker',
              registrationResult,
            ),
          );
        }

        this.controlService = controlService;
        this.localBrokerActive = true;
        this.notifyReevaluationState();

        return {
          ok: true,
          alreadyActive:
            brokerStatusResult.brokerProcessStatus ===
              BROKER_PROCESS_STATUS.RUNNING && controlServiceWasActive,
          activated: true,
          brokerStatusResult,
          startStatusMonitoringResult,
          startBrokerResult,
          usableBrokerStatusResult,
          registrationResult,
        };
      } catch (error) {
        let controlServiceResult = { ok: true, skipped: true };
        let brokerStopResult;
        let statusMonitoringResult;
        let registrationRemovalResult;

        if (controlService) {
          try {
            const destroyResult = await controlService.destroy();
            controlServiceResult = {
              ok: destroyResult?.ok !== false,
              destroyResult,
            };
          } catch (destroyError) {
            controlServiceResult = {
              ok: false,
              error: destroyError?.message ?? String(destroyError),
            };
          }
        }

        if (this.controlService === controlService) {
          this.controlService = null;
        }

        try {
          brokerStopResult = await this.localBrokerManager.stopBroker(false);
        } catch (stopError) {
          brokerStopResult = {
            ok: false,
            error: stopError?.message ?? String(stopError),
          };
        }

        try {
          statusMonitoringResult =
            this.localBrokerManager.stopStatusMonitoring();
        } catch (stopMonitoringError) {
          statusMonitoringResult = {
            ok: false,
            error:
              stopMonitoringError?.message ?? String(stopMonitoringError),
          };
        }

        const localBrokerId = this.getLocalBrokerId();
        if (!this.knownBrokers.has(localBrokerId)) {
          registrationRemovalResult = {
            ok: true,
            skipped: true,
            brokerId: localBrokerId,
          };
        } else {
          try {
            registrationRemovalResult = await this.removeBroker(localBrokerId);
          } catch (removalError) {
            registrationRemovalResult = {
              ok: false,
              brokerId: localBrokerId,
              error: removalError?.message ?? String(removalError),
            };
          }
        }

        this.localBrokerActive = false;
        return {
          ok: false,
          error: `${activationPhase}: ${error?.message ?? String(error)}`,
          startStatusMonitoringResult,
          startBrokerResult,
          brokerStatusResult: usableBrokerStatusResult ?? brokerStatusResult,
          cleanup: {
            controlServiceResult,
            brokerStopResult: {
              ok:
                brokerStopResult?.ok === true &&
                statusMonitoringResult?.ok === true,
              brokerStopResult,
              statusMonitoringResult,
            },
            registrationRemovalResult,
          },
        };
      }
    });
    this.localBrokerActivationPromise = activationPromise;

    try {
      return await activationPromise;
    } finally {
      if (this.localBrokerActivationPromise === activationPromise) {
        this.localBrokerActivationPromise = null;
      }
    }
  }

  getLocalBrokerId() {
    return this.hasLocalBrokerManager()
      ? this.nodeId
      : this.getFakeLocalBrokerId();
  }

  getFakeLocalBrokerId() {
    return `local-${this.nodeId}`;
  }

  async deactivateLocalBroker() {
    if (this.localBrokerDeactivationPromise) {
      return this.localBrokerDeactivationPromise;
    }

    const pendingActivationPromise = this.localBrokerActivationPromise;
    const deactivationPromise = Promise.resolve().then(async () => {
      if (pendingActivationPromise) {
        await pendingActivationPromise;
      }

      if (!this.hasLocalBrokerManager()) {
        if (!this.localBrokerActive) {
          return { ok: true, skipped: true, simulated: true };
        }

        console.log('[PLATFORM_BROKER] Local broker deactivated.');
        this.localBrokerActive = false;
        return { ok: true, deactivated: true, simulated: true };
      }

      let controlServiceResult = { ok: true, skipped: true };
      let brokerStopResult;
      let statusMonitoringResult;
      let registrationRemovalResult;

      if (this.controlService) {
        try {
          const destroyResult = await this.controlService.destroy();
          controlServiceResult = {
            ok: destroyResult?.ok !== false,
            destroyResult,
          };
        } catch (error) {
          controlServiceResult = {
            ok: false,
            error: error?.message ?? String(error),
          };
        }
      }
      this.controlService = null;

      try {
        brokerStopResult = await this.localBrokerManager.stopBroker(false);
      } catch (error) {
        brokerStopResult = {
          ok: false,
          error: error?.message ?? String(error),
        };
      }

      try {
        statusMonitoringResult =
          this.localBrokerManager.stopStatusMonitoring();
      } catch (error) {
        statusMonitoringResult = {
          ok: false,
          error: error?.message ?? String(error),
        };
      }

      const localBrokerId = this.getLocalBrokerId();
      if (!this.knownBrokers.has(localBrokerId)) {
        registrationRemovalResult = {
          ok: true,
          skipped: true,
          brokerId: localBrokerId,
        };
      } else {
        try {
          registrationRemovalResult = await this.removeBroker(localBrokerId);
        } catch (error) {
          registrationRemovalResult = {
            ok: false,
            brokerId: localBrokerId,
            error: error?.message ?? String(error),
          };
        }
      }

      this.localBrokerActive = false;

      return {
        ok:
          controlServiceResult.ok === true &&
          brokerStopResult?.ok === true &&
          statusMonitoringResult?.ok === true &&
          registrationRemovalResult.ok === true,
        deactivated: true,
        controlServiceResult,
        brokerStopResult,
        statusMonitoringResult,
        registrationRemovalResult,
      };
    });
    this.localBrokerDeactivationPromise = deactivationPromise;

    try {
      return await deactivationPromise;
    } finally {
      if (this.localBrokerDeactivationPromise === deactivationPromise) {
        this.localBrokerDeactivationPromise = null;
      }
    }
  }

  async applyModeTransition(nextMode, nextBrokerNodeId) {
    const previousBrokerNodeId =
      this.platformModeManager.getCurrentBrokerNodeId();
    const switchResult = nextBrokerNodeId
      ? await this.switchBroker(previousBrokerNodeId, nextBrokerNodeId)
      : previousBrokerNodeId
      ? {
          ok: true,
          cleanupResult: await this.sessionManager.closeSession(
            previousBrokerNodeId,
          ),
        }
      : { ok: true, skipped: true };

    if (!switchResult?.ok) {
      return { ok: false, switchResult };
    }

    const brokerSelectionResult = this.platformModeManager.selectBroker(
      nextBrokerNodeId ?? null,
    );
    this.operationalSessionId = nextBrokerNodeId ?? null;
    const modeTransitionResult = this.platformModeManager.transitionMode(
      nextMode,
    );
    return {
      ok: true,
      switchResult,
      brokerSelectionResult,
      modeTransitionResult,
    };
  }

  async sendThroughSession(
    sessionId,
    topic,
    payload,
    type = MESSAGE_TYPES.EVENT,
    operation = null,
    requiresResponse = false,
    correlationId = null,
  ) {
    assertNonEmptyString(sessionId, 'sessionId');
    assertNonEmptyString(topic, 'topic');

    const sessionState = this.sessionManager.getSessionState(sessionId);

    if (!sessionState || !sessionState.connected) {
      return {
        ok: false,
        sessionId,
        topic,
        error: `Session '${sessionId}' is not connected`,
      };
    }

    return this.sessionManager.publish(
      sessionId,
      topic,
      payload,
      type,
      operation,
      requiresResponse,
      correlationId,
    );
  }

  async subscribe(sessionId, topicFilter, handler, qos = 0) {
    assertNonEmptyString(sessionId, 'sessionId');
    assertNonEmptyString(topicFilter, 'topicFilter');

    if (typeof handler !== 'function') {
      throw new TypeError('handler must be a function');
    }

    const sessionState = this.sessionManager.getSessionState(sessionId);

    if (!sessionState || !sessionState.connected) {
      return {
        ok: false,
        sessionId,
        topicFilter,
        error: `Session '${sessionId}' is not connected`,
      };
    }

    return this.sessionManager.subscribe(sessionId, topicFilter, handler, qos);
  }

  async send(payload) {
    const mode = this.platformModeManager.getCurrentMode();
    const topic =
      mode === MIDDLEWARE_MODES.OFFLINE
        ? PARTITION_REPLICA_TOPIC
        : OPERATION_INFORMATION_TOPIC;

    try {
      const result = await this.sessionManager.publish(
        this.operationalSessionId,
        topic,
        payload,
      );

      return {
        ...result,
        partitionReplication:
          topic === PARTITION_REPLICA_TOPIC && result.ok === true,
      };
    } catch (error) {
      if (!(error instanceof SessionNotFoundError)) {
        throw error;
      }

      return this.sessionManager.persistOperationalMessage(topic, payload);
    }
  }

  getResyncResponseTopic(sessionId) {
    assertNonEmptyString(sessionId, 'sessionId');
    return buildResyncBatchResponseTopic(this.nodeId, sessionId);
  }

  async getPendingSynchronizationState() {
    const currentMode = this.platformModeManager.getCurrentMode();
    const operationalSessionId = this.operationalSessionId;
    const hasSelectedGlobalRoute =
      currentMode === MIDDLEWARE_MODES.NORMAL ||
      currentMode === MIDDLEWARE_MODES.FALLBACK;

    if (
      !hasSelectedGlobalRoute ||
      typeof operationalSessionId !== 'string' ||
      operationalSessionId.trim() === ''
    ) {
      return {
        eligible: false,
        currentMode,
        operationalSessionId,
        pendingMessages: [],
      };
    }

    const pendingMessages = await this.sessionManager.getPendingMessages();

    return {
      eligible: pendingMessages.length > 0,
      currentMode,
      operationalSessionId,
      pendingMessages,
    };
  }

  async synchronizePendingMessages() {
    if (this.pendingSynchronizationInFlight) {
      return {
        ok: true,
        skipped: true,
        reason: 'synchronization-already-in-progress',
      };
    }

    const synchronizationState = await this.getPendingSynchronizationState();

    if (!synchronizationState.eligible) {
      this.platformModeManager.setPendingResync(false);
      this.stopPeriodicPendingSynchronization();
      return {
        ok: true,
        skipped: true,
        reason: 'synchronization-not-eligible',
        synchronizationState,
      };
    }

    const targetMode = synchronizationState.currentMode;
    const sessionId = synchronizationState.operationalSessionId;
    this.pendingSynchronizationInFlight = true;
    this.platformModeManager.setPendingResync(true);
    this.platformModeManager.transitionMode(MIDDLEWARE_MODES.RESYNC);
    this.notifyReevaluationState();

    try {
      const processingResult = await this.processPendingMessagesByBatches(
        sessionId,
        synchronizationState.pendingMessages,
      );
      const remainingPendingMessages =
        await this.sessionManager.getPendingMessages();
      const result = {
        ok: processingResult.ok === true,
        sessionId,
        targetMode,
        processingResult,
        remainingPendingMessageCount: remainingPendingMessages.length,
        completedAt: new Date().toISOString(),
      };
      this.lastPendingSynchronizationResult = result;

      if (remainingPendingMessages.length > 0) {
        this.startPeriodicPendingSynchronization();
      } else {
        this.stopPeriodicPendingSynchronization();
      }

      return result;
    } catch (error) {
      const result = {
        ok: false,
        sessionId,
        targetMode,
        error: error?.message ?? String(error),
        completedAt: new Date().toISOString(),
      };
      this.lastPendingSynchronizationResult = result;
      this.startPeriodicPendingSynchronization();
      return result;
    } finally {
      this.pendingSynchronizationInFlight = false;
      this.currentPendingSynchronizationBatch = null;
      this.platformModeManager.setPendingResync(false);
      this.platformModeManager.transitionMode(targetMode);
      this.notifyReevaluationState();
    }
  }

  async processPendingMessagesByBatches(sessionId, pendingMessages) {
    const snapshot = pendingMessages.slice();
    const batchResults = [];

    for (
      let index = 0;
      index < snapshot.length;
      index += RESYNC_BATCH_MESSAGE_COUNT
    ) {
      const batchEntries = snapshot.slice(
        index,
        index + RESYNC_BATCH_MESSAGE_COUNT,
      );
      batchResults.push(
        await this.forwardPersistedMessageBatch(sessionId, batchEntries),
      );
    }

    return {
      ok: batchResults.every(result => result.ok === true),
      attemptedMessageCount: snapshot.length,
      batchResults,
    };
  }

  async forwardPersistedMessageBatch(sessionId, batchEntries) {
    const messages = batchEntries.map(entry => (
      entry.encryptedMessage ?? entry.message
    ).toJSON());
    const requestedMessageIds = messages.map(message => message.messageId);
    const correlationId = randomUUID();
    const startedAt = new Date().toISOString();
    this.currentPendingSynchronizationBatch = {
      status: RESYNC_BATCH_STATUS.IN_PROGRESS,
      correlationId,
      sessionId,
      requestedMessageIds,
      startedAt,
    };
    this.notifyReevaluationState();

    const responsePromise = this.pendingRequestRegistry.register({
      correlationId,
      operationType: MESSAGE_OPERATIONS.RESYNC_BATCH_REQUEST,
      timeoutMs: RESYNC_BATCH_RESPONSE_TIMEOUT_MS,
      metadata: { sessionId, requestedMessageIds },
    });
    const publishResult = await this.sendThroughSession(
      sessionId,
      RESYNC_BATCH_REQUEST_TOPIC,
      {
        messages,
        replyTo: this.getResyncResponseTopic(sessionId),
      },
      MESSAGE_TYPES.EVENT,
      MESSAGE_OPERATIONS.RESYNC_BATCH_REQUEST,
      true,
      correlationId,
    );

    if (!publishResult.ok) {
      this.pendingRequestRegistry.clear(correlationId, {
        ok: false,
        error: publishResult.error ?? 'Resync batch could not be published',
      });
    }

    const responseResult = await responsePromise;
    let result;

    if (!responseResult?.ok) {
      result = {
        ok: false,
        status: RESYNC_BATCH_STATUS.FAILED,
        correlationId,
        requestedMessageIds,
        publishResult,
        responseResult,
        startedAt,
        completedAt: new Date().toISOString(),
      };
    } else {
      const synchronizationResults = await Promise.all(
        responseResult.receivedMessageIds.map(messageId =>
          this.sessionManager.markMessageAsSynced(messageId),
        ),
      );
      const receivedAllRequestedMessages = requestedMessageIds.every(
        messageId => responseResult.receivedMessageIds.includes(messageId),
      );
      const synchronized =
        receivedAllRequestedMessages &&
        synchronizationResults.every(
          synchronizationResult => synchronizationResult.ok === true,
        );
      result = {
        ok: synchronized,
        status: synchronized
          ? RESYNC_BATCH_STATUS.COMPLETED
          : RESYNC_BATCH_STATUS.FAILED,
        correlationId,
        requestedMessageIds,
        receivedMessageIds: responseResult.receivedMessageIds,
        publishResult,
        responseResult,
        synchronizationResults,
        startedAt,
        completedAt: new Date().toISOString(),
      };
    }

    this.currentPendingSynchronizationBatch = null;
    this.notifyReevaluationState();
    return result;
  }

  async handleResyncBatchResponse(message, context = {}) {
    if (message.operation !== MESSAGE_OPERATIONS.RESYNC_BATCH_RESPONSE) {
      return { ok: true, ignored: true };
    }

    const correlationId = message.getCorrelationId();
    const pendingRequest = this.pendingRequestRegistry.get(correlationId);

    if (
      !pendingRequest ||
      pendingRequest.operationType !== MESSAGE_OPERATIONS.RESYNC_BATCH_REQUEST
    ) {
      return { ok: true, ignored: true };
    }

    if (context.sessionId !== pendingRequest.metadata.sessionId) {
      this.pendingRequestRegistry.resolve(correlationId, {
        ok: false,
        error: 'Resync batch response arrived through another session',
      });
      return {
        ok: false,
        error: 'Resync batch response arrived through another session',
      };
    }

    const receivedMessageIds = message.getPayload()?.receivedMessageIds;

    if (!Array.isArray(receivedMessageIds)) {
      this.pendingRequestRegistry.resolve(correlationId, {
        ok: false,
        error: 'Resync batch response must contain receivedMessageIds',
      });
      return {
        ok: false,
        error: 'Resync batch response must contain receivedMessageIds',
      };
    }

    const uniqueReceivedMessageIds = Array.from(
      new Set(
        receivedMessageIds
          .filter(
            messageId =>
              typeof messageId === 'string' && messageId.trim() !== '',
          )
          .map(messageId => messageId.trim()),
      ),
    );
    const requestedMessageIds = new Set(
      pendingRequest.metadata.requestedMessageIds,
    );
    const hasUnexpectedMessageId = uniqueReceivedMessageIds.some(
      messageId =>
        !requestedMessageIds.has(messageId) ||
        !this.sessionManager.getMessageEntry(messageId),
    );

    if (hasUnexpectedMessageId) {
      this.pendingRequestRegistry.resolve(correlationId, {
        ok: false,
        error: 'Resync batch response contains an unknown messageId',
      });
      return {
        ok: false,
        error: 'Resync batch response contains an unknown messageId',
      };
    }

    this.pendingRequestRegistry.resolve(correlationId, {
      ok: true,
      receivedMessageIds: uniqueReceivedMessageIds,
    });
    return {
      ok: true,
      correlationId,
      receivedMessageIds: uniqueReceivedMessageIds,
    };
  }

  startPeriodicPendingSynchronization() {
    if (this.periodicPendingSynchronizationTimer) {
      return { ok: true, alreadyRunning: true };
    }

    this.periodicPendingSynchronizationTimer = setInterval(() => {
      this.synchronizePendingMessages().catch(() => undefined);
    }, this.reevaluationIntervalMs);

    return { ok: true, started: true, intervalMs: this.reevaluationIntervalMs };
  }

  stopPeriodicPendingSynchronization() {
    if (!this.periodicPendingSynchronizationTimer) {
      return { ok: true, alreadyStopped: true };
    }

    clearInterval(this.periodicPendingSynchronizationTimer);
    this.periodicPendingSynchronizationTimer = null;
    return { ok: true, stopped: true };
  }

  async withTimeout(promise, timeoutMs, timeoutResult) {
    let timeoutId = null;

    const timeoutPromise = new Promise(resolve => {
      timeoutId = setTimeout(() => {
        resolve(timeoutResult);
      }, timeoutMs);
    });

    try {
      return await Promise.race([promise, timeoutPromise]);
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  }

  async requestBrokerReport(brokerId) {
    assertNonEmptyString(brokerId, 'brokerId');
    const broker = this.knownBrokers.get(brokerId);

    if (!broker) {
      return { ok: false, brokerId, error: `Unknown broker '${brokerId}'` };
    }

    const sessionResult = await this.withTimeout(
      this.initializeBrokerSession(brokerId),
      this.reportTimeoutMs,
      {
        ok: false,
        error: `Connection probe timeout after ${this.reportTimeoutMs}ms`,
      },
    );

    if (!sessionResult.ok) {
      if (this.sessionManager.getSessionState(brokerId)) {
        await this.sessionManager.closeSession(brokerId);
      }

      return { ok: false, brokerId, sessionReachable: false, sessionResult };
    }

    const sessionId = sessionResult.sessionId;
    const correlationId = randomUUID();
    const responsePromise = this.pendingRequestRegistry.register({
      correlationId,
      operationType: MESSAGE_OPERATIONS.PROBE_STATUS_REQUEST,
      timeoutMs: this.reportTimeoutMs,
      metadata: { brokerId, brokerUrl: broker.brokerUrl },
    });
    const publishResult = await this.sendThroughSession(
      sessionId,
      BROKER_STATUS_REQUEST_TOPIC,
      null,
      MESSAGE_TYPES.EVENT,
      MESSAGE_OPERATIONS.PROBE_STATUS_REQUEST,
      true,
      correlationId,
    );

    if (!publishResult.ok) {
      this.pendingRequestRegistry.clear(correlationId, {
        ok: false,
        error:
          publishResult.error ?? 'Broker report request could not be published',
      });
    }

    const responseResult = await responsePromise;

    return {
      ...responseResult,
      brokerId,
      sessionReachable: responseResult?.ok === true,
      publishResult,
    };
  }

  async collectBrokerReports() {
    const reportResults = await Promise.all(
      this.getKnownBrokers().map(broker =>
        this.requestBrokerReport(broker.brokerId),
      ),
    );

    return {
      ok: true,
      reportResults,
      reports: reportResults
        .filter(result => result.ok === true && result.report)
        .map(result => result.report),
    };
  }

  async reevaluate() {
    if (this.reevaluationInFlight) {
      console.log('[PlatformAdapter] Reevaluation skipped: already in progress', {
        nodeId: this.nodeId,
        assignedPrimaryBrokerId: this.assignedPrimaryBrokerId,
      });
      return {
        ok: true,
        skipped: true,
        error: 'Reevaluation already in progress',
      };
    }

    this.reevaluationInFlight = true;
    console.log('[PlatformAdapter] Reevaluation started', {
      nodeId: this.nodeId,
      assignedPrimaryBrokerId: this.assignedPrimaryBrokerId,
      knownBrokerIds: this.getKnownBrokers().map(broker => broker.brokerId),
    });
    this.notifyReevaluationState();

    try {
      const collectionResult = await this.collectBrokerReports();
      const reports = [...collectionResult.reports];
      const snapshot = ReevaluationSnapshot.fromReports(
        reports,
        this.platformModeManager.getAssignedPrimaryBrokerId(),
      );

      this.lastSnapshot = snapshot;
      const attemptedBrokerIds = new Set();
      let localBrokerActivationAttempted = false;
      let localTemporaryManagerActivated = false;
      const adaptationActions = [];
      let lastActionResult = null;
      let selectedCandidate = null;

      const tryRouteTiers = async (routeTiers, findCandidate) => {
        for (const routePriorityTier of routeTiers) {
          let candidate = findCandidate(
            snapshot,
            routePriorityTier,
            attemptedBrokerIds,
          );

          while (candidate && !selectedCandidate) {
            attemptedBrokerIds.add(candidate.brokerId);
            lastActionResult = await this.applyModeTransition(
              resolveCandidateMode(candidate),
              candidate.brokerId,
            );
            adaptationActions.push({
              brokerId: candidate.brokerId,
              ok: lastActionResult.ok === true,
              routePriorityTier,
            });
            this.lastAdaptationAction = {
              brokerId: candidate.brokerId,
              result: lastActionResult,
            };

            if (lastActionResult.ok) {
              selectedCandidate = candidate;
              return;
            }

            candidate = findCandidate(
              snapshot,
              routePriorityTier,
              attemptedBrokerIds,
            );
          }
        }
      };

      await tryRouteTiers(
        USABLE_ROUTE_TIERS_BEFORE_TEMPORARY_ACTIVATION,
        findNextUsableRouteCandidate,
      );

      if (
        !selectedCandidate &&
        !localBrokerActivationAttempted &&
        this.runtimeProfile === RUNTIME_PROFILE.BASE_TEMPORARY
      ) {
        localBrokerActivationAttempted = true;
        lastActionResult = await this.activateLocalBroker();
        localTemporaryManagerActivated = lastActionResult.ok === true;
        adaptationActions.push({
          brokerId: null,
          ok: lastActionResult.ok === true,
          skipped: lastActionResult.skipped === true,
          simulated: lastActionResult.simulated === true,
          action: 'ACTIVATE_LOCAL_TEMPORARY_MANAGER',
        });
        this.lastAdaptationAction = {
          brokerId: null,
          result: lastActionResult,
        };
      }

      if (!selectedCandidate) {
        await tryRouteTiers(
          USABLE_ROUTE_TIERS_AFTER_TEMPORARY_ACTIVATION,
          findNextUsableRouteCandidate,
        );
      }

      if (!selectedCandidate) {
        await tryRouteTiers(
          OBSERVER_UNREACHABLE_ROUTE_TIERS,
          findNextObserverUnreachableRouteCandidate,
        );
      }

      if (!selectedCandidate) {
        lastActionResult = await this.applyModeTransition(
          MIDDLEWARE_MODES.OFFLINE,
          null,
        );
        adaptationActions.push({
          brokerId: null,
          ok: lastActionResult.ok === true,
        });
        this.lastAdaptationAction = {
          brokerId: null,
          result: lastActionResult,
        };
      }

      const pendingSynchronizationResult =
        await this.synchronizePendingMessages();
      const modeState = this.platformModeManager.getState();
      const preferredConfigurationRecovered =
        modeState.currentMode === MIDDLEWARE_MODES.NORMAL &&
        modeState.currentBrokerNodeId ===
          modeState.assignedPrimaryBrokerId;

      const periodicResult = preferredConfigurationRecovered
        ? this.stopPeriodicReevaluation()
        : this.startPeriodicReevaluation();
      const managedBrokerLifecycle =
        this.resolveManagedBrokerLifecycle(selectedCandidate);
      const selectedBrokerEntry = selectedCandidate
        ? this.knownBrokers.get(selectedCandidate.brokerId)
        : null;
      let managedBrokerLifecycleResult = { ok: true, skipped: true };

      if (managedBrokerLifecycle.activateBridge) {
        managedBrokerLifecycleResult = await this.activateManagedBrokerBridge(
          selectedCandidate.brokerId,
          buildBridgeBrokerUrl(
            selectedBrokerEntry?.ip,
            selectedBrokerEntry?.bridgePort,
            this.bridgeUsesTls,
          ),
          selectedCandidate.hopCount,
        );
      } else if (managedBrokerLifecycle.removeBridge) {
        managedBrokerLifecycleResult = await this.disableManagedBrokerBridge();
      }

      if (!managedBrokerLifecycleResult.skipped) {
        adaptationActions.push({
          brokerId: selectedCandidate?.brokerId ?? null,
          ok: managedBrokerLifecycleResult.ok === true,
          bridgeConfigured: this.managedBridgeState?.configured === true,
        });
      }

      const selectedLocalBroker =
        selectedCandidate?.brokerId === this.getLocalBrokerId();
      const deactivationResult =
        selectedCandidate &&
        !selectedLocalBroker &&
        !localTemporaryManagerActivated &&
        this.managedBridgeState?.configured !== true &&
        this.localBrokerActive &&
        this.runtimeProfile !== RUNTIME_PROFILE.PRIMARY_BROKER
          ? await this.deactivateLocalBroker()
          : { ok: true, skipped: true, simulated: true };

      if (!deactivationResult.skipped) {
        adaptationActions.push({
          brokerId: null,
          ok: deactivationResult.ok === true,
          simulated: deactivationResult.simulated === true,
        });
      }

      const adaptationSummary = {
        actions: adaptationActions,
        finalState: {
          brokerId: modeState.currentBrokerNodeId,
          mode: modeState.currentMode,
          sessionId: this.operationalSessionId,
          localBrokerActive: this.localBrokerActive,
          managedBridgeState: this.managedBridgeState,
        },
        pendingSynchronizationResult,
      };
      this.lastAdaptationSummary = adaptationSummary;

      const result = {
        ok: true,
        collectionResult: {
          ...collectionResult,
          reports,
        },
        snapshot,
        lastActionResult,
        adaptationSummary,
        periodicResult,
        managedBrokerLifecycle,
        managedBrokerLifecycleResult,
        deactivationResult,
        pendingSynchronizationResult,
        completedAt: new Date().toISOString(),
      };
      this.lastReevaluationResult = result;
      console.log('[PlatformAdapter] Reevaluation completed', {
        nodeId: this.nodeId,
        reportResults: collectionResult.reportResults.map(reportResult => ({
          brokerId: reportResult.brokerId,
          ok: reportResult.ok,
          sessionReachable: reportResult.sessionReachable,
        })),
        orderedBrokerIds: snapshot.orderedBrokers.map(broker => broker.brokerId),
        adaptationSummary,
        preferredConfigurationRecovered,
      });
      return result;
    } catch (error) {
      const result = {
        ok: false,
        error: error?.message ?? String(error),
        completedAt: new Date().toISOString(),
      };
      this.lastReevaluationResult = result;
      console.error('[PlatformAdapter] Reevaluation failed', {
        nodeId: this.nodeId,
        error: result.error,
      });
      return result;
    } finally {
      this.reevaluationInFlight = false;
      this.notifyReevaluationState();
    }
  }

  startPeriodicReevaluation() {
    if (this.periodicReevaluationTimer) {
      return { ok: true, alreadyRunning: true };
    }

    this.periodicReevaluationTimer = setInterval(() => {
      this.reevaluate().catch(() => undefined);
    }, this.reevaluationIntervalMs);

    return { ok: true, started: true, intervalMs: this.reevaluationIntervalMs };
  }

  async ensurePeriodicReevaluationAndReevaluate(options = {}) {
    const periodicReevaluationResult = this.startPeriodicReevaluation();
    let reevaluationResult;

    try {
      reevaluationResult = await this.reevaluate(options);
    } catch (error) {
      reevaluationResult = {
        ok: false,
        error: error?.message ?? String(error),
      };
    }

    return {
      ok:
        periodicReevaluationResult?.ok === true &&
        reevaluationResult?.ok === true,
      periodicReevaluationResult,
      reevaluationResult,
    };
  }

  stopPeriodicReevaluation() {
    if (!this.periodicReevaluationTimer) {
      return { ok: true, alreadyStopped: true };
    }

    clearInterval(this.periodicReevaluationTimer);
    this.periodicReevaluationTimer = null;
    return { ok: true, stopped: true };
  }

  async handleManagedEntityJoinResponse(message) {
    if (message.operation !== MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_RESPONSE) {
      return { ok: true, ignored: true };
    }

    const correlationId = message.getCorrelationId();
    const pendingRequest = this.pendingRequestRegistry.get(correlationId);

    if (!pendingRequest ||
      pendingRequest.operationType !== MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_REQUEST) {
      return { ok: true, ignored: true };
    }

    const accepted = message.getPayload()?.accepted;

    if (typeof accepted !== 'boolean') {
      this.pendingRequestRegistry.resolve(correlationId, {
        ok: false,
        error: 'Managed entity join response has an invalid accepted value',
      });
      return { ok: false, error: 'Managed entity join response has an invalid accepted value' };
    }

    this.pendingRequestRegistry.resolve(correlationId, { ok: true, accepted });
    return { ok: true, correlationId, accepted };
  }

  async handleBrokerGroupCheckRequest(message, context = {}) {
    if (message.operation !== MESSAGE_OPERATIONS.BROKER_GROUP_CHECK_REQUEST) {
      return { ok: true, ignored: true };
    }

    const correlationId = message.getCorrelationId();
    const replyTo = message.getPayload()?.replyTo;
    const sessionId = context.sessionId;

    if (typeof correlationId !== 'string' || correlationId.trim() === '') {
      return { ok: false, error: 'Managed entity group check has no correlationId' };
    }

    if (typeof replyTo !== 'string' || replyTo.trim() === '') {
      return { ok: false, error: 'Managed entity group check has no replyTo' };
    }

    if (typeof sessionId !== 'string' || sessionId.trim() === '') {
      return { ok: false, error: 'Managed entity group check has no sessionId' };
    }

    return this.sendThroughSession(
      sessionId,
      replyTo.trim(),
      null,
      MESSAGE_TYPES.RESPONSE,
      MESSAGE_OPERATIONS.BROKER_GROUP_CHECK_RESPONSE,
      false,
      correlationId,
    );
  }

  async handleBrokerStatusReport(message) {
    if (message.operation !== MESSAGE_OPERATIONS.PROBE_STATUS_RESPONSE) {
      return { ok: true, ignored: true };
    }

    const correlationId = message.getCorrelationId();
    const pendingRequest = this.pendingRequestRegistry.get(correlationId);

    if (
      !pendingRequest ||
      pendingRequest.operationType !== MESSAGE_OPERATIONS.PROBE_STATUS_REQUEST
    ) {
      return { ok: true, ignored: true };
    }

    try {
      const payload = message.getPayload();
      assertStatusReport(payload);

      if (payload.brokerId !== pendingRequest.metadata.brokerId) {
        this.pendingRequestRegistry.resolve(correlationId, {
          ok: false,
          error: 'Broker report does not match the requested broker',
        });
        return {
          ok: false,
          error: 'Broker report does not match the requested broker',
        };
      }

      this.pendingRequestRegistry.resolve(correlationId, {
        ok: true,
        report: {
          brokerId: payload.brokerId,
          brokerUrl: pendingRequest.metadata.brokerUrl,
          brokerType: payload.brokerType,
          status: payload.status,
          observedAt: payload.observedAt,
          metrics: payload.metrics ?? {},
          latencyMs: Date.now() - pendingRequest.createdAt,
          sessionReachable: true,
          admissionAccepted: payload.admissionAccepted,
          managedEntityCount: payload.managedEntityCount,
          maximumManagedEntityCount: payload.maximumManagedEntityCount,
          hopCount: payload.hopCount,
          lastKnownHopCount: payload.lastKnownHopCount ?? null,
        },
      });

      return { ok: true, correlationId, brokerId: payload.brokerId };
    } catch (error) {
      this.pendingRequestRegistry.resolve(correlationId, {
        ok: false,
        error: error?.message ?? String(error),
      });
      return { ok: false, error };
    }
  }

  async handleBrokerStatusChanged(message) {
    if (message.operation !== MESSAGE_OPERATIONS.MANAGER_STATUS_CHANGED) {
      return { ok: true, ignored: true };
    }

    const payload = message.getPayload();

    if (
      payload?.brokerId !== this.platformModeManager.getCurrentBrokerNodeId() ||
      payload?.status === OBSERVER_REACHABILITY_STATUS.REACHABLE
    ) {
      return { ok: true, ignored: true };
    }

    this.stopPeriodicPendingSynchronization();
    return this.reevaluate();
  }

  async invalidateSelectedRoute() {
    const selectedOperationalSessionId = this.operationalSessionId;
    const pendingSynchronizationPeriodicResult =
      this.stopPeriodicPendingSynchronization();
    const brokerSelectionResult = this.platformModeManager.selectBroker(null);
    this.operationalSessionId = null;
    const modeTransitionResult = this.platformModeManager.transitionMode(
      MIDDLEWARE_MODES.OFFLINE,
    );
    const periodicResult = this.startPeriodicReevaluation();
    this.notifyReevaluationState();
    let cleanupResult;

    try {
      cleanupResult = selectedOperationalSessionId
        ? await this.sessionManager.closeSession(selectedOperationalSessionId)
        : { ok: true, skipped: true };
    } catch (error) {
      cleanupResult = {
        ok: false,
        error: error?.message ?? String(error),
      };
    }

    const invalidationResult = {
      ok:
        brokerSelectionResult?.ok === true &&
        modeTransitionResult?.ok === true,
      selectedOperationalSessionId,
      brokerSelectionResult,
      modeTransitionResult,
      periodicResult,
      pendingSynchronizationPeriodicResult,
      cleanupResult,
    };
    return invalidationResult;
  }

  async handleUnexpectedSessionClose(event = {}) {
    console.log('[PlatformAdapter] Unexpected session close received', {
      nodeId: this.nodeId,
      event,
      knownBroker: this.knownBrokers.has(event.sessionId),
      reevaluationInFlight: this.reevaluationInFlight,
    });

    if (!this.knownBrokers.has(event.sessionId)) {
      console.log('[PlatformAdapter] Unexpected session close ignored: unknown broker', event);
      return { ok: true, ignored: true };
    }

    if (event.sessionId !== this.operationalSessionId) {
      return {
        ok: true,
        ignored: true,
        reason: 'non-operational-session',
        operationalSessionId: this.operationalSessionId,
      };
    }

    const invalidationResult = await this.invalidateSelectedRoute();
    const reevaluationResult = await this.reevaluate();
    const result = {
      ok:
        invalidationResult.ok === true && reevaluationResult.ok === true,
      invalidationResult,
      reevaluationResult,
    };
    console.log('[PlatformAdapter] Unexpected session close reevaluation result', {
      nodeId: this.nodeId,
      event,
      result,
    });
    return result;
  }

  async shutdown() {
    const periodicResult = this.stopPeriodicReevaluation();
    const pendingSynchronizationPeriodicResult =
      this.stopPeriodicPendingSynchronization();
    const localBrokerDeactivationResult = await this.deactivateLocalBroker();
    this.pendingRequestRegistry.clearAll({
      ok: false,
      error: 'Platform adapter shut down',
    });
    this.sessionManager.setUnexpectedSessionCloseHandler(null);

    const results = await Promise.all(
      this.getKnownBrokers().map(broker => {
        const sessionId = broker.brokerId;
        return this.sessionManager.closeSession(sessionId);
      }),
    );

    return {
      ok:
        localBrokerDeactivationResult.ok === true &&
        results.every(result => result.ok === true),
      periodicResult,
      pendingSynchronizationPeriodicResult,
      localBrokerDeactivationResult,
      results,
    };
  }
}

export { PlatformAdapter };
