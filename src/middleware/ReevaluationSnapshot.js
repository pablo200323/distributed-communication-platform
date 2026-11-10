import { BROKER_ROLE } from './BrokerContracts.js';

export const BROKER_SELECTION_WEIGHTS = Object.freeze({
  BATTERY: 0.5,
  LATENCY: 0.5,
});

export const BROKER_LATENCY_REFERENCE_MS = 3000;

export const OBSERVER_REACHABILITY_STATUS = Object.freeze({
  UNKNOWN: 'UNKNOWN',
  REACHABLE: 'REACHABLE',
  UNREACHABLE: 'UNREACHABLE',
});

export const MAX_OBSERVER_ROUTE_HOP_COUNT = 2;

export const ROUTE_PRIORITY_TIER = Object.freeze({
  DIRECT_PRIMARY: 'DIRECT_PRIMARY',
  DIRECT_TEMPORARY: 'DIRECT_TEMPORARY',
  BRIDGED_PRIMARY: 'BRIDGED_PRIMARY',
  BRIDGED_TEMPORARY: 'BRIDGED_TEMPORARY',
  UNKNOWN_PRIMARY: 'UNKNOWN_PRIMARY',
  UNKNOWN_TEMPORARY: 'UNKNOWN_TEMPORARY',
});

const SCORE_EQUALITY_TOLERANCE = 1e-9;

function assertNonEmptyString(value, fieldName) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new TypeError(`${fieldName} must be a non-empty string`);
  }
}

function assertNullableNonEmptyString(value, fieldName) {
  if (value !== null) {
    assertNonEmptyString(value, fieldName);
  }
}

function normalizeBatteryPct(value) {
  return Number.isFinite(value) && value >= 0 && value <= 100 ? value : 0;
}

function normalizeLatencyMs(value) {
  return Number.isFinite(value) && value >= 0 ? value : null;
}

function normalizeHopCount(value) {
  return Number.isInteger(value) && value > 0 ? value : null;
}

function calculateSelectionScore(batteryPct, latencyMs) {
  const batteryUtility = batteryPct / 100;
  const latencyUtility =
    latencyMs === null
      ? 0
      : 1 - Math.min(latencyMs / BROKER_LATENCY_REFERENCE_MS, 1);

  return (
    batteryUtility * BROKER_SELECTION_WEIGHTS.BATTERY +
    latencyUtility * BROKER_SELECTION_WEIGHTS.LATENCY
  );
}

function normalizeReport(report) {
  if (!report || typeof report !== 'object' || Array.isArray(report)) {
    throw new TypeError('Each broker report must be an object');
  }

  assertNonEmptyString(report.brokerId, 'report.brokerId');

  if (!Object.values(BROKER_ROLE).includes(report.brokerType)) {
    throw new TypeError(`Invalid report.brokerType '${String(report.brokerType)}'`);
  }

  if (!Object.values(OBSERVER_REACHABILITY_STATUS).includes(report.status)) {
    throw new TypeError(`Invalid report.status '${String(report.status)}'`);
  }

  assertNonEmptyString(report.observedAt, 'report.observedAt');

  const batteryPct = normalizeBatteryPct(report.metrics?.batteryPct);
  const latencyMs = normalizeLatencyMs(report.latencyMs);
  const hopCount = normalizeHopCount(report.hopCount);
  const lastKnownHopCount = normalizeHopCount(report.lastKnownHopCount);

  if (report.status === OBSERVER_REACHABILITY_STATUS.REACHABLE && hopCount === null) {
    throw new TypeError(
      'A reachable broker report must define a positive integer hopCount',
    );
  }

  if (typeof report.admissionAccepted !== 'boolean') {
    throw new TypeError('report.admissionAccepted must be a boolean');
  }

  if (!Number.isInteger(report.managedEntityCount) || report.managedEntityCount < 0) {
    throw new TypeError('report.managedEntityCount must be a non-negative integer');
  }

  if (!Number.isInteger(report.maximumManagedEntityCount) ||
    report.maximumManagedEntityCount <= 0) {
    throw new TypeError('report.maximumManagedEntityCount must be a positive integer');
  }

  return {
    brokerId: report.brokerId.trim(),
    brokerUrl:
      typeof report.brokerUrl === 'string' && report.brokerUrl.trim() !== ''
        ? report.brokerUrl.trim()
        : null,
    brokerType: report.brokerType,
    status: report.status,
    observedAt: report.observedAt,
    metrics: Object.freeze({ batteryPct }),
    latencyMs,
    selectionScore: calculateSelectionScore(batteryPct, latencyMs),
    sessionReachable: report.sessionReachable === true,
    admissionAccepted: report.admissionAccepted,
    managedEntityCount: report.managedEntityCount,
    maximumManagedEntityCount: report.maximumManagedEntityCount,
    hopCount,
    lastKnownHopCount,
  };
}

function getRoutePriorityTier(broker, hopCount) {
  if (hopCount === 1) {
    return broker.brokerType === BROKER_ROLE.PRIMARY
      ? ROUTE_PRIORITY_TIER.DIRECT_PRIMARY
      : ROUTE_PRIORITY_TIER.DIRECT_TEMPORARY;
  }

  if (hopCount !== null) {
    return broker.brokerType === BROKER_ROLE.PRIMARY
      ? ROUTE_PRIORITY_TIER.BRIDGED_PRIMARY
      : ROUTE_PRIORITY_TIER.BRIDGED_TEMPORARY;
  }

  return broker.brokerType === BROKER_ROLE.PRIMARY
    ? ROUTE_PRIORITY_TIER.UNKNOWN_PRIMARY
    : ROUTE_PRIORITY_TIER.UNKNOWN_TEMPORARY;
}

function getTierRank(tier) {
  return Object.values(ROUTE_PRIORITY_TIER).indexOf(tier);
}

function getAvailabilityGroup(broker) {
  if (broker.usable) {
    return 0;
  }

  if (
    broker.sessionReachable === true &&
    broker.status === OBSERVER_REACHABILITY_STATUS.UNREACHABLE
  ) {
    return 1;
  }

  return 2;
}

function compareBrokers(firstBroker, secondBroker, assignedPrimaryBrokerId) {
  const availabilityGroupDifference =
    getAvailabilityGroup(firstBroker) - getAvailabilityGroup(secondBroker);

  if (availabilityGroupDifference !== 0) {
    return availabilityGroupDifference;
  }

  const firstHopCount = firstBroker.usable
    ? firstBroker.hopCount
    : firstBroker.lastKnownHopCount;
  const secondHopCount = secondBroker.usable
    ? secondBroker.hopCount
    : secondBroker.lastKnownHopCount;
  const tierDifference =
    getTierRank(
      getRoutePriorityTier(firstBroker, firstHopCount),
    ) -
    getTierRank(
      getRoutePriorityTier(secondBroker, secondHopCount),
    );

  if (tierDifference !== 0) {
    return tierDifference;
  }

  const firstIsAssigned = firstBroker.brokerId === assignedPrimaryBrokerId;
  const secondIsAssigned = secondBroker.brokerId === assignedPrimaryBrokerId;

  if (firstIsAssigned !== secondIsAssigned) {
    return firstIsAssigned ? -1 : 1;
  }

  const scoreDifference = secondBroker.selectionScore - firstBroker.selectionScore;

  if (Math.abs(scoreDifference) > SCORE_EQUALITY_TOLERANCE) {
    return scoreDifference;
  }

  return secondBroker.metrics.batteryPct - firstBroker.metrics.batteryPct;
}

class ReevaluationSnapshot {
  constructor(reports, assignedPrimaryBrokerId, observedAt = new Date().toISOString()) {
    if (!Array.isArray(reports)) {
      throw new TypeError('reports must be an array');
    }

    assertNullableNonEmptyString(
      assignedPrimaryBrokerId,
      'assignedPrimaryBrokerId',
    );
    assertNonEmptyString(observedAt, 'observedAt');

    const knownBrokerIds = new Set();
    const orderedBrokers = reports.map(normalizeReport).map(report => {
      if (knownBrokerIds.has(report.brokerId)) {
        throw new TypeError(`Duplicate report for broker '${report.brokerId}'`);
      }

      knownBrokerIds.add(report.brokerId);

      return Object.freeze({
        ...report,
        usable:
          report.sessionReachable === true &&
          report.status === OBSERVER_REACHABILITY_STATUS.REACHABLE &&
          report.hopCount <= MAX_OBSERVER_ROUTE_HOP_COUNT &&
          report.admissionAccepted === true,
      });
    });

    orderedBrokers.sort((firstBroker, secondBroker) =>
      compareBrokers(firstBroker, secondBroker, assignedPrimaryBrokerId),
    );

    this.reports = Object.freeze([...orderedBrokers]);
    this.orderedBrokers = Object.freeze([...orderedBrokers]);
    this.assignedPrimaryBrokerId = assignedPrimaryBrokerId;
    this.observedAt = observedAt;
    this.preferredConfigurationRecovered = orderedBrokers.some(
      broker =>
        broker.brokerId === assignedPrimaryBrokerId && broker.usable === true,
    );

    Object.freeze(this);
  }

  getUsableBrokers() {
    return this.orderedBrokers.filter(broker => broker.usable === true);
  }

  getUsableBrokersInTier(routePriorityTier) {
    return this.getUsableBrokers().filter(
      broker =>
        getRoutePriorityTier(broker, broker.hopCount) === routePriorityTier,
    );
  }

  getObserverUnreachableBrokersInTier(routePriorityTier) {
    return this.orderedBrokers.filter(
      broker =>
        broker.sessionReachable === true &&
        broker.status === OBSERVER_REACHABILITY_STATUS.UNREACHABLE &&
        broker.admissionAccepted === true &&
        (broker.lastKnownHopCount === null ||
          broker.lastKnownHopCount <= MAX_OBSERVER_ROUTE_HOP_COUNT) &&
        getRoutePriorityTier(broker, broker.lastKnownHopCount) ===
          routePriorityTier,
    );
  }

  getBroker(brokerId) {
    assertNonEmptyString(brokerId, 'brokerId');
    return (
      this.orderedBrokers.find(broker => broker.brokerId === brokerId) ?? null
    );
  }

  static fromReports(reports, assignedPrimaryBrokerId) {
    return new ReevaluationSnapshot(reports, assignedPrimaryBrokerId);
  }
}

export { ReevaluationSnapshot };
