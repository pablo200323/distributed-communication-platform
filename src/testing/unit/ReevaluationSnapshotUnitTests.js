import { BROKER_ROLE } from '../../middleware/BrokerContracts.js';
import {
  OBSERVER_REACHABILITY_STATUS,
  ReevaluationSnapshot,
} from '../../middleware/ReevaluationSnapshot.js';

describe('ReevaluationSnapshot unit tests', () => {
  test('places an assigned direct primary before an alternative under equal metrics', () => {
    // 1. Environment configuration
    const assignedPrimaryBrokerId = 'primary-assigned';
    const reports = [
      {
        brokerId: 'primary-alternative',
        brokerUrl: 'ws://primary-alternative.test:9001',
        brokerType: BROKER_ROLE.PRIMARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 500,
      },
      {
        brokerId: 'primary-assigned',
        brokerUrl: 'ws://primary-assigned.test:9001',
        brokerType: BROKER_ROLE.PRIMARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 500,
      },
    ];

    // 2. Behavior under test
    const snapshot = ReevaluationSnapshot.fromReports(
      reports,
      assignedPrimaryBrokerId,
    );

    // 3. Expected result/state
    expect(snapshot.orderedBrokers.map(broker => broker.brokerId)).toEqual([
      'primary-assigned',
      'primary-alternative',
    ]);
  });

  test('places a direct primary before a direct temporary manager', () => {
    // 1. Environment configuration
    const reports = [
      {
        brokerId: 'temporary-direct',
        brokerUrl: 'ws://temporary-direct.test:9001',
        brokerType: BROKER_ROLE.TEMPORARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 500,
      },
      {
        brokerId: 'primary-direct',
        brokerUrl: 'ws://primary-direct.test:9001',
        brokerType: BROKER_ROLE.PRIMARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 500,
      },
    ];

    // 2. Behavior under test
    const snapshot = ReevaluationSnapshot.fromReports(reports, null);

    // 3. Expected result/state
    expect(snapshot.orderedBrokers.map(broker => broker.brokerId)).toEqual([
      'primary-direct',
      'temporary-direct',
    ]);
  });

  test('places a direct temporary manager before a two-hop primary manager', () => {
    // 1. Environment configuration
    const reports = [
      {
        brokerId: 'primary-bridged',
        brokerUrl: 'ws://primary-bridged.test:9001',
        brokerType: BROKER_ROLE.PRIMARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 2,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 500,
      },
      {
        brokerId: 'temporary-direct',
        brokerUrl: 'ws://temporary-direct.test:9001',
        brokerType: BROKER_ROLE.TEMPORARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 500,
      },
    ];

    // 2. Behavior under test
    const snapshot = ReevaluationSnapshot.fromReports(reports, null);

    // 3. Expected result/state
    expect(snapshot.orderedBrokers.map(broker => broker.brokerId)).toEqual([
      'temporary-direct',
      'primary-bridged',
    ]);
  });

  test('places a direct alternative primary before a two-hop assigned primary', () => {
    // 1. Environment configuration
    const assignedPrimaryBrokerId = 'primary-assigned-bridged';
    const reports = [
      {
        brokerId: 'primary-assigned-bridged',
        brokerUrl: 'ws://primary-assigned-bridged.test:9001',
        brokerType: BROKER_ROLE.PRIMARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 2,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 500,
      },
      {
        brokerId: 'primary-alternative-direct',
        brokerUrl: 'ws://primary-alternative-direct.test:9001',
        brokerType: BROKER_ROLE.PRIMARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 500,
      },
    ];

    // 2. Behavior under test
    const snapshot = ReevaluationSnapshot.fromReports(
      reports,
      assignedPrimaryBrokerId,
    );

    // 3. Expected result/state
    expect(snapshot.orderedBrokers.map(broker => broker.brokerId)).toEqual([
      'primary-alternative-direct',
      'primary-assigned-bridged',
    ]);
  });

  test('places a one-hop primary before a two-hop primary under equal metrics', () => {
    // 1. Environment configuration
    const reports = [
      {
        brokerId: 'primary-bridged',
        brokerUrl: 'ws://primary-bridged.test:9001',
        brokerType: BROKER_ROLE.PRIMARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 2,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 500,
      },
      {
        brokerId: 'primary-direct',
        brokerUrl: 'ws://primary-direct.test:9001',
        brokerType: BROKER_ROLE.PRIMARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 500,
      },
    ];

    // 2. Behavior under test
    const snapshot = ReevaluationSnapshot.fromReports(reports, null);

    // 3. Expected result/state
    expect(snapshot.orderedBrokers.map(broker => broker.brokerId)).toEqual([
      'primary-direct',
      'primary-bridged',
    ]);
  });

  test('uses the weighted score inside one direct temporary route tier', () => {
    // 1. Environment configuration
    const reports = [
      {
        brokerId: 'temporary-high-battery',
        brokerUrl: 'ws://temporary-high-battery.test:9001',
        brokerType: BROKER_ROLE.TEMPORARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 100 },
        latencyMs: 3000,
      },
      {
        brokerId: 'temporary-low-latency',
        brokerUrl: 'ws://temporary-low-latency.test:9001',
        brokerType: BROKER_ROLE.TEMPORARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 50 },
        latencyMs: 0,
      },
    ];

    // 2. Behavior under test
    const snapshot = ReevaluationSnapshot.fromReports(reports, null);

    // 3. Expected result/state
    expect(snapshot.orderedBrokers.map(broker => broker.brokerId)).toEqual([
      'temporary-low-latency',
      'temporary-high-battery',
    ]);
  });

  test('uses battery to resolve an equal-score direct temporary tie', () => {
    // 1. Environment configuration
    const reports = [
      {
        brokerId: 'temporary-battery-60-latency-600',
        brokerUrl: 'ws://temporary-battery-60-latency-600.test:9001',
        brokerType: BROKER_ROLE.TEMPORARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 60 },
        latencyMs: 600,
      },
      {
        brokerId: 'temporary-battery-80-latency-1200',
        brokerUrl: 'ws://temporary-battery-80-latency-1200.test:9001',
        brokerType: BROKER_ROLE.TEMPORARY,
        status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
        observedAt: '2026-11-08T10:00:00.000Z',
        sessionReachable: true,
        admissionAccepted: true,
        managedEntityCount: 1,
        maximumManagedEntityCount: 4,
        hopCount: 1,
        lastKnownHopCount: null,
        metrics: { batteryPct: 80 },
        latencyMs: 1200,
      },
    ];

    // 2. Behavior under test
    const snapshot = ReevaluationSnapshot.fromReports(reports, null);

    // 3. Expected result/state
    expect(snapshot.orderedBrokers.map(broker => broker.brokerId)).toEqual([
      'temporary-battery-80-latency-1200',
      'temporary-battery-60-latency-600',
    ]);
  });
});
