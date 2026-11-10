import { BROKER_PROCESS_STATUS, BROKER_ROLE } from '../../middleware/BrokerContracts.js';
import {
  LOCAL_SYNC_STATE,
  Message,
  MESSAGE_OPERATIONS,
  MESSAGE_TYPES,
} from '../../middleware/Message.js';
import { MIDDLEWARE_MODES } from '../../middleware/PlatformModeManager.js';
import {
  OPERATION_INFORMATION_TOPIC,
  PlatformAdapter,
  RESYNC_BATCH_REQUEST_TOPIC,
  RESYNC_BATCH_STATUS,
} from '../../middleware/PlatformAdapter.js';
import { OBSERVER_REACHABILITY_STATUS } from '../../middleware/ReevaluationSnapshot.js';
import { RUNTIME_PROFILE } from '../../middleware/RuntimeProfiles.js';
import { SessionManager } from '../../middleware/SessionManager.js';
import { TermuxMosquittoManager } from '../../middleware/TermuxMosquittoManager.js';

jest.mock('uuid', () => {
  let counter = 0;
  return {
    v4: jest.fn(() => `test-uuid-${++counter}`),
  };
});

function createMemoryMessageManager() {
  const messages = new Map();

  return {
    async initialize() {
      return { ok: true };
    },
    async storeSentMessage(message) {
      messages.set(message.messageId, message);
      return { ok: true };
    },
    async storeReceivedMessage(message) {
      messages.set(message.messageId, message);
      return { ok: true };
    },
    getMessageEntry(messageId) {
      return messages.get(messageId) ?? null;
    },
    hasMessage(messageId) {
      return messages.has(messageId);
    },
    markMessageAsSynced(messageId) {
      return { ok: messages.has(messageId), messageId };
    },
    async getPendingMessages() {
      return [];
    },
  };
}

function createControlledBrokerSession(
  sessionManager,
  brokerId,
  brokerUrl,
  publishedMessages,
  subscriptions,
  connectError = null,
) {
  return {
    brokerUrl,
    clientId: `client-${brokerId}`,
    status: 'DISCONNECTED',
    connected: false,

    async connect() {
      if (connectError) {
        throw connectError;
      }

      this.connected = true;
      this.status = 'CONNECTED';
    },

    isConnected() {
      return this.connected;
    },

    async subscribe(topicFilter, handler, { qos }) {
      subscriptions.set(topicFilter, { handler, qos });
      return { ok: true, topicFilter, qos };
    },

    async publish(topic, message, { qos, retain }) {
      publishedMessages.push({ topic, message, qos, retain });

      if (
        message.operation === MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_REQUEST
      ) {
        const responseTopic = message.getPayload().replyTo;
        const joinResponse = new Message(
          {
            nodeId: brokerId,
            sessionId: brokerId,
            clientId: `client-${brokerId}`,
          },
          { brokerUrl, topic: responseTopic },
          { accepted: true },
          MESSAGE_TYPES.RESPONSE,
          undefined,
          MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_RESPONSE,
          false,
          message.getCorrelationId(),
        );

        await sessionManager.handleIncomingMessage(
          brokerId,
          responseTopic,
          JSON.stringify(joinResponse.toJSON()),
        );
      }

      return { ok: true, topic };
    },

    async destroy() {
      this.connected = false;
      this.status = 'DISCONNECTED';
      return { ok: true };
    },

    getSuscriptions() {
      return subscriptions;
    },

    matchesTopicFilter(topicFilter, topic) {
      return topicFilter === topic;
    },
  };
}

function createReachablePrimaryReport(brokerId, brokerUrl) {
  return {
    brokerId,
    brokerUrl,
    brokerType: BROKER_ROLE.PRIMARY,
    status: OBSERVER_REACHABILITY_STATUS.REACHABLE,
    observedAt: '2026-11-08T10:00:00.000Z',
    sessionReachable: true,
    admissionAccepted: true,
    managedEntityCount: 1,
    maximumManagedEntityCount: 4,
    hopCount: 1,
    lastKnownHopCount: null,
    metrics: { batteryPct: 80 },
    latencyMs: 50,
  };
}

function createUnknownPrimaryReport(brokerId, brokerUrl) {
  return {
    brokerId,
    brokerUrl,
    brokerType: BROKER_ROLE.PRIMARY,
    status: OBSERVER_REACHABILITY_STATUS.UNKNOWN,
    observedAt: '2026-11-08T10:00:00.000Z',
    sessionReachable: false,
    admissionAccepted: true,
    managedEntityCount: 1,
    maximumManagedEntityCount: 4,
    hopCount: null,
    lastKnownHopCount: null,
    metrics: { batteryPct: 80 },
    latencyMs: null,
  };
}

describe('PlatformAdapter integration tests', () => {
  test('initializes in NORMAL mode through the available assigned primary', async () => {
    // 1. Environment configuration
    const nodeId = 'base-node';
    const assignedPrimaryBrokerId = 'primary-assigned';
    const brokerUrl = 'ws://primary-assigned.test:9001';
    const publishedMessages = [];
    const subscriptions = new Map();
    const sessionManager = new SessionManager(
      createMemoryMessageManager(),
      null,
      0,
      false,
    );
    const adapter = new PlatformAdapter(
      sessionManager,
      nodeId,
      assignedPrimaryBrokerId,
      60000,
      100,
    );

    adapter.setRuntimeProfile(RUNTIME_PROFILE.BASE);
    adapter.loadKnownBrokers({
      brokers: [
        {
          brokerId: assignedPrimaryBrokerId,
          brokerUrl,
          brokerType: BROKER_ROLE.PRIMARY,
        },
      ],
    });

    const controlledSession = createControlledBrokerSession(
      sessionManager,
      assignedPrimaryBrokerId,
      brokerUrl,
      publishedMessages,
      subscriptions,
    );
    sessionManager.sessions.set(assignedPrimaryBrokerId, controlledSession);

    jest.spyOn(adapter, 'collectBrokerReports').mockResolvedValue({
      ok: true,
      reportResults: [
        {
          brokerId: assignedPrimaryBrokerId,
          ok: true,
          sessionReachable: true,
        },
      ],
      reports: [
        createReachablePrimaryReport(assignedPrimaryBrokerId, brokerUrl),
      ],
    });

    try {
      // 2. Behavior under test
      const initializationResult = await adapter.initialize();

      // 3. Expected result/state
      expect(initializationResult.ok).toBe(true);
      expect(controlledSession.isConnected()).toBe(true);
      expect(publishedMessages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            topic: 'control/node/primary-assigned/inbox/primary-assigned',
            message: expect.objectContaining({
              operation: MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_REQUEST,
              requiresResponse: true,
            }),
          }),
        ]),
      );
      expect([...subscriptions.keys()]).toEqual(
        expect.arrayContaining([
          'base-node/response',
          'notify/observer/status',
          'control/node/base-node/inbox/primary-assigned/managed-entity-join',
          'operational/partition/replica',
          'confirmation/base-node/message',
        ]),
      );

      const state = adapter.getReevaluationState();
      expect(state.modeState).toMatchObject({
        currentMode: MIDDLEWARE_MODES.NORMAL,
        assignedPrimaryBrokerId,
        currentBrokerNodeId: assignedPrimaryBrokerId,
        hasPendingResync: false,
      });
      expect(state.currentOperationalSessionId).toBe(assignedPrimaryBrokerId);
      expect(state.reevaluationInFlight).toBe(false);
      expect(state.periodicReevaluationActive).toBe(false);
      expect(state.pendingSynchronizationInFlight).toBe(false);
      expect(state.periodicPendingSynchronizationActive).toBe(false);
      expect(state.managedBridgeState).toBeNull();
      expect(state.localBrokerControlReport).toBeNull();
      expect(adapter.localBrokerActive).toBe(false);
    } finally {
      adapter.stopPeriodicReevaluation();
      adapter.stopPeriodicPendingSynchronization();
    }
  });

  test('uses the next direct primary when the assigned primary session cannot connect', async () => {
    // 1. Environment configuration
    const nodeId = 'base-node';
    const assignedPrimaryBrokerId = 'primary-assigned';
    const alternativePrimaryBrokerId = 'primary-alternative';
    const assignedBrokerUrl = 'ws://primary-assigned.test:9001';
    const alternativeBrokerUrl = 'ws://primary-alternative.test:9001';
    const assignedPublishedMessages = [];
    const alternativePublishedMessages = [];
    const assignedSubscriptions = new Map();
    const alternativeSubscriptions = new Map();
    const sessionManager = new SessionManager(
      createMemoryMessageManager(),
      null,
      0,
      false,
    );
    const adapter = new PlatformAdapter(
      sessionManager,
      nodeId,
      assignedPrimaryBrokerId,
      60000,
      100,
    );

    adapter.setRuntimeProfile(RUNTIME_PROFILE.BASE);
    adapter.loadKnownBrokers({
      brokers: [
        {
          brokerId: assignedPrimaryBrokerId,
          brokerUrl: assignedBrokerUrl,
          brokerType: BROKER_ROLE.PRIMARY,
        },
        {
          brokerId: alternativePrimaryBrokerId,
          brokerUrl: alternativeBrokerUrl,
          brokerType: BROKER_ROLE.PRIMARY,
        },
      ],
    });

    const assignedSession = createControlledBrokerSession(
      sessionManager,
      assignedPrimaryBrokerId,
      assignedBrokerUrl,
      assignedPublishedMessages,
      assignedSubscriptions,
      new Error('Assigned broker connection refused'),
    );
    const alternativeSession = createControlledBrokerSession(
      sessionManager,
      alternativePrimaryBrokerId,
      alternativeBrokerUrl,
      alternativePublishedMessages,
      alternativeSubscriptions,
    );
    sessionManager.sessions.set(assignedPrimaryBrokerId, assignedSession);
    sessionManager.sessions.set(alternativePrimaryBrokerId, alternativeSession);

    jest.spyOn(adapter, 'collectBrokerReports').mockResolvedValue({
      ok: true,
      reportResults: [
        { brokerId: assignedPrimaryBrokerId, ok: true, sessionReachable: true },
        { brokerId: alternativePrimaryBrokerId, ok: true, sessionReachable: true },
      ],
      reports: [
        createReachablePrimaryReport(assignedPrimaryBrokerId, assignedBrokerUrl),
        createReachablePrimaryReport(
          alternativePrimaryBrokerId,
          alternativeBrokerUrl,
        ),
      ],
    });

    try {
      // 2. Behavior under test
      const initializationResult = await adapter.initialize();

      // 3. Expected result/state
      expect(initializationResult.ok).toBe(true);
      expect(assignedSession.isConnected()).toBe(false);
      expect(alternativeSession.isConnected()).toBe(true);
      expect(assignedPublishedMessages).toHaveLength(0);
      expect(alternativePublishedMessages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            message: expect.objectContaining({
              operation: MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_REQUEST,
            }),
          }),
        ]),
      );

      const state = adapter.getReevaluationState();
      expect(state.modeState).toMatchObject({
        currentMode: MIDDLEWARE_MODES.NORMAL,
        assignedPrimaryBrokerId,
        currentBrokerNodeId: alternativePrimaryBrokerId,
        hasPendingResync: false,
      });
      expect(state.currentOperationalSessionId).toBe(alternativePrimaryBrokerId);
      expect(state.reevaluationInFlight).toBe(false);
      expect(state.periodicReevaluationActive).toBe(true);
      expect(state.pendingSynchronizationInFlight).toBe(false);
      expect(state.periodicPendingSynchronizationActive).toBe(false);
      expect(state.managedBridgeState).toBeNull();
      expect(state.localBrokerControlReport).toBeNull();
      expect(adapter.localBrokerActive).toBe(false);
    } finally {
      adapter.stopPeriodicReevaluation();
      adapter.stopPeriodicPendingSynchronization();
    }
  });

  test('activates the temporary local manager when no usable remote route exists', async () => {
    // 1. Environment configuration
    const nodeId = 'temporary-base-node';
    const assignedPrimaryBrokerId = 'primary-assigned';
    const assignedBrokerUrl = 'ws://primary-assigned.test:9001';
    const sessionManager = new SessionManager(
      createMemoryMessageManager(),
      null,
      0,
      false,
    );
    const adapter = new PlatformAdapter(
      sessionManager,
      nodeId,
      assignedPrimaryBrokerId,
      60000,
      100,
    );
    const localBrokerManager = new TermuxMosquittoManager(
      nodeId,
      'ws',
      '0.0.0.0',
      '127.0.0.1',
      9001,
    );
    const controlService = {
      setObserverReachabilityChangeHandler: jest.fn(),
      destroy: jest.fn(async () => ({ ok: true })),
      getStatusReport: jest.fn(() => ({ ok: true, active: true })),
    };

    adapter.setRuntimeProfile(RUNTIME_PROFILE.BASE_TEMPORARY);
    adapter.setLocalBrokerManager(localBrokerManager, BROKER_ROLE.TEMPORARY);
    adapter.loadKnownBrokers({
      brokers: [
        {
          brokerId: assignedPrimaryBrokerId,
          brokerUrl: assignedBrokerUrl,
          brokerType: BROKER_ROLE.PRIMARY,
        },
      ],
    });
    adapter.controlService = controlService;

    const startStatusMonitoring = jest
      .spyOn(localBrokerManager, 'startStatusMonitoring')
      .mockResolvedValue({ ok: true });
    const getBrokerStatus = jest
      .spyOn(localBrokerManager, 'getBrokerStatus')
      .mockResolvedValueOnce({
        ok: true,
        brokerProcessStatus: BROKER_PROCESS_STATUS.STOPPED,
        usable: false,
      })
      .mockResolvedValueOnce({
        ok: true,
        brokerProcessStatus: BROKER_PROCESS_STATUS.RUNNING,
        usable: true,
      });
    const startBroker = jest
      .spyOn(localBrokerManager, 'startBroker')
      .mockResolvedValue({ ok: true });
    jest.spyOn(localBrokerManager, 'getBrokerEndpoint').mockReturnValue({
      ok: true,
      endpoint: { brokerUrl: 'ws://127.0.0.1:9001' },
    });
    const stopBroker = jest
      .spyOn(localBrokerManager, 'stopBroker')
      .mockResolvedValue({ ok: true });
    const stopStatusMonitoring = jest
      .spyOn(localBrokerManager, 'stopStatusMonitoring')
      .mockReturnValue({ ok: true });

    jest.spyOn(adapter, 'collectBrokerReports').mockResolvedValue({
      ok: true,
      reportResults: [
        { brokerId: assignedPrimaryBrokerId, ok: false, sessionReachable: false },
      ],
      reports: [
        createUnknownPrimaryReport(assignedPrimaryBrokerId, assignedBrokerUrl),
      ],
    });

    try {
      // 2. Behavior under test
      const initializationResult = await adapter.initialize();

      // 3. Expected result/state
      expect(initializationResult.ok).toBe(true);
      expect(startStatusMonitoring).toHaveBeenCalled();
      expect(getBrokerStatus).toHaveBeenCalledTimes(2);
      expect(startBroker).toHaveBeenCalledTimes(1);
      expect(stopBroker).not.toHaveBeenCalled();
      expect(stopStatusMonitoring).not.toHaveBeenCalled();
      expect(controlService.setObserverReachabilityChangeHandler).toHaveBeenCalledTimes(1);

      const state = adapter.getReevaluationState();
      expect(state.modeState).toMatchObject({
        currentMode: MIDDLEWARE_MODES.OFFLINE,
        assignedPrimaryBrokerId,
        currentBrokerNodeId: null,
        hasPendingResync: false,
      });
      expect(state.currentOperationalSessionId).toBeNull();
      expect(state.reevaluationInFlight).toBe(false);
      expect(state.periodicReevaluationActive).toBe(true);
      expect(state.pendingSynchronizationInFlight).toBe(false);
      expect(state.periodicPendingSynchronizationActive).toBe(false);
      expect(state.managedBridgeState).toBeNull();
      expect(state.localBrokerControlReport).toEqual({ ok: true, active: true });
      expect(adapter.localBrokerActive).toBe(true);
      expect(adapter.getKnownBrokers().map(broker => broker.brokerId)).toContain(nodeId);
    } finally {
      adapter.stopPeriodicReevaluation();
      adapter.stopPeriodicPendingSynchronization();
      await adapter.deactivateLocalBroker();
    }
  });

  test('deactivates the temporary local manager after a remote primary is adopted', async () => {
    // 1. Environment configuration
    const nodeId = 'temporary-base-node';
    const assignedPrimaryBrokerId = 'primary-assigned';
    const assignedBrokerUrl = 'ws://primary-assigned.test:9001';
    const publishedMessages = [];
    const subscriptions = new Map();
    const sessionManager = new SessionManager(
      createMemoryMessageManager(),
      null,
      0,
      false,
    );
    const adapter = new PlatformAdapter(
      sessionManager,
      nodeId,
      assignedPrimaryBrokerId,
      60000,
      100,
    );
    const localBrokerManager = new TermuxMosquittoManager(
      nodeId,
      'ws',
      '0.0.0.0',
      '127.0.0.1',
      9001,
    );
    const controlService = {
      setObserverReachabilityChangeHandler: jest.fn(),
      destroy: jest.fn(async () => ({ ok: true })),
      getStatusReport: jest.fn(() => ({ ok: true, active: true })),
    };

    adapter.setRuntimeProfile(RUNTIME_PROFILE.BASE_TEMPORARY);
    adapter.setLocalBrokerManager(localBrokerManager, BROKER_ROLE.TEMPORARY);
    adapter.loadKnownBrokers({
      brokers: [
        {
          brokerId: assignedPrimaryBrokerId,
          brokerUrl: assignedBrokerUrl,
          brokerType: BROKER_ROLE.PRIMARY,
        },
      ],
    });
    adapter.controlService = controlService;

    const controlledSession = createControlledBrokerSession(
      sessionManager,
      assignedPrimaryBrokerId,
      assignedBrokerUrl,
      publishedMessages,
      subscriptions,
    );
    sessionManager.sessions.set(assignedPrimaryBrokerId, controlledSession);

    jest
      .spyOn(localBrokerManager, 'startStatusMonitoring')
      .mockResolvedValue({ ok: true });
    jest
      .spyOn(localBrokerManager, 'getBrokerStatus')
      .mockResolvedValueOnce({
        ok: true,
        brokerProcessStatus: BROKER_PROCESS_STATUS.STOPPED,
        usable: false,
      })
      .mockResolvedValueOnce({
        ok: true,
        brokerProcessStatus: BROKER_PROCESS_STATUS.RUNNING,
        usable: true,
      });
    jest.spyOn(localBrokerManager, 'startBroker').mockResolvedValue({ ok: true });
    jest.spyOn(localBrokerManager, 'getBrokerEndpoint').mockReturnValue({
      ok: true,
      endpoint: { brokerUrl: 'ws://127.0.0.1:9001' },
    });
    const stopBroker = jest
      .spyOn(localBrokerManager, 'stopBroker')
      .mockResolvedValue({ ok: true });
    const stopStatusMonitoring = jest
      .spyOn(localBrokerManager, 'stopStatusMonitoring')
      .mockReturnValue({ ok: true });

    jest
      .spyOn(adapter, 'collectBrokerReports')
      .mockResolvedValueOnce({
        ok: true,
        reportResults: [
          { brokerId: assignedPrimaryBrokerId, ok: false, sessionReachable: false },
        ],
        reports: [
          createUnknownPrimaryReport(assignedPrimaryBrokerId, assignedBrokerUrl),
        ],
      })
      .mockResolvedValueOnce({
        ok: true,
        reportResults: [
          { brokerId: assignedPrimaryBrokerId, ok: true, sessionReachable: true },
        ],
        reports: [
          createReachablePrimaryReport(assignedPrimaryBrokerId, assignedBrokerUrl),
        ],
      });

    try {
      // 2. Behavior under test
      const initializationResult = await adapter.initialize();
      const reevaluationResult = await adapter.reevaluate();

      // 3. Expected result/state
      expect(initializationResult.ok).toBe(true);
      expect(reevaluationResult.ok).toBe(true);
      expect(controlledSession.isConnected()).toBe(true);
      expect(publishedMessages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            message: expect.objectContaining({
              operation: MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_REQUEST,
            }),
          }),
        ]),
      );
      expect(stopBroker).toHaveBeenCalledTimes(1);
      expect(stopStatusMonitoring).toHaveBeenCalledTimes(1);
      expect(controlService.destroy).toHaveBeenCalledTimes(1);

      const state = adapter.getReevaluationState();
      expect(state.modeState).toMatchObject({
        currentMode: MIDDLEWARE_MODES.NORMAL,
        assignedPrimaryBrokerId,
        currentBrokerNodeId: assignedPrimaryBrokerId,
        hasPendingResync: false,
      });
      expect(state.currentOperationalSessionId).toBe(assignedPrimaryBrokerId);
      expect(state.reevaluationInFlight).toBe(false);
      expect(state.periodicReevaluationActive).toBe(false);
      expect(state.pendingSynchronizationInFlight).toBe(false);
      expect(state.periodicPendingSynchronizationActive).toBe(false);
      expect(state.managedBridgeState).toBeNull();
      expect(state.localBrokerControlReport).toBeNull();
      expect(adapter.localBrokerActive).toBe(false);
      expect(adapter.getKnownBrokers().map(broker => broker.brokerId)).not.toContain(nodeId);
    } finally {
      adapter.stopPeriodicReevaluation();
      adapter.stopPeriodicPendingSynchronization();
    }
  });

  test('keeps the primary local manager active after adopting a remote primary', async () => {
    // 1. Environment configuration
    const nodeId = 'primary-local-node';
    const remotePrimaryBrokerId = 'primary-remote';
    const remoteBrokerUrl = 'ws://primary-remote.test:9001';
    const publishedMessages = [];
    const subscriptions = new Map();
    const sessionManager = new SessionManager(
      createMemoryMessageManager(),
      null,
      0,
      false,
    );
    const adapter = new PlatformAdapter(
      sessionManager,
      nodeId,
      null,
      60000,
      100,
    );
    const localBrokerManager = new TermuxMosquittoManager(
      nodeId,
      'ws',
      '0.0.0.0',
      '127.0.0.1',
      9001,
    );
    const controlService = {
      setObserverReachabilityChangeHandler: jest.fn(),
      destroy: jest.fn(async () => ({ ok: true })),
      getStatusReport: jest.fn(() => ({ ok: true, active: true })),
    };

    adapter.setRuntimeProfile(RUNTIME_PROFILE.PRIMARY_BROKER);
    adapter.setLocalBrokerManager(localBrokerManager, BROKER_ROLE.PRIMARY);
    adapter.loadKnownBrokers({
      brokers: [
        {
          brokerId: remotePrimaryBrokerId,
          brokerUrl: remoteBrokerUrl,
          brokerType: BROKER_ROLE.PRIMARY,
        },
      ],
    });
    adapter.controlService = controlService;

    const controlledSession = createControlledBrokerSession(
      sessionManager,
      remotePrimaryBrokerId,
      remoteBrokerUrl,
      publishedMessages,
      subscriptions,
    );
    sessionManager.sessions.set(remotePrimaryBrokerId, controlledSession);

    jest
      .spyOn(localBrokerManager, 'startStatusMonitoring')
      .mockResolvedValue({ ok: true });
    jest
      .spyOn(localBrokerManager, 'getBrokerStatus')
      .mockResolvedValueOnce({
        ok: true,
        brokerProcessStatus: BROKER_PROCESS_STATUS.STOPPED,
        usable: false,
      })
      .mockResolvedValueOnce({
        ok: true,
        brokerProcessStatus: BROKER_PROCESS_STATUS.RUNNING,
        usable: true,
      });
    const startBroker = jest
      .spyOn(localBrokerManager, 'startBroker')
      .mockResolvedValue({ ok: true });
    jest.spyOn(localBrokerManager, 'getBrokerEndpoint').mockReturnValue({
      ok: true,
      endpoint: { brokerUrl: 'ws://127.0.0.1:9001' },
    });
    const stopBroker = jest
      .spyOn(localBrokerManager, 'stopBroker')
      .mockResolvedValue({ ok: true });
    const stopStatusMonitoring = jest
      .spyOn(localBrokerManager, 'stopStatusMonitoring')
      .mockReturnValue({ ok: true });

    jest.spyOn(adapter, 'collectBrokerReports').mockResolvedValue({
      ok: true,
      reportResults: [
        { brokerId: remotePrimaryBrokerId, ok: true, sessionReachable: true },
      ],
      reports: [
        createReachablePrimaryReport(remotePrimaryBrokerId, remoteBrokerUrl),
      ],
    });

    try {
      // 2. Behavior under test
      const initializationResult = await adapter.initialize();

      // 3. Expected result/state
      expect(initializationResult.ok).toBe(true);
      expect(controlledSession.isConnected()).toBe(true);
      expect(startBroker).toHaveBeenCalledTimes(1);
      expect(stopBroker).not.toHaveBeenCalled();
      expect(stopStatusMonitoring).not.toHaveBeenCalled();
      expect(controlService.destroy).not.toHaveBeenCalled();

      const state = adapter.getReevaluationState();
      expect(state.modeState).toMatchObject({
        currentMode: MIDDLEWARE_MODES.NORMAL,
        assignedPrimaryBrokerId: nodeId,
        currentBrokerNodeId: remotePrimaryBrokerId,
        hasPendingResync: false,
      });
      expect(state.currentOperationalSessionId).toBe(remotePrimaryBrokerId);
      expect(state.reevaluationInFlight).toBe(false);
      expect(state.periodicReevaluationActive).toBe(true);
      expect(state.pendingSynchronizationInFlight).toBe(false);
      expect(state.periodicPendingSynchronizationActive).toBe(false);
      expect(state.managedBridgeState).toBeNull();
      expect(state.localBrokerControlReport).toEqual({ ok: true, active: true });
      expect(adapter.localBrokerActive).toBe(true);
      expect(adapter.getKnownBrokers().map(broker => broker.brokerId)).toContain(nodeId);
    } finally {
      adapter.stopPeriodicReevaluation();
      adapter.stopPeriodicPendingSynchronization();
      await adapter.deactivateLocalBroker();
    }
  });

  test('activates the managed bridge when the selected upstream has one hop', async () => {
    // 1. Environment configuration
    const nodeId = 'temporary-bridge-node';
    const assignedPrimaryBrokerId = 'primary-assigned';
    const assignedBrokerUrl = 'ws://primary-assigned.test:9001';
    const publishedMessages = [];
    const subscriptions = new Map();
    const sessionManager = new SessionManager(
      createMemoryMessageManager(),
      null,
      0,
      false,
    );
    const adapter = new PlatformAdapter(
      sessionManager,
      nodeId,
      assignedPrimaryBrokerId,
      60000,
      100,
      true,
    );
    const localBrokerManager = new TermuxMosquittoManager(
      nodeId,
      'ws',
      '0.0.0.0',
      '127.0.0.1',
      9001,
    );
    const controlService = {
      setObserverReachabilityChangeHandler: jest.fn(),
      setBridgeUpstreamHopCount: jest.fn(async () => ({ ok: true })),
      invalidateObserverReachability: jest.fn(async () => ({ ok: true })),
      checkObserverReachability: jest.fn(async () => ({ ok: true })),
      destroy: jest.fn(async () => ({ ok: true })),
      getStatusReport: jest.fn(() => ({ ok: true, active: true })),
    };

    adapter.setRuntimeProfile(RUNTIME_PROFILE.BASE_TEMPORARY);
    adapter.setLocalBrokerManager(localBrokerManager, BROKER_ROLE.TEMPORARY);
    adapter.loadKnownBrokers({
      brokers: [
        {
          brokerId: assignedPrimaryBrokerId,
          brokerUrl: assignedBrokerUrl,
          brokerType: BROKER_ROLE.PRIMARY,
          ip: '10.0.0.20',
          bridgePort: 1883,
        },
      ],
    });
    adapter.controlService = controlService;

    const controlledSession = createControlledBrokerSession(
      sessionManager,
      assignedPrimaryBrokerId,
      assignedBrokerUrl,
      publishedMessages,
      subscriptions,
    );
    sessionManager.sessions.set(assignedPrimaryBrokerId, controlledSession);

    const applyBridgeConfiguration = jest.spyOn(
      localBrokerManager,
      'applyBridgeConfiguration',
    );
    jest
      .spyOn(localBrokerManager, 'startStatusMonitoring')
      .mockResolvedValue({ ok: true });
    jest
      .spyOn(localBrokerManager, 'getBrokerStatus')
      .mockResolvedValue({
        ok: true,
        brokerProcessStatus: BROKER_PROCESS_STATUS.RUNNING,
        usable: true,
      });
    const startBroker = jest
      .spyOn(localBrokerManager, 'startBroker')
      .mockResolvedValue({ ok: true });
    jest.spyOn(localBrokerManager, 'getBrokerEndpoint').mockReturnValue({
      ok: true,
      endpoint: { brokerUrl: 'ws://127.0.0.1:9001' },
    });
    const updateRunningBrokerConfiguration = jest
      .spyOn(localBrokerManager, 'updateRunningBrokerConfiguration')
      .mockResolvedValue({ ok: true });
    jest.spyOn(localBrokerManager, 'stopBroker').mockResolvedValue({ ok: true });
    jest.spyOn(localBrokerManager, 'stopStatusMonitoring').mockReturnValue({
      ok: true,
    });

    jest.spyOn(adapter, 'collectBrokerReports').mockResolvedValue({
      ok: true,
      reportResults: [
        { brokerId: assignedPrimaryBrokerId, ok: true, sessionReachable: true },
      ],
      reports: [
        createReachablePrimaryReport(assignedPrimaryBrokerId, assignedBrokerUrl),
      ],
    });

    try {
      // 2. Behavior under test
      const initializationResult = await adapter.initialize();

      // 3. Expected result/state
      expect(initializationResult.ok).toBe(true);
      expect(controlledSession.isConnected()).toBe(true);
      expect(startBroker).not.toHaveBeenCalled();
      expect(applyBridgeConfiguration).toHaveBeenCalledWith(
        assignedPrimaryBrokerId,
        'mqtt://10.0.0.20:1883',
        expect.any(Array),
      );
      expect(updateRunningBrokerConfiguration).toHaveBeenCalledTimes(1);
      expect(localBrokerManager.getBridgeConnections()).toEqual({
        [assignedPrimaryBrokerId]: {
          brokerUrl: 'mqtt://10.0.0.20:1883',
          bridgeRules: expect.any(Array),
        },
      });
      expect(
        localBrokerManager.getBridgeConnections()[assignedPrimaryBrokerId]
          .bridgeRules,
      ).toHaveLength(7);
      expect(controlService.setBridgeUpstreamHopCount).toHaveBeenCalledWith(1);
      expect(controlService.invalidateObserverReachability).toHaveBeenCalledTimes(1);
      expect(controlService.checkObserverReachability).toHaveBeenCalledTimes(1);

      const state = adapter.getReevaluationState();
      expect(state.modeState).toMatchObject({
        currentMode: MIDDLEWARE_MODES.NORMAL,
        assignedPrimaryBrokerId,
        currentBrokerNodeId: assignedPrimaryBrokerId,
        hasPendingResync: false,
      });
      expect(state.currentOperationalSessionId).toBe(assignedPrimaryBrokerId);
      expect(state.reevaluationInFlight).toBe(false);
      expect(state.periodicReevaluationActive).toBe(false);
      expect(state.pendingSynchronizationInFlight).toBe(false);
      expect(state.periodicPendingSynchronizationActive).toBe(false);
      expect(state.managedBridgeState).toMatchObject({
        configured: true,
        upstreamBrokerId: assignedPrimaryBrokerId,
        upstreamHopCount: 1,
        upstreamEndpoint: 'mqtt://10.0.0.20:1883',
      });
      expect(state.localBrokerControlReport).toEqual({ ok: true, active: true });
      expect(adapter.localBrokerActive).toBe(true);
    } finally {
      adapter.stopPeriodicReevaluation();
      adapter.stopPeriodicPendingSynchronization();
      await adapter.deactivateLocalBroker();
    }
  });

  test('does not activate the managed bridge when the selected upstream has two hops', async () => {
    // 1. Environment configuration
    const nodeId = 'temporary-bridge-node';
    const assignedPrimaryBrokerId = 'primary-assigned';
    const assignedBrokerUrl = 'ws://primary-assigned.test:9001';
    const publishedMessages = [];
    const subscriptions = new Map();
    const sessionManager = new SessionManager(
      createMemoryMessageManager(),
      null,
      0,
      false,
    );
    const adapter = new PlatformAdapter(
      sessionManager,
      nodeId,
      assignedPrimaryBrokerId,
      60000,
      100,
      true,
    );
    const localBrokerManager = new TermuxMosquittoManager(
      nodeId,
      'ws',
      '0.0.0.0',
      '127.0.0.1',
      9001,
    );
    const controlService = {
      setObserverReachabilityChangeHandler: jest.fn(),
      setBridgeUpstreamHopCount: jest.fn(async () => ({ ok: true })),
      invalidateObserverReachability: jest.fn(async () => ({ ok: true })),
      checkObserverReachability: jest.fn(async () => ({ ok: true })),
      destroy: jest.fn(async () => ({ ok: true })),
      getStatusReport: jest.fn(() => ({ ok: true, active: true })),
    };

    adapter.setRuntimeProfile(RUNTIME_PROFILE.BASE_TEMPORARY);
    adapter.setLocalBrokerManager(localBrokerManager, BROKER_ROLE.TEMPORARY);
    adapter.loadKnownBrokers({
      brokers: [
        {
          brokerId: assignedPrimaryBrokerId,
          brokerUrl: assignedBrokerUrl,
          brokerType: BROKER_ROLE.PRIMARY,
          ip: '10.0.0.20',
          bridgePort: 1883,
        },
      ],
    });
    adapter.controlService = controlService;

    const controlledSession = createControlledBrokerSession(
      sessionManager,
      assignedPrimaryBrokerId,
      assignedBrokerUrl,
      publishedMessages,
      subscriptions,
    );
    sessionManager.sessions.set(assignedPrimaryBrokerId, controlledSession);

    const applyBridgeConfiguration = jest.spyOn(
      localBrokerManager,
      'applyBridgeConfiguration',
    );
    jest
      .spyOn(localBrokerManager, 'startStatusMonitoring')
      .mockResolvedValue({ ok: true });
    jest
      .spyOn(localBrokerManager, 'getBrokerStatus')
      .mockResolvedValueOnce({
        ok: true,
        brokerProcessStatus: BROKER_PROCESS_STATUS.STOPPED,
        usable: false,
      })
      .mockResolvedValueOnce({
        ok: true,
        brokerProcessStatus: BROKER_PROCESS_STATUS.RUNNING,
        usable: true,
      });
    const startBroker = jest
      .spyOn(localBrokerManager, 'startBroker')
      .mockResolvedValue({ ok: true });
    jest.spyOn(localBrokerManager, 'getBrokerEndpoint').mockReturnValue({
      ok: true,
      endpoint: { brokerUrl: 'ws://127.0.0.1:9001' },
    });
    const updateRunningBrokerConfiguration = jest
      .spyOn(localBrokerManager, 'updateRunningBrokerConfiguration')
      .mockResolvedValue({ ok: true });
    jest.spyOn(localBrokerManager, 'stopBroker').mockResolvedValue({ ok: true });
    jest.spyOn(localBrokerManager, 'stopStatusMonitoring').mockReturnValue({
      ok: true,
    });

    jest.spyOn(adapter, 'collectBrokerReports').mockResolvedValue({
      ok: true,
      reportResults: [
        { brokerId: assignedPrimaryBrokerId, ok: true, sessionReachable: true },
      ],
      reports: [
        {
          ...createReachablePrimaryReport(
            assignedPrimaryBrokerId,
            assignedBrokerUrl,
          ),
          hopCount: 2,
        },
      ],
    });

    try {
      // 2. Behavior under test
      const initializationResult = await adapter.initialize();

      // 3. Expected result/state
      expect(initializationResult.ok).toBe(true);
      expect(controlledSession.isConnected()).toBe(true);
      expect(startBroker).toHaveBeenCalledTimes(1);
      expect(applyBridgeConfiguration).not.toHaveBeenCalled();
      expect(updateRunningBrokerConfiguration).not.toHaveBeenCalled();
      expect(localBrokerManager.getBridgeConnections()).toEqual({});
      expect(controlService.setBridgeUpstreamHopCount).not.toHaveBeenCalled();
      expect(controlService.invalidateObserverReachability).not.toHaveBeenCalled();
      expect(controlService.checkObserverReachability).not.toHaveBeenCalled();

      const state = adapter.getReevaluationState();
      expect(state.modeState).toMatchObject({
        currentMode: MIDDLEWARE_MODES.FALLBACK,
        assignedPrimaryBrokerId,
        currentBrokerNodeId: assignedPrimaryBrokerId,
        hasPendingResync: false,
      });
      expect(state.currentOperationalSessionId).toBe(assignedPrimaryBrokerId);
      expect(state.reevaluationInFlight).toBe(false);
      expect(state.periodicReevaluationActive).toBe(true);
      expect(state.pendingSynchronizationInFlight).toBe(false);
      expect(state.periodicPendingSynchronizationActive).toBe(false);
      expect(state.managedBridgeState).toBeNull();
      expect(state.localBrokerControlReport).toEqual({ ok: true, active: true });
      expect(adapter.localBrokerActive).toBe(true);
    } finally {
      adapter.stopPeriodicReevaluation();
      adapter.stopPeriodicPendingSynchronization();
      await adapter.deactivateLocalBroker();
    }
  });

  test('resynchronizes pending operational messages after selecting a global route', async () => {
    // 1. Environment configuration
    const nodeId = 'base-node';
    const assignedPrimaryBrokerId = 'primary-assigned';
    const assignedBrokerUrl = 'ws://primary-assigned.test:9001';
    const storedMessages = new Map();
    const publishedMessages = [];
    const subscriptions = new Map();
    const reevaluationStates = [];
    const messageManager = {
      async initialize() {
        return { ok: true };
      },
      async storeSentMessage(message) {
        storedMessages.set(message.messageId, { origin: 'SENT', message });
        return { ok: true };
      },
      async storeReceivedMessage(message) {
        storedMessages.set(message.messageId, { origin: 'RECEIVED', message });
        return { ok: true };
      },
      getMessageEntry(messageId) {
        return storedMessages.get(messageId) ?? null;
      },
      hasMessage(messageId) {
        return storedMessages.has(messageId);
      },
      markMessageAsSynced: jest.fn(messageId => {
        const entry = storedMessages.get(messageId);
        if (!entry) {
          return { ok: false, messageId };
        }

        entry.message.markAsSynced();
        return { ok: true, messageId };
      }),
      async getPendingMessages() {
        return Array.from(storedMessages.values()).filter(
          entry => entry.message.syncState === LOCAL_SYNC_STATE.PENDING_GLOBAL_SYNC,
        );
      },
    };
    const sessionManager = new SessionManager(messageManager, null, 0, false);
    const adapter = new PlatformAdapter(
      sessionManager,
      nodeId,
      assignedPrimaryBrokerId,
      60000,
      100,
    );

    adapter.setRuntimeProfile(RUNTIME_PROFILE.BASE);
    adapter.loadKnownBrokers({
      brokers: [
        {
          brokerId: assignedPrimaryBrokerId,
          brokerUrl: assignedBrokerUrl,
          brokerType: BROKER_ROLE.PRIMARY,
        },
      ],
    });
    adapter.setReevaluationStateHandler(state => reevaluationStates.push(state));

    const persistedMessageResult = await sessionManager.persistOperationalMessage(
      OPERATION_INFORMATION_TOPIC,
      { event: 'created-while-offline' },
    );
    const pendingMessageId = persistedMessageResult.message.messageId;

    const controlledSession = createControlledBrokerSession(
      sessionManager,
      assignedPrimaryBrokerId,
      assignedBrokerUrl,
      publishedMessages,
      subscriptions,
    );
    const publishThroughMqttBoundary = controlledSession.publish.bind(
      controlledSession,
    );
    controlledSession.publish = async (topic, message, publishOptions) => {
      const publishResult = await publishThroughMqttBoundary(
        topic,
        message,
        publishOptions,
      );

      if (message.operation === MESSAGE_OPERATIONS.RESYNC_BATCH_REQUEST) {
        const responseTopic = message.getPayload().replyTo;
        const receivedMessageIds = message
          .getPayload()
          .messages.map(pendingMessage => pendingMessage.messageId);
        const batchResponse = new Message(
          {
            nodeId: assignedPrimaryBrokerId,
            sessionId: assignedPrimaryBrokerId,
            clientId: `client-${assignedPrimaryBrokerId}`,
          },
          { brokerUrl: assignedBrokerUrl, topic: responseTopic },
          { receivedMessageIds },
          MESSAGE_TYPES.RESPONSE,
          undefined,
          MESSAGE_OPERATIONS.RESYNC_BATCH_RESPONSE,
          false,
          message.getCorrelationId(),
        );

        await sessionManager.handleIncomingMessage(
          assignedPrimaryBrokerId,
          responseTopic,
          JSON.stringify(batchResponse.toJSON()),
        );
      }

      return publishResult;
    };
    sessionManager.sessions.set(assignedPrimaryBrokerId, controlledSession);

    jest.spyOn(adapter, 'collectBrokerReports').mockResolvedValue({
      ok: true,
      reportResults: [
        { brokerId: assignedPrimaryBrokerId, ok: true, sessionReachable: true },
      ],
      reports: [
        createReachablePrimaryReport(assignedPrimaryBrokerId, assignedBrokerUrl),
      ],
    });

    try {
      // 2. Behavior under test
      const initializationResult = await adapter.initialize();

      // 3. Expected result/state
      expect(initializationResult.ok).toBe(true);
      expect(persistedMessageResult.ok).toBe(true);
      expect(publishedMessages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            topic: RESYNC_BATCH_REQUEST_TOPIC,
            message: expect.objectContaining({
              operation: MESSAGE_OPERATIONS.RESYNC_BATCH_REQUEST,
              requiresResponse: true,
              payload: expect.objectContaining({
                messages: [expect.objectContaining({ messageId: pendingMessageId })],
              }),
            }),
          }),
        ]),
      );
      expect(messageManager.markMessageAsSynced).toHaveBeenCalledWith(
        pendingMessageId,
      );
      expect(storedMessages.get(pendingMessageId).message.syncState).toBe(
        LOCAL_SYNC_STATE.SYNCED,
      );
      expect(await messageManager.getPendingMessages()).toEqual([]);
      expect(
        reevaluationStates.some(
          state =>
            state.modeState.currentMode === MIDDLEWARE_MODES.RESYNC &&
            state.modeState.hasPendingResync === true,
        ),
      ).toBe(true);

      const state = adapter.getReevaluationState();
      expect(state.modeState).toMatchObject({
        currentMode: MIDDLEWARE_MODES.NORMAL,
        assignedPrimaryBrokerId,
        currentBrokerNodeId: assignedPrimaryBrokerId,
        hasPendingResync: false,
      });
      expect(state.currentOperationalSessionId).toBe(assignedPrimaryBrokerId);
      expect(state.reevaluationInFlight).toBe(false);
      expect(state.pendingSynchronizationInFlight).toBe(false);
      expect(state.periodicPendingSynchronizationActive).toBe(false);
      expect(state.currentPendingSynchronizationBatch).toBeNull();
      expect(state.lastPendingSynchronizationResult).toMatchObject({
        ok: true,
        remainingPendingMessageCount: 0,
        processingResult: {
          ok: true,
          attemptedMessageCount: 1,
          batchResults: [
            expect.objectContaining({
              status: RESYNC_BATCH_STATUS.COMPLETED,
              requestedMessageIds: [pendingMessageId],
              receivedMessageIds: [pendingMessageId],
            }),
          ],
        },
      });
      expect(state.periodicReevaluationActive).toBe(false);
      expect(state.managedBridgeState).toBeNull();
      expect(state.localBrokerControlReport).toBeNull();
      expect(adapter.localBrokerActive).toBe(false);
    } finally {
      adapter.stopPeriodicReevaluation();
      adapter.stopPeriodicPendingSynchronization();
    }
  });
});
