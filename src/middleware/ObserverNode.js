import { MESSAGE_OPERATIONS, MESSAGE_TYPES } from './Message.js';
import { SessionManager } from './SessionManager.js';
import { logRuntimeEvent } from './RuntimeLogger.js';

const HEARTBEAT_TOPIC = 'broker/heartbeat';
const OBSERVER_STATUS_RESPONSE_TOPIC = 'observer/status/response';
const OPERATION_INFORMATION_TOPIC = 'operation/info';
const RESYNC_BATCH_REQUEST_TOPIC = 'control/subscriber/resync/batch/request';
const OBSERVER_RECONNECT_PERIOD_MS = 5000;

function normalizeBroker(broker) {
  if (!broker || typeof broker !== 'object') {
    throw new TypeError('broker must be an object');
  }

  if (
    typeof broker.brokerId !== 'string' ||
    broker.brokerId.trim() === ''
  ) {
    throw new TypeError('broker.brokerId must be a non-empty string');
  }

  if (
    typeof broker.brokerUrl !== 'string' ||
    broker.brokerUrl.trim() === ''
  ) {
    throw new TypeError('broker.brokerUrl must be a non-empty string');
  }

  return {
    brokerId: broker.brokerId.trim(),
    brokerUrl: broker.brokerUrl.trim(),
  };
}

class ObserverNode {
  constructor(
    observerId,
    brokers,
    messageManager,
    securityManager = null,
    messageSecurityKeyId = null,
  ) {
    if (typeof observerId !== 'string' || observerId.trim() === '') {
      throw new TypeError('observerId must be a non-empty string');
    }

    if (!Array.isArray(brokers) || brokers.length === 0) {
      throw new TypeError('brokers must be a non-empty array');
    }

    const normalizedBrokers = brokers.map(normalizeBroker);
    const brokerIds = new Set();

    for (const broker of normalizedBrokers) {
      if (brokerIds.has(broker.brokerId)) {
        throw new TypeError('brokers cannot contain duplicate brokerId values');
      }

      brokerIds.add(broker.brokerId);
    }

    this.observerId = observerId.trim();
    this.brokers = normalizedBrokers;
    this.sessionManager = new SessionManager(
      messageManager,
      this.observerId,
      OBSERVER_RECONNECT_PERIOD_MS,
      false,
      securityManager,
      messageSecurityKeyId,
    );
    this.handleBrokerHeartbeat = this.handleBrokerHeartbeat.bind(this);
    this.handleOperationInformation =
      this.handleOperationInformation.bind(this);
    this.handleResyncBatchRequest = this.handleResyncBatchRequest.bind(this);
    this.subscribedSessionIds = new Set();
    this.subscriptionPromises = new Map();
  }

  async initialize() {
    const storageResult = await this.sessionManager.initialize();

    if (!storageResult.ok) {
      return { ok: false, storageResult, results: [] };
    }

    const results = [];

    for (const broker of this.brokers) {
      const sessionId = broker.brokerId;
      const sessionResult = this.sessionManager.createSession(
        broker.brokerUrl,
        sessionId,
        this.observerId,
        () => this.ensureBrokerSubscriptions(sessionId),
      );

      if (!sessionResult.ok) {
        results.push({ brokerId: broker.brokerId, sessionId, ok: false, sessionResult });
        continue;
      }

      const connectionResult = await this.sessionManager.connectSession(
        sessionId,
      );

      if (!connectionResult.ok) {
        results.push({
          brokerId: broker.brokerId,
          sessionId,
          ok: true,
          connectionPending: true,
          sessionResult,
          connectionResult,
        });
        continue;
      }

      const subscriptionResult = await this.ensureBrokerSubscriptions(sessionId);

      results.push({
        brokerId: broker.brokerId,
        sessionId,
        ok: subscriptionResult.ok === true,
        sessionResult,
        connectionResult,
        subscriptionResult,
      });
    }

    return {
      ok: results.every(result => result.ok),
      storageResult,
      results,
    };
  }

  async ensureBrokerSubscriptions(sessionId) {
    if (this.subscribedSessionIds.has(sessionId)) {
      return { ok: true, sessionId, alreadySubscribed: true };
    }

    const pendingSubscription = this.subscriptionPromises.get(sessionId);

    if (pendingSubscription) {
      return pendingSubscription;
    }

    const subscriptionAttempt = this.subscribeToBroker(sessionId)
      .then(result => {
        if (result.ok) {
          this.subscribedSessionIds.add(sessionId);
        }
        return result;
      })
      .finally(() => {
        this.subscriptionPromises.delete(sessionId);
      });

    this.subscriptionPromises.set(sessionId, subscriptionAttempt);
    return subscriptionAttempt;
  }

  async subscribeToBroker(sessionId) {
    const heartbeatSubscriptionResult = await this.sessionManager.subscribe(
      sessionId,
      HEARTBEAT_TOPIC,
      this.handleBrokerHeartbeat,
    );
    const operationSubscriptionResult = await this.sessionManager.subscribe(
      sessionId,
      OPERATION_INFORMATION_TOPIC,
      this.handleOperationInformation,
    );
    const resyncBatchSubscriptionResult = await this.sessionManager.subscribe(
      sessionId,
      RESYNC_BATCH_REQUEST_TOPIC,
      this.handleResyncBatchRequest,
      1,
    );

    return {
      ok:
        heartbeatSubscriptionResult.ok === true &&
        operationSubscriptionResult.ok === true &&
        resyncBatchSubscriptionResult.ok === true,
      sessionId,
      heartbeatSubscriptionResult,
      operationSubscriptionResult,
      resyncBatchSubscriptionResult,
    };
  }

  async handleBrokerHeartbeat(message, context = {}) {
    if (
      message.operation !==
      MESSAGE_OPERATIONS.SUBSCRIBERS_REACHABILITY_CHECK_REQUEST
    ) {
      return { ok: true, ignored: true };
    }

    if (
      typeof message.correlationId !== 'string' ||
      message.correlationId.trim() === ''
    ) {
      return { ok: false, error: 'Heartbeat correlationId is required' };
    }

    if (
      typeof context.sessionId !== 'string' ||
      context.sessionId.trim() === ''
    ) {
      return { ok: false, error: 'Receiving sessionId is required' };
    }

    return this.sessionManager.publish(
      context.sessionId,
      OBSERVER_STATUS_RESPONSE_TOPIC,
      { observerId: this.observerId },
      MESSAGE_TYPES.RESPONSE,
      MESSAGE_OPERATIONS.SUBSCRIBERS_REACHABILITY_CHECK_RESPONSE,
      false,
      message.correlationId,
    );
  }

  async handleOperationInformation(message, context = {}) {
    if (
      typeof context.sessionId !== 'string' ||
      context.sessionId.trim() === ''
    ) {
      return { ok: false, error: 'Receiving sessionId is required' };
    }

    logRuntimeEvent('[' + this.observerId + '] Información operativa recibida', {
      messageId: message.messageId,
      content: message.getPayload(),
    });

    const confirmationNodeId = message.source?.nodeId;

    if (
      typeof confirmationNodeId !== 'string' ||
      confirmationNodeId.trim() === ''
    ) {
      return { ok: false, error: 'Message source nodeId is required' };
    }

    const syncResult = await this.sessionManager.markMessageAsSynced(
      message.messageId,
    );

    if (!syncResult.ok) {
      return { ok: false, syncResult };
    }

    if (context.recoveredFromResyncBatch === true) {
      return {
        ok: true,
        syncResult,
        individualConfirmationSkipped: true,
      };
    }

    const confirmationResult = await this.sessionManager.publish(
      context.sessionId,
      'confirmation/' + confirmationNodeId + '/message',
      { messageId: message.messageId, receivedAt: new Date().toISOString() },
      MESSAGE_TYPES.RESPONSE,
      MESSAGE_OPERATIONS.EVENT_RECEIPT_CONFIRMATION,
      false,
      null,
    );

    return {
      ok: confirmationResult.ok === true,
      syncResult,
      confirmationResult,
    };
  }

  async handleResyncBatchRequest(message, context = {}) {
    if (message.operation !== MESSAGE_OPERATIONS.RESYNC_BATCH_REQUEST) {
      return { ok: true, ignored: true };
    }

    logRuntimeEvent('[' + this.observerId + '] Mensaje recibido de sincronizacion', {
      messageId: message.messageId,
      content: message.getPayload(),
    });

    const correlationId = message.getCorrelationId();
    const payload = message.getPayload();
    const messages = payload?.messages;
    const replyTo = payload?.replyTo;

    if (typeof correlationId !== 'string' || correlationId.trim() === '') {
      return { ok: false, error: 'Resync batch correlationId is required' };
    }

    if (typeof context.sessionId !== 'string' || context.sessionId.trim() === '') {
      return { ok: false, error: 'Receiving sessionId is required' };
    }

    if (typeof replyTo !== 'string' || replyTo.trim() === '') {
      return { ok: false, error: 'Resync batch replyTo is required' };
    }

    if (!Array.isArray(messages)) {
      return { ok: false, error: 'Resync batch messages must be an array' };
    }

    const receivedMessageIds = [];
    const seenMessageIds = new Set();

    for (const messageData of messages) {
      const messageId = messageData?.messageId;

      if (
        typeof messageId === 'string' &&
        messageId.trim() !== '' &&
        !seenMessageIds.has(messageId.trim())
      ) {
        const normalizedMessageId = messageId.trim();
        seenMessageIds.add(normalizedMessageId);
        receivedMessageIds.push(normalizedMessageId);
      }
    }

    const confirmationPromise = this.sessionManager.publish(
      context.sessionId,
      replyTo.trim(),
      { receivedMessageIds },
      MESSAGE_TYPES.RESPONSE,
      MESSAGE_OPERATIONS.RESYNC_BATCH_RESPONSE,
      false,
      correlationId.trim(),
      1,
    ).catch(error => ({ ok: false, error }));

    const processingResults = [];

    for (const messageData of messages) {
      processingResults.push(
        await this.sessionManager.dispatchRecoveredMessageToOperationalHandler(
          context.sessionId,
          OPERATION_INFORMATION_TOPIC,
          messageData,
        ),
      );
    }

    const confirmationResult = await confirmationPromise;

    return {
      ok:
        confirmationResult.ok === true &&
        processingResults.every(result => result.ok === true),
      receivedMessageIds,
      confirmationResult,
      processingResults,
    };
  }

  async shutdown() {
    const results = [];

    for (const broker of this.brokers) {
      const sessionId = broker.brokerId;
      results.push(await this.sessionManager.closeSession(sessionId));
    }

    this.subscribedSessionIds.clear();
    this.subscriptionPromises.clear();

    return {
      ok: results.every(result => result.ok),
      results,
    };
  }
}

export {
  HEARTBEAT_TOPIC,
  OBSERVER_STATUS_RESPONSE_TOPIC,
  OPERATION_INFORMATION_TOPIC,
  RESYNC_BATCH_REQUEST_TOPIC,
  ObserverNode,
};
