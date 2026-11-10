import { BrokerSession } from './BrokerSession.js';
import {
  isOperationalMessage,
  LOCAL_SYNC_STATE,
  Message,
  MESSAGE_TYPES,
} from './Message.js';
import { EncryptedMessage } from './EncryptedMessage.js';
import { assertMessageSecurityManager } from './MessageSecurityContract.js';

class SessionNotFoundError extends Error {
  constructor(sessionId) {
    super(`Session '${String(sessionId)}' not found`);
    this.name = 'SessionNotFoundError';
    this.code = 'SESSION_NOT_FOUND';
  }
}

class SessionManager {
  constructor(
    messageManager,
    localNodeId = null,
    reconnectPeriodMs = 0,
    logConnectionEvents = true,
    securityManager = null,
    messageSecurityKeyId = null,
  ) {
    if (
      !messageManager ||
      typeof messageManager.initialize !== 'function' ||
      typeof messageManager.storeSentMessage !== 'function' ||
      typeof messageManager.storeReceivedMessage !== 'function' ||
      typeof messageManager.getMessageEntry !== 'function' ||
      typeof messageManager.hasMessage !== 'function' ||
      typeof messageManager.markMessageAsSynced !== 'function'
    ) {
      throw new TypeError('messageManager must support session message persistence');
    }

    if (
      !Number.isInteger(reconnectPeriodMs) ||
      reconnectPeriodMs < 0
    ) {
      throw new TypeError('reconnectPeriodMs must be a non-negative integer');
    }

    if (securityManager !== null) {
      assertMessageSecurityManager(securityManager);
    }

    if (messageSecurityKeyId !== null &&
      (typeof messageSecurityKeyId !== 'string' || messageSecurityKeyId.trim() === '')) {
      throw new TypeError('messageSecurityKeyId must be a non-empty string or null');
    }

    this.sessions = new Map();
    this.messageManager = messageManager;
    this.localNodeId = null;
    this.reconnectPeriodMs = reconnectPeriodMs;
    this.logConnectionEvents = logConnectionEvents;
    this.securityManager = securityManager;
    this.messageSecurityKeyId = messageSecurityKeyId?.trim() ?? null;
    this.unexpectedSessionCloseHandler = null;

    if (localNodeId !== null) {
      this.setLocalNodeId(localNodeId);
    }
  }

  async initialize() {
    try {
      return await this.messageManager.initialize();
    } catch (error) {
      return { ok: false, error };
    }
  }

  async getStoredMessages() {
    return this.messageManager.getStoredMessages();
  }

  async getPendingMessages() {
    return this.messageManager.getPendingMessages();
  }

  async getSyncedMessages() {
    return this.messageManager.getSyncedMessages();
  }

  clearMessages() {
    return this.messageManager.clearMessages();
  }

  markMessageAsSynced(messageId) {
    return this.messageManager.markMessageAsSynced(messageId);
  }

  getMessageEntry(messageId) {
    return this.messageManager.getMessageEntry(messageId);
  }

  setSecurityManager(securityManager, messageSecurityKeyId) {
    assertMessageSecurityManager(securityManager);

    if (typeof messageSecurityKeyId !== 'string' || messageSecurityKeyId.trim() === '') {
      throw new TypeError('messageSecurityKeyId must be a non-empty string');
    }

    this.securityManager = securityManager;
    this.messageSecurityKeyId = messageSecurityKeyId.trim();
    return { ok: true, enabled: true, messageSecurityKeyId: this.messageSecurityKeyId };
  }

  assertMessageSecurityEnabled() {
    if (!this.securityManager || !this.messageSecurityKeyId) {
      throw new Error('Message security is required for encrypted message lifecycle');
    }
  }

  isMessageSecurityEnabled() {
    return this.securityManager !== null && this.messageSecurityKeyId !== null;
  }

  setLocalNodeId(localNodeId) {
    if (typeof localNodeId !== 'string' || localNodeId.trim() === '') {
      throw new TypeError('localNodeId must be a non-empty string');
    }

    this.localNodeId = localNodeId.trim();
  }

  setUnexpectedSessionCloseHandler(handler) {
    if (handler !== null && typeof handler !== 'function') {
      throw new TypeError('handler must be a function or null');
    }

    this.unexpectedSessionCloseHandler = handler;
    return { ok: true, listening: handler !== null };
  }

  getSessionState(sessionId) {
    const session = this.sessions.get(sessionId);

    if (!session) {
      return null;
    }

    return {
      sessionId,
      brokerUrl: session.brokerUrl,
      clientId: session.clientId,
      connected: session.isConnected(),
      status: session.status,
    };
  }

  async handleUnexpectedSessionClose(event) {
    if (this.logConnectionEvents) {
      console.log('[SessionManager] Unexpected session close received', event);
    }

    if (typeof this.unexpectedSessionCloseHandler !== 'function') {
      if (this.logConnectionEvents) {
        console.log('[SessionManager] Unexpected session close ignored: no handler', event);
      }
      return { ok: true, ignored: true };
    }

    const result = await this.unexpectedSessionCloseHandler(event);
    if (this.logConnectionEvents) {
      console.log('[SessionManager] Unexpected session close handler completed', {
        event,
        result,
      });
    }
    return result;
  }

  createMessage(
    sessionId,
    topic,
    payload,
    type,
    operation,
    requiresResponse,
    correlationId,
  ) {
    if (!this.localNodeId) {
      throw new Error('SessionManager localNodeId must be set before publishing');
    }

    const session = this.sessions.get(sessionId);
    const source = {
      nodeId: this.localNodeId,
      sessionId,
      clientId: session?.clientId ?? null,
    };
    const target = {
      brokerUrl: session?.brokerUrl ?? null,
      topic,
    };

    return new Message(
      source,
      target,
      payload,
      type,
      LOCAL_SYNC_STATE.PENDING_GLOBAL_SYNC,
      operation,
      requiresResponse,
      correlationId,
    );
  }

  createSession(brokerUrl, sessionId, clientId, onConnected = null) {
    if (this.sessions.has(sessionId)) {
      return { ok: true, sessionId, alreadyCreated: true };
    }

    if (onConnected !== null && typeof onConnected !== 'function') {
      return {
        ok: false,
        sessionId,
        error: new TypeError('onConnected must be a function or null'),
      };
    }

    try {
      const session = new BrokerSession({
        brokerUrl,
        sessionId,
        clientId,
        reconnectPeriodMs: this.reconnectPeriodMs,
        logConnectionEvents: this.logConnectionEvents,
        handleIncomingMessage: (incomingSessionId, topic, messageJSON) =>
          this.handleIncomingMessage(incomingSessionId, topic, messageJSON),
        onUnexpectedClose: event => this.handleUnexpectedSessionClose(event),
        onConnected,
      });

      this.sessions.set(sessionId, session);

      return { ok: true, sessionId, alreadyCreated: false };
    } catch (error) {
      return { ok: false, sessionId, error };
    }
  }

  async connectSession(sessionId, connectionTimeoutMs = null) {
    const session = this.sessions.get(sessionId);

    if (!session) {
      return { ok: false, sessionId, error: 'Session not found' };
    }

    if (
      connectionTimeoutMs !== null &&
      (!Number.isInteger(connectionTimeoutMs) || connectionTimeoutMs <= 0)
    ) {
      throw new TypeError(
        'connectionTimeoutMs must be a positive integer or null',
      );
    }

    const connectionAttempt = session
      .connect()
      .then(() => ({ ok: true, sessionId }))
      .catch(error => ({ ok: false, sessionId, error }));

    if (connectionTimeoutMs === null) {
      return connectionAttempt;
    }

    let timeoutId = null;
    const timeoutAttempt = new Promise(resolve => {
      timeoutId = setTimeout(() => {
        resolve({
          ok: false,
          sessionId,
          timedOut: true,
          error: `Connection timeout after ${connectionTimeoutMs}ms`,
        });
      }, connectionTimeoutMs);
    });
    const connectionResult = await Promise.race([
      connectionAttempt,
      timeoutAttempt,
    ]);
    clearTimeout(timeoutId);

    if (!connectionResult.timedOut) {
      return connectionResult;
    }

    const cleanupResult = await this.closeSession(sessionId);

    return {
      ...connectionResult,
      cleanupResult,
    };
  }

  async subscribe(sessionId, topicFilter, handler, qos = 0) {
    const session = this.sessions.get(sessionId);

    if (!session) {
      return { ok: false, sessionId, error: 'Session not found' };
    }

    try {
      return await session.subscribe(topicFilter, handler, { qos });
    } catch (error) {
      return {
        ok: false,
        sessionId,
        topicFilter,
        error,
      };
    }
  }

  /**
   * @param {string} sessionId
   * @param {string} topic
   * @param {unknown} payload
   * @param {string} type
   * @param {string | null} operation
   * @param {boolean} requiresResponse
   * @param {string | null} correlationId
   * @param {number} qos
   * @param {boolean} retain
   */
  async publish(
    sessionId,
    topic,
    payload,
    type = MESSAGE_TYPES.EVENT,
    operation = null,
    requiresResponse = false,
    correlationId = null,
    qos = 1,
    retain = false,
  ) {
    const session = this.sessions.get(sessionId);

    if (!session) {
      throw new SessionNotFoundError(sessionId);
    }

    let message;

    try {
      message = this.createMessage(
        sessionId,
        topic,
        payload,
        type,
        operation,
        requiresResponse,
        correlationId,
      );
    } catch (error) {
      return { ok: false, sessionId, topic, error, message: null, persistenceResult: null };
    }

    let encryptedMessage = null;
    let persistedMessage = message;
    let persistenceResult = null;

    try {
      if (this.isMessageSecurityEnabled()) {
        encryptedMessage = await this.securityManager.encryptMessage(
          message,
          this.messageSecurityKeyId,
        );
        persistedMessage = encryptedMessage;
      }

      if (isOperationalMessage({ topic, operation })) {
        persistenceResult = await this.messageManager.storeSentMessage(persistedMessage);

        if (!persistenceResult.ok) {
          return { ok: false, sessionId, topic, message, encryptedMessage, persistenceResult };
        }
      }
    } catch (error) {
      return { ok: false, sessionId, topic, error, message, encryptedMessage: null, persistenceResult: null };
    }

    try {
      const publishResult = await session.publish(topic, persistedMessage, { qos, retain });
      let statusPersistenceResult = null;

      if (isOperationalMessage({ topic, operation })) {
        persistedMessage.statusToPublished();
        statusPersistenceResult = await this.messageManager.storeSentMessage(
          persistedMessage,
          true,
        );
      }

      return {
        ...publishResult,
        message,
        encryptedMessage,
        persistenceResult,
        statusPersistenceResult,
      };
    } catch (error) {
      let statusPersistenceResult = null;

      if (isOperationalMessage({ topic, operation })) {
        persistedMessage.statusToError(error?.message ?? String(error));
        try {
          statusPersistenceResult = await this.messageManager.storeSentMessage(
            persistedMessage,
            true,
          );
        } catch (statusPersistenceError) {
          statusPersistenceResult = { ok: false, error: statusPersistenceError };
        }
      }

      return {
        ok: false,
        sessionId,
        topic,
        error,
        message,
        encryptedMessage,
        persistenceResult,
        statusPersistenceResult,
      };
    }
  }

  async persistOperationalMessage(topic, payload) {
    let message;

    try {
      message = this.createMessage(
        null,
        topic,
        payload,
        MESSAGE_TYPES.EVENT,
        null,
        false,
        null,
      );
    } catch (error) {
      return {
        ok: false,
        persisted: false,
        published: false,
        error,
        message: null,
      };
    }

    if (!isOperationalMessage({ topic, operation: message.operation })) {
      return {
        ok: false,
        persisted: false,
        published: false,
        error: 'Only operational messages can be persisted without a session',
        message,
      };
    }

    try {
      const encryptedMessage = this.isMessageSecurityEnabled()
        ? await this.securityManager.encryptMessage(message, this.messageSecurityKeyId)
        : null;
      const persistedMessage = encryptedMessage ?? message;
      const persistenceResult = await this.messageManager.storeSentMessage(persistedMessage);

      return {
        ok: persistenceResult.ok === true,
        persisted: persistenceResult.ok === true,
        published: false,
        pendingGlobalSync: persistenceResult.ok === true,
        message,
        encryptedMessage,
        persistenceResult,
      };
    } catch (error) {
      return {
        ok: false,
        persisted: false,
        published: false,
        error,
        message,
      };
    }
  }

  async closeSession(sessionId) {
    const session = this.sessions.get(sessionId);

    if (!session) {
      return { ok: true, sessionId, alreadyClosed: true };
    }

    const result = await session.destroy();
    this.sessions.delete(sessionId);

    return { ...result, sessionId };
  }

  async handleIncomingMessage(sessionId, topic, messageJSON) {
    const session = this.sessions.get(sessionId);

    if (!session) {
      return { ok: false, sessionId, topic, error: 'Session not found' };
    }

    let encryptedMessage;
    let message;

    try {
      const parsedMessage = JSON.parse(messageJSON.toString());
      if (this.isMessageSecurityEnabled()) {
        encryptedMessage = EncryptedMessage.fromJSON(parsedMessage);
        message = await this.securityManager.decodeMessage(encryptedMessage, topic);
      } else {
        message = Message.fromJSON(parsedMessage);
      }
    } catch (error) {
      return {
        ok: false,
        sessionId,
        topic,
        error,
      };
    }

    return this.processReceivedMessage(sessionId, topic, message, encryptedMessage ?? message, false);
  }

  getMatchingSubscriptions(sessionId, topic) {
    const session = this.sessions.get(sessionId);

    if (!session) {
      return null;
    }

    return Array.from(session.getSuscriptions().entries()).filter(
      ([topicFilter]) => session.matchesTopicFilter(topicFilter, topic),
    );
  }

  async dispatchRecoveredMessageToOperationalHandler(
    sessionId,
    operationalTopic,
    messageData,
  ) {
    let encryptedMessage;
    let message;

    try {
      if (this.isMessageSecurityEnabled()) {
        encryptedMessage = messageData instanceof EncryptedMessage
          ? messageData
          : EncryptedMessage.fromJSON(messageData);
      } else {
        message = messageData instanceof Message
          ? messageData
          : Message.fromJSON(messageData);
      }
    } catch (error) {
      return { ok: false, sessionId, error };
    }

    if (
      typeof operationalTopic !== 'string' ||
      operationalTopic.trim() === ''
    ) {
      return {
        ok: false,
        sessionId,
        messageId: (encryptedMessage ?? message).messageId,
        error: 'Recovered operational topic is required',
      };
    }

    if (this.isMessageSecurityEnabled()) {
      try {
        message = await this.securityManager.decodeMessage(
          encryptedMessage,
          operationalTopic.trim(),
        );
      } catch (error) {
        return { ok: false, sessionId, messageId: encryptedMessage.messageId, error };
      }
    }

    return this.processReceivedMessage(
      sessionId,
      operationalTopic.trim(),
      message,
      encryptedMessage ?? message,
      true,
    );
  }

  async processReceivedMessage(
    sessionId,
    topic,
    message,
    persistedMessage,
    recoveredFromResyncBatch,
  ) {
    const session = this.sessions.get(sessionId);

    if (!session) {
      return { ok: false, sessionId, topic, error: 'Session not found' };
    }

    const matchingSubscriptions = this.getMatchingSubscriptions(sessionId, topic);

    if (matchingSubscriptions.length === 0) {
      return { ok: true, sessionId, topic, ignored: true };
    }

    let persistenceResult = null;

    if (isOperationalMessage({ topic, operation: message.operation })) {
      if (this.messageManager.hasMessage(persistedMessage.messageId)) {
        return {
          ok: true,
          sessionId,
          topic,
          duplicate: true,
          processed: false,
          persistenceResult: null,
          results: [],
        };
      }

      try {
        persistedMessage.statusToProcessing();
        persistenceResult = await this.messageManager.storeReceivedMessage(persistedMessage);
      } catch (error) {
        return { ok: false, sessionId, topic, error, persistenceResult: null };
      }

      if (!persistenceResult.ok) {
        return { ok: false, sessionId, topic, persistenceResult };
      }

      if (persistenceResult.duplicate === true) {
        return {
          ok: true,
          sessionId,
          topic,
          duplicate: true,
          processed: false,
          persistenceResult,
          results: [],
        };
      }
    }

    const results = [];

    for (const [, subscription] of matchingSubscriptions) {
      try {
        const result = await subscription.handler(message, {
          sessionId,
          topic,
          recoveredFromResyncBatch,
          ...(persistedMessage instanceof EncryptedMessage
            ? { encryptedMessage: persistedMessage.toJSON() }
            : {}),
        });
        results.push({ ok: true, result: result ?? null });
      } catch (error) {
        results.push({ ok: false, error });
      }
    }

    let statusPersistenceResult = null;

    if (isOperationalMessage({ topic, operation: message.operation })) {
      if (results.every(result => result.ok)) {
        persistedMessage.statusToProcessed();
      } else {
        persistedMessage.statusToError();
      }

      try {
        statusPersistenceResult = await this.messageManager.storeReceivedMessage(
          persistedMessage,
          true,
        );
      } catch (error) {
        results.push({ ok: false, error });
      }
    }

    return {
      ok: results.every(result => result.ok),
      sessionId,
      topic,
      duplicate: false,
      processed: true,
      persistenceResult,
      statusPersistenceResult,
      results,
    };
  }
}

export { SessionManager, SessionNotFoundError };
