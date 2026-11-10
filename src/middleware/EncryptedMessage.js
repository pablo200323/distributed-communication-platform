import {
  LOCAL_SYNC_STATE,
  MESSAGE_STATUS,
} from './Message.js';
import {
  MESSAGE_SECURITY_ERROR,
  assertEncryptedContent,
  isNonEmptyString,
} from './MessageSecurityContracts.js';

class EncryptedMessage {
  constructor({
    messageId,
    source = null,
    targetTopic,
    syncState = LOCAL_SYNC_STATE.PENDING_GLOBAL_SYNC,
    status = MESSAGE_STATUS.CREATED,
    encryptedContent,
  } = {}) {
    this.messageId = messageId;
    this.source = source;
    this.targetTopic = targetTopic;
    this.syncState = syncState;
    this.status = status;
    this.encryptedContent = encryptedContent;

    EncryptedMessage.assertValidateEncryptedMessage(this);
  }

  static fromMessage(message, encryptedContent) {
    if (!message || typeof message.getMessageId !== 'function') {
      throw new TypeError('message must provide getMessageId()');
    }

    const targetTopic = message.target?.topic;

    if (!isNonEmptyString(targetTopic)) {
      throw new Error(MESSAGE_SECURITY_ERROR.INVALID_MESSAGE_TOPIC);
    }

    return new EncryptedMessage({
      messageId: message.getMessageId(),
      source: message.source ?? null,
      targetTopic: targetTopic.trim(),
      syncState: message.syncState,
      status: message.status,
      encryptedContent,
    });
  }

  static fromJSON(data) {
    EncryptedMessage.assertValidateEncryptedMessage(data);

    return new EncryptedMessage({
      messageId: data.messageId,
      source: data.source ?? null,
      targetTopic: data.targetTopic,
      syncState: data.syncState,
      status: data.status,
      encryptedContent: data.encryptedContent,
    });
  }

  static assertValidateEncryptedMessage(data, description = 'encrypted message') {
    if (!data || typeof data !== 'object' || Array.isArray(data)) {
      throw new TypeError(`${description} must be an object`);
    }

    if (!isNonEmptyString(data.messageId)) {
      throw new TypeError(`${description}.messageId must be a non-empty string`);
    }

    if (
      data.source !== null &&
      (
        typeof data.source !== 'object' ||
        Array.isArray(data.source)
      )
    ) {
      throw new TypeError(`${description}.source must be an object or null`);
    }

    if (!isNonEmptyString(data.targetTopic)) {
      throw new TypeError(`${description}.targetTopic must be a non-empty string`);
    }

    if (!Object.values(LOCAL_SYNC_STATE).includes(data.syncState)) {
      throw new TypeError(`${description}.syncState must be a valid local sync state`);
    }

    if (!Object.values(MESSAGE_STATUS).includes(data.status)) {
      throw new TypeError(`${description}.status must be a valid message status`);
    }

    assertEncryptedContent(data.encryptedContent, `${description}.encryptedContent`);
  }

  statusToPublished(syncState = LOCAL_SYNC_STATE.PENDING_GLOBAL_SYNC) {
    this.status = MESSAGE_STATUS.PUBLISHED;
    if (this.syncState !== LOCAL_SYNC_STATE.SYNCED) {
      this.syncState = syncState;
    }
  }

  statusToProcessing() {
    this.status = MESSAGE_STATUS.PROCESSING;
  }

  statusToProcessed() {
    this.status = MESSAGE_STATUS.PROCESSED;
  }

  statusToError() {
    this.status = MESSAGE_STATUS.ERROR;
    if (this.syncState !== LOCAL_SYNC_STATE.SYNCED) {
      this.syncState = LOCAL_SYNC_STATE.PENDING_GLOBAL_SYNC;
    }
  }

  markAsSynced() {
    this.syncState = LOCAL_SYNC_STATE.SYNCED;
  }

  getMessageId() {
    return this.messageId;
  }

  getTargetTopic() {
    return this.targetTopic;
  }

  toJSON() {
    return {
      messageId: this.messageId,
      source: this.source,
      targetTopic: this.targetTopic,
      syncState: this.syncState,
      status: this.status,
      encryptedContent: this.encryptedContent,
    };
  }
}

export { EncryptedMessage };
