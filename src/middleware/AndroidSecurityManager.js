import { NativeModules } from 'react-native';
import { EncryptedMessage } from './EncryptedMessage.js';
import { Message } from './Message.js';
import { assertMessageSecurityManager } from './MessageSecurityContract.js';

const MESSAGE_SECURITY_ERROR = Object.freeze({
  INVALID_ENCRYPTED_CONTENT: 'INVALID_ENCRYPTED_CONTENT',
  INVALID_ENCRYPTED_MESSAGE: 'INVALID_ENCRYPTED_MESSAGE',
  INVALID_MESSAGE_TOPIC: 'INVALID_MESSAGE_TOPIC',
  UNKNOWN_KEY: 'UNKNOWN_KEY',
});

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim() !== '';
}

function assertEncryptedContent(encryptedContent) {
  if (!encryptedContent || typeof encryptedContent !== 'object' || Array.isArray(encryptedContent)) {
    throw new TypeError('encryptedContent must be an object');
  }

  for (const field of ['keyId', 'nonce', 'ciphertext', 'tag']) {
    if (!isNonEmptyString(encryptedContent[field])) {
      throw new TypeError(`encryptedContent.${field} must be a non-empty string`);
    }
  }
}

class AndroidSecurityManager {
  constructor() {
    this.nativeSecurityModule = NativeModules?.MessageSecurity;
    assertMessageSecurityManager(this, 'AndroidSecurityManager');
  }

  getNativeSecurityModule() {
    const nativeSecurityModule = this.nativeSecurityModule;

    if (!nativeSecurityModule) {
      throw new Error('MessageSecurity native module is not available');
    }

    for (const methodName of ['hasKey', 'importKey', 'encrypt', 'decrypt']) {
      if (typeof nativeSecurityModule[methodName] !== 'function') {
        throw new TypeError(`MessageSecurity.${methodName} must be a function`);
      }
    }

    return nativeSecurityModule;
  }

  getMessageTopic(message) {
    const topic = message?.target?.topic;

    if (!isNonEmptyString(topic)) {
      const error = new Error(MESSAGE_SECURITY_ERROR.INVALID_MESSAGE_TOPIC);
      error.code = MESSAGE_SECURITY_ERROR.INVALID_MESSAGE_TOPIC;
      throw error;
    }

    return topic.trim();
  }

  async hasKey(keyId) {
    if (!isNonEmptyString(keyId)) {
      throw new TypeError('keyId must be a non-empty string');
    }

    return this.getNativeSecurityModule().hasKey(keyId.trim());
  }

  async importKey(keyId, base64Key) {
    if (!isNonEmptyString(keyId)) {
      throw new TypeError('keyId must be a non-empty string');
    }

    if (!isNonEmptyString(base64Key)) {
      throw new TypeError('base64Key must be a non-empty string');
    }

    return this.getNativeSecurityModule().importKey(keyId.trim(), base64Key.trim());
  }

  async assertKeyAvailable(keyId) {
    if (!isNonEmptyString(keyId)) {
      throw new TypeError('keyId must be a non-empty string');
    }

    const normalizedKeyId = keyId.trim();
    const exists = await this.hasKey(normalizedKeyId);

    if (!exists) {
      const error = new Error(MESSAGE_SECURITY_ERROR.UNKNOWN_KEY);
      error.code = MESSAGE_SECURITY_ERROR.UNKNOWN_KEY;
      throw error;
    }

    return normalizedKeyId;
  }

  async encryptMessage(message, keyId) {
    if (!message || typeof message.toJSON !== 'function') {
      throw new TypeError('message must provide toJSON()');
    }

    const messageData = message.toJSON();
    Message.assertValidateMessage(messageData);

    const topic = this.getMessageTopic(message);
    const normalizedKeyId = await this.assertKeyAvailable(keyId);
    const encryptedParts = await this.getNativeSecurityModule().encrypt(
      normalizedKeyId,
      JSON.stringify(messageData),
      topic,
    );
    const encryptedContent = {
      keyId: normalizedKeyId,
      nonce: encryptedParts?.nonce,
      ciphertext: encryptedParts?.ciphertext,
      tag: encryptedParts?.tag,
    };

    assertEncryptedContent(encryptedContent);
    return EncryptedMessage.fromMessage(message, encryptedContent);
  }

  async decodeMessage(encryptedMessage, receivedTopic) {
    try {
      EncryptedMessage.assertValidateEncryptedMessage(encryptedMessage);
    } catch (error) {
      error.code = MESSAGE_SECURITY_ERROR.INVALID_ENCRYPTED_MESSAGE;
      throw error;
    }

    if (!isNonEmptyString(receivedTopic)) {
      const error = new Error(MESSAGE_SECURITY_ERROR.INVALID_MESSAGE_TOPIC);
      error.code = MESSAGE_SECURITY_ERROR.INVALID_MESSAGE_TOPIC;
      throw error;
    }

    const encryptedContent = encryptedMessage.encryptedContent;
    const normalizedKeyId = await this.assertKeyAvailable(encryptedContent.keyId);
    const plaintext = await this.getNativeSecurityModule().decrypt(
      normalizedKeyId,
      encryptedContent.nonce.trim(),
      encryptedContent.ciphertext.trim(),
      encryptedContent.tag.trim(),
      receivedTopic.trim(),
    );

    let messageData;

    try {
      messageData = JSON.parse(plaintext);
    } catch (error) {
      error.code = MESSAGE_SECURITY_ERROR.INVALID_ENCRYPTED_CONTENT;
      throw error;
    }

    return Message.fromJSON(messageData);
  }
}

export { AndroidSecurityManager, MESSAGE_SECURITY_ERROR };
