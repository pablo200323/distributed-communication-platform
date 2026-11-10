import { Buffer } from 'node:buffer';
import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { Message } from './Message.js';
import { EncryptedMessage } from './EncryptedMessage.js';
import { assertMessageSecurityManager } from './MessageSecurityContract.js';
import {
  MESSAGE_SECURITY_ERROR,
  assertEncryptedContent,
  isNonEmptyString,
} from './MessageSecurityContracts.js';

function assertBase64Key(base64Key) {
  if (typeof base64Key !== 'string' || base64Key.trim() === '') {
    throw new TypeError('base64Key must be a non-empty string');
  }
  const key = Buffer.from(base64Key.trim(), 'base64');
  if (key.length !== 32) {
    throw new Error('base64Key must decode to exactly 32 bytes');
  }
  return key;
}

class NodeSecurityManager {
  constructor(keyId, base64Key) {
    if (typeof keyId !== 'string' || keyId.trim() === '') {
      throw new TypeError('keyId must be a non-empty string');
    }
    this.keyId = keyId.trim();
    this.key = assertBase64Key(base64Key);
    assertMessageSecurityManager(this, 'NodeSecurityManager');
  }

  async hasKey(keyId) {
    return keyId?.trim?.() === this.keyId;
  }

  async importKey(keyId, base64Key) {
    if (
      keyId?.trim?.() !== this.keyId ||
      Buffer.compare(assertBase64Key(base64Key), this.key) !== 0
    ) {
      throw new Error(MESSAGE_SECURITY_ERROR.UNKNOWN_KEY);
    }
    return { keyId: this.keyId, imported: false };
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
    const encryptedParts = await this.encryptContent(
      JSON.stringify(messageData),
      normalizedKeyId,
      topic,
    );
    const encryptedContent = {
      keyId: normalizedKeyId,
      nonce: encryptedParts.nonce,
      ciphertext: encryptedParts.ciphertext,
      tag: encryptedParts.tag,
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
    const plaintext = await this.decryptContent(
      encryptedContent.ciphertext.trim(),
      normalizedKeyId,
      encryptedContent.nonce.trim(),
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

  async encryptContent(plaintext, keyId, aad) {
    if (!(await this.hasKey(keyId))) {
      throw new Error(MESSAGE_SECURITY_ERROR.UNKNOWN_KEY);
    }
    if (typeof plaintext !== 'string') {
      throw new TypeError('plaintext must be a string');
    }
    if (typeof aad !== 'string') {
      throw new TypeError('aad must be a string');
    }
    const nonce = randomBytes(12);
    const cipher = createCipheriv('aes-256-gcm', this.key, nonce);
    cipher.setAAD(Buffer.from(aad, 'utf8'));
    const ciphertext = Buffer.concat([
      cipher.update(plaintext, 'utf8'),
      cipher.final(),
    ]);
    return {
      nonce: nonce.toString('base64'),
      ciphertext: ciphertext.toString('base64'),
      tag: cipher.getAuthTag().toString('base64'),
    };
  }

  async decryptContent(ciphertext, keyId, nonce, tag, aad) {
    if (!(await this.hasKey(keyId))) {
      throw new Error(MESSAGE_SECURITY_ERROR.UNKNOWN_KEY);
    }
    if (![ciphertext, nonce, tag, aad].every(value => typeof value === 'string')) {
      throw new TypeError('ciphertext, nonce, tag and aad must be strings');
    }
    try {
      const decipher = createDecipheriv(
        'aes-256-gcm',
        this.key,
        Buffer.from(nonce, 'base64'),
      );
      decipher.setAAD(Buffer.from(aad, 'utf8'));
      decipher.setAuthTag(Buffer.from(tag, 'base64'));
      return Buffer.concat([
        decipher.update(Buffer.from(ciphertext, 'base64')),
        decipher.final(),
      ]).toString('utf8');
    } catch (error) {
      error.code = MESSAGE_SECURITY_ERROR.AUTHENTICATION_FAILED;
      throw error;
    }
  }
}

export { NodeSecurityManager };
