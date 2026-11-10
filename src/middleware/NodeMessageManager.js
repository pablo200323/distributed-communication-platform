import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { EncryptedMessage } from './EncryptedMessage.js';
import { LOCAL_SYNC_STATE, Message } from './Message.js';

const MESSAGE_ORIGIN = Object.freeze({
  SENT: 'SENT',
  RECEIVED: 'RECEIVED',
});

const INITIAL_STORAGE_SCHEMA_VERSION = 1;
const STORAGE_SCHEMA_VERSION = 2;
const DEFAULT_MESSAGES_FILE_NAME = 'local_messages.json';

class MessageManager {
  constructor(
    storageDirectory = process.env.MESSAGE_STORAGE_DIRECTORY ?? './data',
    messagesFileName = DEFAULT_MESSAGES_FILE_NAME,
  ) {
    this.storageDirectory = storageDirectory;
    this.messagesFilePath = join(storageDirectory, messagesFileName);
    this.messages = new Map();
    this.pendingOperations = [];
    this.processingOperations = false;
    this.loaded = false;
  }

  async initialize() {
    if (this.loaded) {
      return { ok: true, alreadyInitialized: true, recoveredMessages: this.messages.size };
    }

    await mkdir(this.storageDirectory, { recursive: true });

    try {
      const storedDocument = JSON.parse(await readFile(this.messagesFilePath, 'utf8'));
      this.messages = this.readStoredMessages(storedDocument);
      this.loaded = true;
      return { ok: true, recoveredMessages: this.messages.size };
    } catch (error) {
      if (error?.code === 'ENOENT') {
        this.loaded = true;
        return { ok: true, recoveredMessages: 0 };
      }

      return { ok: false, error: error?.message ?? String(error) };
    }
  }

  readStoredMessages(storedDocument) {
    if (!storedDocument || typeof storedDocument !== 'object' || Array.isArray(storedDocument)) {
      throw new TypeError('Stored messages must be an object');
    }

    if (
      storedDocument.schemaVersion !== INITIAL_STORAGE_SCHEMA_VERSION &&
      storedDocument.schemaVersion !== STORAGE_SCHEMA_VERSION
    ) {
      throw new TypeError('Stored messages use an unsupported schemaVersion');
    }

    if (!storedDocument.messages || typeof storedDocument.messages !== 'object' || Array.isArray(storedDocument.messages)) {
      throw new TypeError('Stored messages must contain a messages object');
    }

    const recoveredMessages = new Map();

    for (const [messageId, entry] of Object.entries(storedDocument.messages)) {
      if (!entry || typeof entry !== 'object' || !Object.values(MESSAGE_ORIGIN).includes(entry.origin)) {
        throw new TypeError(`Stored message '${messageId}' has an invalid origin`);
      }

      const hasEncryptedMessage = entry.encryptedMessage !== undefined;
      const hasMessage = entry.message !== undefined;

      if (hasEncryptedMessage === hasMessage) {
        throw new TypeError(`Stored message '${messageId}' must contain exactly one message form`);
      }

      if (
        storedDocument.schemaVersion === INITIAL_STORAGE_SCHEMA_VERSION &&
        hasEncryptedMessage
      ) {
        throw new TypeError(`Stored message '${messageId}' cannot be encrypted in schemaVersion 1`);
      }

      const storedMessage = hasEncryptedMessage
        ? EncryptedMessage.fromJSON(entry.encryptedMessage)
        : Message.fromJSON(entry.message);

      if (storedMessage.messageId !== messageId) {
        throw new TypeError(`Stored message '${messageId}' has a different messageId`);
      }

      recoveredMessages.set(messageId, hasEncryptedMessage
        ? { origin: entry.origin, encryptedMessage: storedMessage }
        : { origin: entry.origin, message: storedMessage });
    }

    return recoveredMessages;
  }

  buildStoredDocument() {
    return {
      schemaVersion: STORAGE_SCHEMA_VERSION,
      messages: Object.fromEntries(
        Array.from(this.messages.entries()).map(([messageId, entry]) => [
          messageId,
          entry.encryptedMessage
            ? { origin: entry.origin, encryptedMessage: entry.encryptedMessage.toJSON() }
            : { origin: entry.origin, message: entry.message.toJSON() },
        ]),
      ),
    };
  }

  async writeMessages() {
    await writeFile(
      this.messagesFilePath,
      JSON.stringify(this.buildStoredDocument(), null, 2),
      'utf8',
    );
  }

  assertInitialized() {
    if (!this.loaded) {
      throw new Error('MessageManager must be initialized before use');
    }
  }

  enqueueOperation(applyChange) {
    return new Promise((resolve, reject) => {
      this.pendingOperations.push({ applyChange, resolve, reject });
      this.processOperations();
    });
  }

  async processOperations() {
    if (this.processingOperations) {
      return;
    }

    this.processingOperations = true;

    try {
      while (this.pendingOperations.length > 0) {
        const operation = this.pendingOperations.shift();
        const previousMessages = this.messages;

        try {
          const result = operation.applyChange();

          if (result.changed) {
            await this.writeMessages();
          }

          operation.resolve(result.value);
        } catch (error) {
          this.messages = previousMessages;
          operation.reject(error);
        }
      }
    } finally {
      this.processingOperations = false;
    }
  }

  storeSentMessage(message, replace = false) {
    return this.storeMessage(message, MESSAGE_ORIGIN.SENT, replace);
  }

  storeReceivedMessage(message, replace = false) {
    return this.storeMessage(message, MESSAGE_ORIGIN.RECEIVED, replace);
  }

  getMessageEntry(messageId) {
    this.assertInitialized();

    if (typeof messageId !== 'string' || messageId.trim() === '') {
      return null;
    }

    return this.messages.get(messageId.trim()) ?? null;
  }

  hasMessage(messageId) {
    return this.getMessageEntry(messageId) !== null;
  }

  storeMessage(message, origin, replace = false) {
    this.assertInitialized();
    if (typeof replace !== 'boolean') {
      throw new TypeError('replace must be a boolean');
    }

    const serializedMessage = message?.toJSON?.() ?? message;
    const isEncryptedMessage = serializedMessage?.encryptedContent !== undefined;
    const normalizedMessage = isEncryptedMessage
      ? (message instanceof EncryptedMessage ? message : EncryptedMessage.fromJSON(message))
      : (message instanceof Message ? message : Message.fromJSON(message));

    const messageId = normalizedMessage.messageId;

    return this.enqueueOperation(() => {
      const existingEntry = this.messages.get(messageId);

      if (existingEntry) {
        if (replace && existingEntry.origin === origin) {
          const existingMessage = existingEntry.encryptedMessage ?? existingEntry.message;
          if (
            existingMessage.syncState === LOCAL_SYNC_STATE.SYNCED &&
            normalizedMessage.syncState !== LOCAL_SYNC_STATE.SYNCED
          ) {
            normalizedMessage.markAsSynced();
          }

          const nextMessages = new Map(this.messages);
          nextMessages.set(messageId, isEncryptedMessage
            ? { origin, encryptedMessage: normalizedMessage }
            : { origin, message: normalizedMessage });
          this.messages = nextMessages;

          return {
            changed: true,
            value: { ok: true, messageId, duplicate: false, replaced: true },
          };
        }

        return {
          changed: false,
          value: {
            ok: existingEntry.origin === origin,
            messageId,
            duplicate: true,
            error: existingEntry.origin === origin
              ? null
              : `Message '${messageId}' already has origin '${existingEntry.origin}'`,
          },
        };
      }

      const nextMessages = new Map(this.messages);
      nextMessages.set(messageId, isEncryptedMessage
        ? { origin, encryptedMessage: normalizedMessage }
        : { origin, message: normalizedMessage });
      this.messages = nextMessages;

      return { changed: true, value: { ok: true, messageId, duplicate: false } };
    });
  }

  markMessageAsSynced(messageId) {
    this.assertInitialized();

    if (typeof messageId !== 'string' || messageId.trim() === '') {
      return Promise.resolve({ ok: false, error: 'messageId must be a non-empty string' });
    }

    const normalizedMessageId = messageId.trim();

    return this.enqueueOperation(() => {
      const entry = this.messages.get(normalizedMessageId);

      if (!entry) {
        return {
          changed: false,
          value: { ok: false, messageId: normalizedMessageId, error: 'Message was not found' },
        };
      }

      const storedMessage = entry.encryptedMessage ?? entry.message;
      if (storedMessage.syncState === LOCAL_SYNC_STATE.SYNCED) {
        return {
          changed: false,
          value: { ok: true, messageId: normalizedMessageId, alreadySynced: true },
        };
      }

      const syncedMessage = entry.encryptedMessage
        ? EncryptedMessage.fromJSON(entry.encryptedMessage.toJSON())
        : Message.fromJSON(entry.message.toJSON());
      syncedMessage.markAsSynced();
      const nextMessages = new Map(this.messages);
      nextMessages.set(normalizedMessageId, entry.encryptedMessage
        ? { ...entry, encryptedMessage: syncedMessage }
        : { ...entry, message: syncedMessage });
      this.messages = nextMessages;

      return {
        changed: true,
        value: { ok: true, messageId: normalizedMessageId, alreadySynced: false },
      };
    });
  }

  getStoredMessages() {
    this.assertInitialized();

    return this.enqueueOperation(() => ({
      changed: false,
      value: Array.from(this.messages.values()).map(entry => entry.encryptedMessage
        ? { origin: entry.origin, encryptedMessage: entry.encryptedMessage }
        : { origin: entry.origin, message: entry.message }),
    }));
  }

  async getPendingMessages() {
    const storedMessages = await this.getStoredMessages();

    return storedMessages.filter(
      entry => (entry.encryptedMessage ?? entry.message).syncState === LOCAL_SYNC_STATE.PENDING_GLOBAL_SYNC,
    );
  }

  async getSyncedMessages() {
    const storedMessages = await this.getStoredMessages();

    return storedMessages.filter(
      entry => (entry.encryptedMessage ?? entry.message).syncState === LOCAL_SYNC_STATE.SYNCED,
    );
  }

  clearMessages() {
    this.assertInitialized();

    return this.enqueueOperation(() => {
      if (this.messages.size === 0) {
        return { changed: false, value: { ok: true, alreadyEmpty: true } };
      }

      this.messages = new Map();
      return { changed: true, value: { ok: true, alreadyEmpty: false } };
    });
  }
}

export { MessageManager, MESSAGE_ORIGIN };
