export const MESSAGE_SECURITY_ERROR = Object.freeze({
  INVALID_ENCRYPTED_CONTENT: 'INVALID_ENCRYPTED_CONTENT',
  INVALID_ENCRYPTED_MESSAGE: 'INVALID_ENCRYPTED_MESSAGE',
  INVALID_MESSAGE_TOPIC: 'INVALID_MESSAGE_TOPIC',
  UNKNOWN_KEY: 'UNKNOWN_KEY',
  AUTHENTICATION_FAILED: 'AUTHENTICATION_FAILED',
});

export const DEFAULT_MESSAGE_SECURITY_KEY_ID = 'test-group-key-v1';

export function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim() !== '';
}

export function assertEncryptedContent(encryptedContent, description = 'encrypted content') {
  if (
    !encryptedContent ||
    typeof encryptedContent !== 'object' ||
    Array.isArray(encryptedContent)
  ) {
    throw new TypeError(`${description} must be an object`);
  }

  const requiredFields = ['keyId', 'nonce', 'ciphertext', 'tag'];

  for (const field of requiredFields) {
    if (!isNonEmptyString(encryptedContent[field])) {
      throw new TypeError(`${description}.${field} must be a non-empty string`);
    }
  }
}

export function isEncryptedContent(encryptedContent) {
  try {
    assertEncryptedContent(encryptedContent);
    return true;
  } catch {
    return false;
  }
}
