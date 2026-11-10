import RNFS from 'react-native-fs';

import { AndroidSecurityManager } from './middleware/AndroidSecurityManager';

export const MESSAGE_SECURITY_KEY_ID = 'platform-group-v1';
export const MESSAGE_SECURITY_BOOTSTRAP_PATH = `${RNFS.DocumentDirectoryPath}/platform-message-security-bootstrap.json`;

type SecurityBootstrapDocument = {
  keyId?: unknown;
  base64Key?: unknown;
};

export type MessageSecurityState = {
  ok: boolean;
  state: 'UNAVAILABLE' | 'PREPARED';
  keyId: string;
  importedDuringBootstrap: boolean;
  bootstrapRemoved: boolean;
  cleanupWarning: string | null;
  error: string | null;
  securityManager: AndroidSecurityManager | null;
};

function isSecurityBootstrapDocument(
  value: SecurityBootstrapDocument,
): value is { keyId: string; base64Key: string } {
  return (
    value?.keyId === MESSAGE_SECURITY_KEY_ID &&
    typeof value.base64Key === 'string' &&
    value.base64Key.trim() !== ''
  );
}

export async function prepareMessageSecurity(): Promise<MessageSecurityState> {
  const securityManager = new AndroidSecurityManager();

  try {
    const bootstrapAvailable = await RNFS.exists(
      MESSAGE_SECURITY_BOOTSTRAP_PATH,
    );

    if (!bootstrapAvailable && await securityManager.hasKey(
      MESSAGE_SECURITY_KEY_ID,
    )) {
      return {
        ok: true,
        state: 'PREPARED',
        keyId: MESSAGE_SECURITY_KEY_ID,
        importedDuringBootstrap: false,
        bootstrapRemoved: false,
        cleanupWarning: null,
        error: null,
        securityManager,
      };
    }

    if (!bootstrapAvailable) {
      return {
        ok: false,
        state: 'UNAVAILABLE',
        keyId: MESSAGE_SECURITY_KEY_ID,
        importedDuringBootstrap: false,
        bootstrapRemoved: false,
        cleanupWarning: null,
        error:
          'The message-security bootstrap file is unavailable.',
        securityManager: null,
      };
    }

    const parsedDocument = JSON.parse(
      await RNFS.readFile(MESSAGE_SECURITY_BOOTSTRAP_PATH, 'utf8'),
    ) as SecurityBootstrapDocument;

    if (!isSecurityBootstrapDocument(parsedDocument)) {
      return {
        ok: false,
        state: 'UNAVAILABLE',
        keyId: MESSAGE_SECURITY_KEY_ID,
        importedDuringBootstrap: false,
        bootstrapRemoved: false,
        cleanupWarning: null,
        error:
          'The message-security bootstrap document is invalid.',
        securityManager: null,
      };
    }

    await securityManager.importKey(
      MESSAGE_SECURITY_KEY_ID,
      parsedDocument.base64Key,
    );

    let cleanupWarning: string | null = null;
    let bootstrapRemoved = false;

    try {
      await RNFS.unlink(MESSAGE_SECURITY_BOOTSTRAP_PATH);
      bootstrapRemoved = true;
    } catch (error) {
      cleanupWarning = error instanceof Error ? error.message : String(error);
    }

    return {
      ok: true,
      state: 'PREPARED',
      keyId: MESSAGE_SECURITY_KEY_ID,
      importedDuringBootstrap: true,
      bootstrapRemoved,
      cleanupWarning,
      error: null,
      securityManager,
    };
  } catch (error) {
    return {
      ok: false,
      state: 'UNAVAILABLE',
      keyId: MESSAGE_SECURITY_KEY_ID,
      importedDuringBootstrap: false,
      bootstrapRemoved: false,
      cleanupWarning: null,
      error: error instanceof Error ? error.message : String(error),
      securityManager: null,
    };
  }
}
