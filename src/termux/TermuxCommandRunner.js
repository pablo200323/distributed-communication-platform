import { NativeModules, Platform } from 'react-native';

const { TermuxCommandRunner } = NativeModules;

function normalizeError(error, command = null) {
  if (error instanceof Error) {
    const plainError = {};

    Object.getOwnPropertyNames(error).forEach((key) => {
      plainError[key] = error[key];
    });

    return command ? { command, ...plainError } : plainError;
  }

  if (typeof error === 'object' && error !== null) {
    return command ? { command, ...error } : error;
  }

  return command
    ? { command, message: String(error) }
    : { message: String(error) };
}

export async function requestTermuxRunCommandPermission() {
  try {
    if (Platform.OS !== 'android') {
      return {
        granted: false,
        errorCode: 'ANDROID_ONLY',
        errorMessage: 'Termux command execution is only available on Android.',
      };
    }

    if (!TermuxCommandRunner) {
      return {
        granted: false,
        errorCode: 'NATIVE_MODULE_UNAVAILABLE',
        errorMessage: 'Native module TermuxCommandRunner is not available.',
      };
    }

    return await TermuxCommandRunner.requestRunCommandPermission();
  } catch (error) {
    return {
      granted: false,
      errorCode: 'TERMUX_PERMISSION_REQUEST_FAILED',
      errorMessage: String(error?.message ?? error),
      error,
    };
  }
}

function withTimeout(promise, timeoutMs, command) {
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      reject({
        code: 'TERMUX_COMMAND_TIMEOUT',
        message: `Termux command did not finish within ${timeoutMs} ms.`,
        command,
      });
    }, timeoutMs);

    promise
      .then((result) => {
        clearTimeout(timeoutId);
        resolve(result);
      })
      .catch((error) => {
        clearTimeout(timeoutId);
        reject(error);
      });
  });
}

export async function checkTermuxAvailability() {
  try {
    if (Platform.OS !== 'android') {
      return {
        available: false,
        termuxInstalled: false,
        runCommandServiceAvailable: false,
        permissionGranted: false,
        errorCode: 'ANDROID_ONLY',
        errorMessage: 'Termux command execution is only available on Android.',
      };
    }

    if (!TermuxCommandRunner) {
      return {
        available: false,
        termuxInstalled: false,
        runCommandServiceAvailable: false,
        permissionGranted: false,
        errorCode: 'NATIVE_MODULE_UNAVAILABLE',
        errorMessage: 'Native module TermuxCommandRunner is not available.',
      };
    }

    return await TermuxCommandRunner.checkAvailability();
  } catch (error) {
    return {
      available: false,
      termuxInstalled: false,
      runCommandServiceAvailable: false,
      permissionGranted: false,
      errorCode: 'TERMUX_AVAILABILITY_CHECK_FAILED',
      errorMessage: String(error?.message ?? error),
      error: normalizeError(error),
    };
  }
}

export async function runTermuxCommand(command, timeoutMs = 7000) {
  try {
    if (Platform.OS !== 'android') {
      throw {
        code: 'ANDROID_ONLY',
        message: 'Termux command execution is only available on Android.',
        command,
      };
    }

    if (!TermuxCommandRunner) {
      throw {
        code: 'NATIVE_MODULE_UNAVAILABLE',
        message: 'Native module TermuxCommandRunner is not available.',
        command,
      };
    }

    const availability = await checkTermuxAvailability();

    if (!availability.available) {
      throw {
        code: availability.errorCode || 'TERMUX_UNAVAILABLE',
        message: availability.errorMessage || 'Termux is not available.',
        command,
        availability,
      };
    }

    const result = await withTimeout(
      TermuxCommandRunner.run(command),
      timeoutMs,
      command
    );

    const stdout = result?.stdout ?? '';
    const stderr = result?.stderr ?? '';
    const exitCode = result?.exitCode ?? -1;
    const errCode = result?.errCode ?? -1;
    const errmsg = result?.errmsg ?? '';

    if (errCode !== -1) {
      throw {
        code: 'TERMUX_INTERNAL_ERROR',
        message: errmsg || 'Termux internal execution error.',
        command,
        stdout,
        stderr,
        exitCode,
        errCode,
        errmsg,
      };
    }

    if (exitCode !== 0) {
      throw {
        code: 'TERMUX_COMMAND_FAILED',
        message: stderr || stdout || `Command exited with code ${exitCode}.`,
        command,
        stdout,
        stderr,
        exitCode,
        errCode,
        errmsg,
      };
    }

    return {
      command,
      stdout,
      stderr,
      exitCode,
      errCode,
      errmsg,
    };
  } catch (error) {
    throw normalizeError(error, command);
  }
}