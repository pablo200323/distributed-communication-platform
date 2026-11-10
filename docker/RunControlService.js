import { spawn } from 'node:child_process';
import net from 'node:net';
import { ControlService } from '../src/middleware/ControlSesion.js';
import { NodeSecurityManager } from '../src/middleware/NodeSecurityManager.js';
import { logRuntimeError } from '../src/middleware/RuntimeLogger.js';

const brokerId = process.env.BROKER_ID;
const brokerType = process.env.BROKER_TYPE;
const batteryPct = Number(process.env.BROKER_BATTERY_PCT);
const localBrokerUrl = 'mqtt://127.0.0.1:1883';
const localBrokerHost = '127.0.0.1';
const localBrokerPort = 1883;
const mosquittoConfigPath = '/mosquitto/config/mosquitto.conf';
const brokerStartTimeoutMs = 10000;
const brokerStartRetryDelayMs = 100;
const messageSecurityKeyId = process.env.MESSAGE_SECURITY_KEY_ID;
const messageSecurityKeyBase64 = process.env.MESSAGE_SECURITY_KEY_BASE64;

if (typeof brokerId !== 'string' || brokerId.trim() === '') {
  throw new Error('BROKER_ID must identify this broker container');
}

if (typeof brokerType !== 'string' || brokerType.trim() === '') {
  throw new Error('BROKER_TYPE must identify the broker role');
}

if (!Number.isFinite(batteryPct) || batteryPct < 0 || batteryPct > 100) {
  throw new Error('BROKER_BATTERY_PCT must be a number between 0 and 100');
}
if (typeof messageSecurityKeyId !== 'string' || messageSecurityKeyId.trim() === '' || typeof messageSecurityKeyBase64 !== 'string' || messageSecurityKeyBase64.trim() === '') {
  throw new Error('MESSAGE_SECURITY_KEY_ID and MESSAGE_SECURITY_KEY_BASE64 are required');
}

let controlService = null;
let mosquittoProcess = null;
let shutdownStarted = false;

function waitForLocalBroker() {
  const deadline = Date.now() + brokerStartTimeoutMs;

  return new Promise((resolve, reject) => {
    const attemptConnection = () => {
      const socket = net.createConnection({ host: localBrokerHost, port: localBrokerPort });

      socket.once('connect', () => {
        socket.end();
        resolve();
      });

      socket.once('error', () => {
        socket.destroy();

        if (Date.now() >= deadline) {
          reject(new Error('Mosquitto did not become available on ' + localBrokerUrl));
          return;
        }

        setTimeout(attemptConnection, brokerStartRetryDelayMs);
      });
    };

    attemptConnection();
  });
}

async function shutdown(signal) {
  if (shutdownStarted) {
    return;
  }

  shutdownStarted = true;

  try {
    if (controlService) {
      await controlService.destroy();
    }
  } finally {
    if (mosquittoProcess && mosquittoProcess.exitCode === null) {
      mosquittoProcess.kill('SIGTERM');
    }
  }
}

async function start() {
  mosquittoProcess = spawn('mosquitto', ['-c', mosquittoConfigPath], { stdio: 'ignore' });

  mosquittoProcess.once('error', error => {
    logRuntimeError('[' + brokerId + '] No se pudo iniciar Mosquitto', error);
    process.exitCode = 1;
  });

  mosquittoProcess.once('exit', (code, signal) => {
    if (!shutdownStarted) {
      logRuntimeError('[' + brokerId + '] Mosquitto finalizó inesperadamente', { code, signal });
      process.exitCode = 1;
      shutdown('salida inesperada de Mosquitto').catch(error => logRuntimeError(error));
    }
  });

  await waitForLocalBroker();

  const securityManager = new NodeSecurityManager(messageSecurityKeyId, messageSecurityKeyBase64);
  controlService = new ControlService(
    localBrokerUrl,
    'control-session-' + brokerId,
    'control-service-' + brokerId,
    brokerId,
    5000,
    brokerType,
    batteryPct,
    securityManager,
    messageSecurityKeyId,
  );

  await controlService.initializationPromise;
}

process.once('SIGINT', () => shutdown('SIGINT').catch(error => logRuntimeError(error)));
process.once('SIGTERM', () => shutdown('SIGTERM').catch(error => logRuntimeError(error)));

start().catch(async error => {
  logRuntimeError('[' + brokerId + '] No se pudo iniciar el entorno del broker', error);
  process.exitCode = 1;
  await shutdown('error de inicio');
});
