import { ObserverNode } from '../src/middleware/ObserverNode.js';
import { logRuntimeError } from '../src/middleware/RuntimeLogger.js';
import { MessageManager } from '../src/middleware/NodeMessageManager.js';
import { NodeSecurityManager } from '../src/middleware/NodeSecurityManager.js';

const observerId = process.env.OBSERVER_ID;
const brokers = process.env.BROKERS;
const messageSecurityKeyId = process.env.MESSAGE_SECURITY_KEY_ID;
const messageSecurityKeyBase64 = process.env.MESSAGE_SECURITY_KEY_BASE64;

if (typeof observerId !== 'string' || observerId.trim() === '') {
  throw new Error('OBSERVER_ID must identify this observer container');
}

if (typeof brokers !== 'string' || brokers.trim() === '') {
  throw new Error('BROKERS must declare the configured brokers as JSON');
}
if (typeof messageSecurityKeyId !== 'string' || messageSecurityKeyId.trim() === '' || typeof messageSecurityKeyBase64 !== 'string' || messageSecurityKeyBase64.trim() === '') {
  throw new Error('MESSAGE_SECURITY_KEY_ID and MESSAGE_SECURITY_KEY_BASE64 are required');
}

let configuredBrokers;

try {
  configuredBrokers = JSON.parse(brokers);
} catch (error) {
  throw new Error('BROKERS must contain valid JSON');
}

if (!Array.isArray(configuredBrokers) || configuredBrokers.length === 0) {
  throw new Error('BROKERS must declare at least one broker');
}

if (!configuredBrokers.every(broker => (
  broker
  && typeof broker.brokerId === 'string'
  && broker.brokerId.trim() !== ''
  && typeof broker.brokerUrl === 'string'
  && broker.brokerUrl.trim() !== ''
))) {
  throw new Error('Each BROKERS entry requires brokerId and brokerUrl');
}

const observer = new ObserverNode(
  observerId,
  configuredBrokers,
  new MessageManager(),
  new NodeSecurityManager(messageSecurityKeyId, messageSecurityKeyBase64),
  messageSecurityKeyId,
);

observer.initialize()
  .then(initializationResult => {
    if (!initializationResult.ok) {
      throw new Error('ObserverNode could not initialize every configured broker');
    }

  })
  .catch(error => {
    logRuntimeError('[' + observerId + '] No se pudo iniciar el observador', error);
    process.exitCode = 1;
  });
