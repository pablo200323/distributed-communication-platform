export const ENVIRONMENT_STATUS = Object.freeze({
  UNKNOWN: 'UNKNOWN',
  AVAILABLE: 'AVAILABLE',
  UNAVAILABLE: 'UNAVAILABLE',
  ERROR: 'ERROR',
});

export const BROKER_PROCESS_STATUS = Object.freeze({
  UNKNOWN: 'UNKNOWN',
  STOPPED: 'STOPPED',
  RUNNING: 'RUNNING',
  ERROR: 'ERROR',
});

export const BROKER_ROLE = Object.freeze({
  PRIMARY: 'PRIMARY',
  TEMPORARY: 'TEMPORARY',
});

export const BRIDGE_RULE_DIRECTION = Object.freeze({
  OUTBOUND: 'out',
  INBOUND: 'in',
});
