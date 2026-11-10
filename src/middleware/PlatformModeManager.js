export const MIDDLEWARE_MODES = Object.freeze({
  NORMAL: 'NORMAL',
  FALLBACK: 'FALLBACK',
  OFFLINE: 'OFFLINE',
  RESYNC: 'RESYNC',
});

function assertValidMode(mode) {
  if (!Object.values(MIDDLEWARE_MODES).includes(mode)) {
    throw new Error(`Invalid middleware mode: ${String(mode)}`);
  }
}

function assertNullableString(value, fieldName) {
  if (value !== null && (typeof value !== 'string' || value.trim() === '')) {
    throw new Error(`${fieldName} must be null or a non-empty string`);
  }
}

function assertBoolean(value, fieldName) {
  if (typeof value !== 'boolean') {
    throw new Error(`${fieldName} must be a boolean`);
  }
}

class PlatformModeManager {
  constructor({
    initialMode = MIDDLEWARE_MODES.OFFLINE,
    assignedPrimaryBrokerId = null,
    currentBrokerNodeId = null,
    hasPendingResync = false,
  } = {}) {
    assertValidMode(initialMode);
    assertNullableString(assignedPrimaryBrokerId, 'assignedPrimaryBrokerId');
    assertNullableString(currentBrokerNodeId, 'currentBrokerNodeId');
    assertBoolean(hasPendingResync, 'hasPendingResync');

    this.currentMode = initialMode;
    this.assignedPrimaryBrokerId = assignedPrimaryBrokerId;
    this.currentBrokerNodeId = currentBrokerNodeId;
    this.hasPendingResync = hasPendingResync;
    this.lastChangeAt = new Date().toISOString();
    this.transitionLog = [];
  }

  getCurrentMode() {
    return this.currentMode;
  }

  getAssignedPrimaryBrokerId() {
    return this.assignedPrimaryBrokerId;
  }

  getCurrentBrokerNodeId() {
    return this.currentBrokerNodeId;
  }

  setAssignedPrimaryBrokerId(assignedPrimaryBrokerId) {
    assertNullableString(assignedPrimaryBrokerId, 'assignedPrimaryBrokerId');
    this.assignedPrimaryBrokerId = assignedPrimaryBrokerId;

    return { ok: true, assignedPrimaryBrokerId: this.assignedPrimaryBrokerId };
  }

  setCurrentBrokerNodeId(currentBrokerNodeId) {
    assertNullableString(currentBrokerNodeId, 'currentBrokerNodeId');
    this.currentBrokerNodeId = currentBrokerNodeId;

    return { ok: true, currentBrokerNodeId: this.currentBrokerNodeId };
  }

  setPendingResync(hasPendingResync) {
    assertBoolean(hasPendingResync, 'hasPendingResync');
    this.hasPendingResync = hasPendingResync;

    return { ok: true, hasPendingResync: this.hasPendingResync };
  }

  setMode(nextMode) {
    assertValidMode(nextMode);
    if (this.currentMode === nextMode) {
      return { ok: true, changed: false, currentMode: this.currentMode };
    }

    this.currentMode = nextMode;
    this.lastChangeAt = new Date().toISOString();
    return { ok: true, changed: true, currentMode: this.currentMode };
  }

  recordTransition(previousMode, nextMode, previousBrokerNodeId, nextBrokerNodeId) {
    const transition = {
      previousMode,
      nextMode,
      previousBrokerNodeId,
      nextBrokerNodeId,
      changedAt: this.lastChangeAt,
    };
    this.transitionLog.push(transition);
    return transition;
  }

  selectBroker(nextBrokerNodeId) {
    assertNullableString(nextBrokerNodeId, 'nextBrokerNodeId');
    const previousMode = this.currentMode;
    const previousBrokerNodeId = this.currentBrokerNodeId;
    this.currentBrokerNodeId = nextBrokerNodeId;
    this.lastChangeAt = new Date().toISOString();
    const transition = this.recordTransition(
      previousMode,
      previousMode,
      previousBrokerNodeId,
      nextBrokerNodeId,
    );

    return {
      ok: true,
      changed: previousBrokerNodeId !== nextBrokerNodeId,
      transition,
      currentMode: this.currentMode,
      currentBrokerNodeId: this.currentBrokerNodeId,
    };
  }

  transitionMode(nextMode) {
    assertValidMode(nextMode);
    const previousMode = this.currentMode;
    const previousBrokerNodeId = this.currentBrokerNodeId;
    this.currentMode = nextMode;
    this.lastChangeAt = new Date().toISOString();
    const transition = this.recordTransition(
      previousMode,
      nextMode,
      previousBrokerNodeId,
      previousBrokerNodeId,
    );

    return {
      ok: true,
      changed: previousMode !== nextMode,
      transition,
      currentMode: this.currentMode,
      currentBrokerNodeId: this.currentBrokerNodeId,
    };
  }

  transitionTo(nextMode, nextBrokerNodeId = null) {
    assertValidMode(nextMode);
    assertNullableString(nextBrokerNodeId, 'nextBrokerNodeId');
    const previousMode = this.currentMode;
    const previousBrokerNodeId = this.currentBrokerNodeId;
    this.currentMode = nextMode;
    this.currentBrokerNodeId = nextBrokerNodeId;
    this.lastChangeAt = new Date().toISOString();
    const transition = this.recordTransition(
      previousMode,
      nextMode,
      previousBrokerNodeId,
      nextBrokerNodeId,
    );

    return {
      ok: true,
      transition,
      currentMode: this.currentMode,
      currentBrokerNodeId: this.currentBrokerNodeId,
    };
  }

  getTransitionLog() {
    return [...this.transitionLog];
  }

  getLastTransition() {
    return this.transitionLog[this.transitionLog.length - 1] ?? null;
  }

  clearTransitionLog() {
    this.transitionLog = [];
    return { ok: true };
  }

  getState() {
    return {
      currentMode: this.currentMode,
      assignedPrimaryBrokerId: this.assignedPrimaryBrokerId,
      currentBrokerNodeId: this.currentBrokerNodeId,
      hasPendingResync: this.hasPendingResync,
      lastChangeAt: this.lastChangeAt,
      lastTransition: this.getLastTransition(),
      transitionLog: this.getTransitionLog().slice(-5),
    };
  }
}

export { PlatformModeManager };
