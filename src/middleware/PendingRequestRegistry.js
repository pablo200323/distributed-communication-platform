const DEFAULT_PENDING_REQUEST_TIMEOUT_MS = 3000;
const DEFAULT_PENDING_REQUEST_TIMEOUT_ERROR = 'Request timeout';
const DEFAULT_PENDING_REQUEST_CLEAR_ERROR = 'Pending request cleared';

function buildPendingRequestFailureResult(error) {
  return {
    ok: false,
    error,
  };
}

export class PendingRequestRegistry {
  constructor(defaultTimeoutMs = DEFAULT_PENDING_REQUEST_TIMEOUT_MS) {
    this.defaultTimeoutMs = defaultTimeoutMs;
    this.pendingRequests = new Map();
  }

  normalizeCorrelationId(correlationId) {
    if (typeof correlationId !== 'string' || correlationId.trim() === '') {
      return null;
    }

    return correlationId.trim();
  }

  register({
    correlationId,
    operationType,
    timeoutMs = this.defaultTimeoutMs,
    metadata = {},
    onTimeout = null,
  }) {
    const normalizedCorrelationId = this.normalizeCorrelationId(correlationId);

    if (!normalizedCorrelationId) {
      throw new Error('correlationId must be a non-empty string');
    }

    if (this.pendingRequests.has(normalizedCorrelationId)) {
      throw new Error(
        `Pending request already exists for correlationId ${normalizedCorrelationId}`,
      );
    }

    const effectiveTimeoutMs =
      Number.isFinite(timeoutMs) && timeoutMs > 0
        ? timeoutMs
        : this.defaultTimeoutMs;

    let resolvePending = null;
    const responsePromise = new Promise(resolve => {
      resolvePending = resolve;
    });

    const entry = {
      correlationId: normalizedCorrelationId,
      operationType,
      createdAt: Date.now(),
      metadata: metadata ?? {},
      onTimeout,
      resolve: resolvePending,
      timeout: null,
    };

    entry.timeout = setTimeout(() => {
      this.resolveTimeout(normalizedCorrelationId);
    }, effectiveTimeoutMs);

    this.pendingRequests.set(normalizedCorrelationId, entry);

    return responsePromise;
  }

  get(correlationId) {
    const normalizedCorrelationId = this.normalizeCorrelationId(correlationId);

    if (!normalizedCorrelationId) {
      return null;
    }

    const entry = this.pendingRequests.get(normalizedCorrelationId);

    if (!entry) {
      return null;
    }

    return {
      correlationId: entry.correlationId,
      operationType: entry.operationType,
      createdAt: entry.createdAt,
      metadata: entry.metadata,
    };
  }

  has(correlationId) {
    const normalizedCorrelationId = this.normalizeCorrelationId(correlationId);

    return normalizedCorrelationId
      ? this.pendingRequests.has(normalizedCorrelationId)
      : false;
  }

  updateMetadata(correlationId, metadataPatch = {}) {
    const normalizedCorrelationId = this.normalizeCorrelationId(correlationId);
    const entry = normalizedCorrelationId
      ? this.pendingRequests.get(normalizedCorrelationId)
      : null;

    if (!entry) {
      return false;
    }

    entry.metadata = {
      ...entry.metadata,
      ...(metadataPatch ?? {}),
    };

    return true;
  }

  resolve(correlationId, result) {
    return this.complete(correlationId, result);
  }

  clear(
    correlationId,
    result = buildPendingRequestFailureResult(
      DEFAULT_PENDING_REQUEST_CLEAR_ERROR,
    ),
  ) {
    return this.complete(correlationId, result);
  }

  clearAll(
    result = buildPendingRequestFailureResult(
      DEFAULT_PENDING_REQUEST_CLEAR_ERROR,
    ),
  ) {
    const correlationIds = Array.from(this.pendingRequests.keys());

    for (const correlationId of correlationIds) {
      this.clear(correlationId, result);
    }

    return correlationIds.length;
  }

  resolveTimeout(correlationId) {
    const normalizedCorrelationId = this.normalizeCorrelationId(correlationId);
    const entry = normalizedCorrelationId
      ? this.pendingRequests.get(normalizedCorrelationId)
      : null;

    if (!entry) {
      return false;
    }

    let timeoutResult = buildPendingRequestFailureResult(
      DEFAULT_PENDING_REQUEST_TIMEOUT_ERROR,
    );

    if (typeof entry.onTimeout === 'function') {
      try {
        timeoutResult =
          entry.onTimeout(this.get(normalizedCorrelationId)) ??
          buildPendingRequestFailureResult(
            DEFAULT_PENDING_REQUEST_TIMEOUT_ERROR,
          );
      } catch {
        timeoutResult = buildPendingRequestFailureResult(
          DEFAULT_PENDING_REQUEST_TIMEOUT_ERROR,
        );
      }
    }

    return this.complete(normalizedCorrelationId, timeoutResult);
  }

  complete(correlationId, result) {
    const normalizedCorrelationId = this.normalizeCorrelationId(correlationId);
    const entry = normalizedCorrelationId
      ? this.pendingRequests.get(normalizedCorrelationId)
      : null;

    if (!entry) {
      return false;
    }

    clearTimeout(entry.timeout);
    this.pendingRequests.delete(normalizedCorrelationId);
    entry.resolve(result);

    return true;
  }
}
