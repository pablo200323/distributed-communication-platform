import { v4 as randomUUID } from 'uuid';
import { BrokerSession } from './BrokerSession.js';
import {
    LOCAL_SYNC_STATE,
    Message,
    MESSAGE_OPERATIONS,
    MESSAGE_TYPES,
} from './Message.js';
import { EncryptedMessage } from './EncryptedMessage.js';
import { assertMessageSecurityManager } from './MessageSecurityContract.js';
import { PendingRequestRegistry } from './PendingRequestRegistry.js';
import { logRuntimeError, logRuntimeEvent } from './RuntimeLogger.js';
import { BROKER_ROLE } from './BrokerContracts.js';

const MAX_MANAGED_ENTITY_COUNT = 2;
const MANAGED_ENTITY_REGISTRY_VALIDITY_MS = 5000;
const MANAGED_ENTITY_CHECK_TIMEOUT_MS = 3000;

function buildNodeInboxTopic(nodeId, sessionId) {
    return `control/node/${nodeId}/inbox/${sessionId}`;
}

function buildBrokerGroupCheckRequestTopic(managerNodeId) {
    return `control/manager/${managerNodeId}/group/check/request`;
}

class ControlService {
    constructor(
        brokerUrl,
        sessionId,
        clientId,
        brokerId,
        observerHealthRequestTimeoutMs = 5000,
        brokerType = BROKER_ROLE.PRIMARY,
        batteryPct,
        securityManager = null,
        messageSecurityKeyId = null,
        maximumManagedEntityCount = MAX_MANAGED_ENTITY_COUNT,
        managedEntityRegistryValidityMs = MANAGED_ENTITY_REGISTRY_VALIDITY_MS,
        managedEntityCheckTimeoutMs = MANAGED_ENTITY_CHECK_TIMEOUT_MS,
    ) {
        if (typeof brokerId !== 'string' || brokerId.trim() === '') {
            throw new Error('brokerId is required');
        }

        if (typeof sessionId !== 'string' || sessionId.trim() === '') {
            throw new Error('sessionId is required');
        }

        if (sessionId.trim() === brokerId.trim()) {
            throw new Error('sessionId must differ from brokerId');
        }

        if (!Number.isFinite(observerHealthRequestTimeoutMs) || observerHealthRequestTimeoutMs <= 0) {
            throw new Error('observerHealthRequestTimeoutMs must be a positive number');
        }

        if (!Object.values(BROKER_ROLE).includes(brokerType)) {
            throw new Error(`Invalid brokerType '${String(brokerType)}'`);
        }

        if (!Number.isFinite(batteryPct) || batteryPct < 0 || batteryPct > 100) {
            throw new Error('batteryPct must be a number between 0 and 100');
        }

        if (securityManager !== null) {
            assertMessageSecurityManager(securityManager);
        }

        if (messageSecurityKeyId !== null &&
            (typeof messageSecurityKeyId !== 'string' || messageSecurityKeyId.trim() === '')) {
            throw new TypeError('messageSecurityKeyId must be a non-empty string or null');
        }

        if ((securityManager === null) !== (messageSecurityKeyId === null)) {
            throw new TypeError('securityManager and messageSecurityKeyId must be configured together');
        }

        if (!Number.isInteger(maximumManagedEntityCount) || maximumManagedEntityCount <= 0) {
            throw new Error('maximumManagedEntityCount must be a positive integer');
        }

        if (!Number.isInteger(managedEntityRegistryValidityMs) || managedEntityRegistryValidityMs <= 0) {
            throw new Error('managedEntityRegistryValidityMs must be a positive integer');
        }

        if (!Number.isInteger(managedEntityCheckTimeoutMs) || managedEntityCheckTimeoutMs <= 0) {
            throw new Error('managedEntityCheckTimeoutMs must be a positive integer');
        }

        this.brokerUrl = brokerUrl;
        this.sessionId = sessionId;
        this.clientId = clientId;
        this.brokerId = brokerId;
        this.brokerType = brokerType;
        this.batteryPct = batteryPct;
        this.securityManager = securityManager;
        this.messageSecurityKeyId = messageSecurityKeyId?.trim() ?? null;
        this.observerHealthRequestTimeoutMs = observerHealthRequestTimeoutMs;
        this.maximumManagedEntityCount = maximumManagedEntityCount;
        this.managedEntityRegistryValidityMs = managedEntityRegistryValidityMs;
        this.managedEntityCheckTimeoutMs = managedEntityCheckTimeoutMs;
        this.topicHandlers = this.createTopicHandlers();
        this.pendingRequestRegistry = new PendingRequestRegistry(this.observerHealthRequestTimeoutMs);
        this.managedEntityRegistry = {
            nodeIds: new Set(),
            lastValidatedAtMs: null,
        };
        this.managedEntityJoinQueue = Promise.resolve();
        this.managedEntityHealthMonitoringTimer = null;
        this.managedEntityRegistryRefreshInFlight = false;
        this.observerReachability = {
            status: 'UNKNOWN',
            hopCount: null,
            lastKnownHopCount: null,
            observedAt: new Date().toISOString(),
        };
        this.bridgeUpstreamHopCount = null;
        this.observerHealthMonitoringActive = false;
        this.observerHealthCheckInFlight = false;
        this.activeObserverHealthCheckCorrelationId = null;
        this.observerReachabilityChangeHandler = null;
        this.brokerSession = new BrokerSession({
            brokerUrl,
            sessionId,
            clientId,
            logConnectionEvents: false,
            handleIncomingMessage: this.handleIncomingMessage.bind(this),
            onUnexpectedClose: this.handleUnexpectedControlSessionClose.bind(this),
        });

        this.initializationPromise = this.initialize();
    }

    createTopicHandlers() {
        return [
            {
                topic: 'check/observer/status',
                handler: this.handleObserverReachabilityStatusRequest.bind(this),
            },
            {
                topic: 'observer/status/response',
                handler: this.handleObserverReachabilityResponse.bind(this),
            },
            {
                topic: buildNodeInboxTopic(this.brokerId, this.brokerId),
                handler: this.handleManagedEntityJoinRequest.bind(this),
            },
            {
                topic: buildNodeInboxTopic(this.brokerId, this.sessionId),
                handler: this.handleBrokerGroupCheckResponse.bind(this),
            },
        ];
    }

    async initialize() {
        try {
            await this.brokerSession.connect();

            for (const { topic, handler } of this.topicHandlers) {
                await this.brokerSession.subscribe(topic, handler);
            }

            await this.startObserverHealthMonitoring();
            this.startManagedEntityHealthMonitoring();
            return this;
        } catch (error) {
            await this.destroy();
            throw error;
        }
    }

    async handleIncomingMessage(sessionId, topic, rawMessage) {
        const topicHandler = this.topicHandlers.find((entry) =>
            this.brokerSession.matchesTopicFilter(entry.topic, topic),
        );

        if (!topicHandler) {
            return;
        }

        const message = await this.decodeEncryptedControlMessage(topic, rawMessage);
        await topicHandler.handler({ sessionId, topic, message });
    }

    async publishEncryptedControlMessage(message) {
        const topic = message?.target?.topic;

        if (typeof topic !== 'string' || topic.trim() === '') {
            throw new Error('Control message requires target.topic');
        }

        if (this.isMessageSecurityEnabled()) {
            const encryptedMessage = await this.securityManager.encryptMessage(
                message,
                this.messageSecurityKeyId,
            );
            return this.brokerSession.publish(topic.trim(), encryptedMessage);
        }

        return this.brokerSession.publish(topic.trim(), message);
    }

    async decodeEncryptedControlMessage(receivedTopic, rawMessage) {
        if (!this.isMessageSecurityEnabled()) {
            return Message.fromJSON(JSON.parse(rawMessage.toString()));
        }

        const encryptedMessage = EncryptedMessage.fromJSON(
            JSON.parse(rawMessage.toString()),
        );

        return this.securityManager.decodeMessage(encryptedMessage, receivedTopic);
    }

    isMessageSecurityEnabled() {
        return this.securityManager !== null && this.messageSecurityKeyId !== null;
    }

    async startObserverHealthMonitoring() {
        if (this.observerHealthMonitoringActive) {
            return { ok: true, alreadyRunning: true };
        }

        this.observerHealthMonitoringActive = true;
        return this.checkObserverReachability();
    }

    stopObserverHealthMonitoring() {
        this.observerHealthMonitoringActive = false;
        return { ok: true };
    }

    setObserverReachabilityChangeHandler(handler) {
        if (handler !== null && typeof handler !== 'function') {
            throw new TypeError('handler must be a function or null');
        }

        this.observerReachabilityChangeHandler = handler;
        return { ok: true, statusReport: this.getStatusReport() };
    }

    notifyObserverReachabilityChanged() {
        if (typeof this.observerReachabilityChangeHandler !== 'function') {
            return { ok: true, notified: false };
        }

        try {
            this.observerReachabilityChangeHandler(this.getStatusReport());
            return { ok: true, notified: true };
        } catch (error) {
            return { ok: false, notified: false, error };
        }
    }

    registerManagedEntity(nodeId) {
        if (typeof nodeId !== 'string' || nodeId.trim() === '') {
            return { ok: false, error: 'nodeId must be a non-empty string' };
        }

        const normalizedNodeId = nodeId.trim();
        const alreadyRegistered = this.managedEntityRegistry.nodeIds.has(normalizedNodeId);
        this.managedEntityRegistry.nodeIds.add(normalizedNodeId);

        return { ok: true, alreadyRegistered, nodeId: normalizedNodeId };
    }

    unregisterManagedEntity(nodeId) {
        if (typeof nodeId !== 'string' || nodeId.trim() === '') {
            return { ok: false, error: 'nodeId must be a non-empty string' };
        }

        const normalizedNodeId = nodeId.trim();
        const removed = this.managedEntityRegistry.nodeIds.delete(normalizedNodeId);

        return { ok: true, removed, nodeId: normalizedNodeId };
    }

    getManagedEntityNodeIds() {
        return Array.from(this.managedEntityRegistry.nodeIds);
    }

    clearManagedEntities() {
        const removedNodeIds = this.getManagedEntityNodeIds();
        this.managedEntityRegistry.nodeIds = new Set();
        this.managedEntityRegistry.lastValidatedAtMs = null;

        return { ok: true, removedNodeIds };
    }

    enqueueManagedEntityJoin(joinOperation) {
        const resultPromise = this.managedEntityJoinQueue.then(joinOperation, joinOperation);
        this.managedEntityJoinQueue = resultPromise.catch(() => undefined);
        return resultPromise;
    }

    needToRefreshManagedEntityRegistry() {
        const lastValidatedAtMs = this.managedEntityRegistry.lastValidatedAtMs;

        return !(
            typeof lastValidatedAtMs === 'number' &&
            Date.now() >= lastValidatedAtMs &&
            Date.now() - lastValidatedAtMs < this.managedEntityRegistryValidityMs
        );
    }

    canAcceptManagedEntity(nodeId) {
        if (typeof nodeId !== 'string' || nodeId.trim() === '') {
            return {
                ok: false,
                accepted: false,
                error: 'nodeId must be a non-empty string',
            };
        }

        const normalizedNodeId = nodeId.trim();
        const alreadyRegistered = this.managedEntityRegistry.nodeIds.has(normalizedNodeId);
        const accepted = alreadyRegistered ||
            this.managedEntityRegistry.nodeIds.size < this.maximumManagedEntityCount;

        return {
            ok: true,
            accepted,
            alreadyRegistered,
            managedEntityCount: this.managedEntityRegistry.nodeIds.size,
            maximumManagedEntityCount: this.maximumManagedEntityCount,
        };
    }

    applyManagedEntityJoin(nodeId) {
        const admissionResult = this.canAcceptManagedEntity(nodeId);

        if (admissionResult.accepted) {
            this.registerManagedEntity(nodeId.trim());
        }

        return {
            ...admissionResult,
            managedEntityCount: this.managedEntityRegistry.nodeIds.size,
        };
    }

    buildManagedEntityGroupCheckResult(pendingRequest) {
        const expectedNodeIds = pendingRequest?.metadata?.expectedNodeIds;
        const respondedNodeIds = pendingRequest?.metadata?.respondedNodeIds;

        if (!(expectedNodeIds instanceof Set) || !(respondedNodeIds instanceof Set)) {
            return { ok: false, error: 'Managed entity group check metadata is invalid' };
        }

        return {
            ok: true,
            correlationId: pendingRequest.correlationId,
            reachableNodeIds: Array.from(respondedNodeIds),
            unreachableNodeIds: Array.from(expectedNodeIds).filter(nodeId => !respondedNodeIds.has(nodeId)),
        };
    }

    async refreshManagedEntityRegistry() {
        if (this.managedEntityRegistryRefreshInFlight) {
            return { ok: true, skipped: true, reason: 'GROUP_CHECK_IN_PROGRESS' };
        }

        const expectedNodeIds = new Set(this.getManagedEntityNodeIds());

        if (expectedNodeIds.size === 0) {
            this.managedEntityRegistry.lastValidatedAtMs = Date.now();
            return {
                ok: true,
                skipped: true,
                reachableNodeIds: [this.brokerId],
                unreachableNodeIds: [],
            };
        }

        const correlationId = randomUUID();
        this.managedEntityRegistryRefreshInFlight = true;

        try {
            const groupCheckPromise = this.pendingRequestRegistry.register({
                correlationId,
                operationType: MESSAGE_OPERATIONS.BROKER_GROUP_CHECK_REQUEST,
                timeoutMs: this.managedEntityCheckTimeoutMs,
                metadata: { expectedNodeIds, respondedNodeIds: new Set() },
                onTimeout: pendingRequest => this.buildManagedEntityGroupCheckResult(pendingRequest),
            });
            const groupCheckTopic = buildBrokerGroupCheckRequestTopic(this.brokerId);
            const publishResult = await this.publishEncryptedControlMessage(
                new Message(
                    { nodeId: this.brokerId },
                    { topic: groupCheckTopic },
                    { replyTo: buildNodeInboxTopic(this.brokerId, this.sessionId) },
                    MESSAGE_TYPES.EVENT,
                    LOCAL_SYNC_STATE.SYNCED,
                    MESSAGE_OPERATIONS.BROKER_GROUP_CHECK_REQUEST,
                    true,
                    correlationId,
                ),
            );

            if (publishResult?.ok === false) {
                this.pendingRequestRegistry.clear(correlationId, {
                    ok: false,
                    error: 'Managed entity group check could not be published',
                });
            }

            const checkResult = await groupCheckPromise;

            if (!checkResult.ok) {
                return {
                    ok: false,
                    error: checkResult.error ?? 'Managed entity group check failed',
                    publishResult,
                    registeredNodeIds: this.getManagedEntityNodeIds(),
                };
            }

            for (const nodeId of checkResult.unreachableNodeIds) {
                this.managedEntityRegistry.nodeIds.delete(nodeId);
            }
            this.managedEntityRegistry.lastValidatedAtMs = Date.now();

            return {
                ...checkResult,
                reachableNodeIds: [this.brokerId, ...checkResult.reachableNodeIds],
                publishResult,
                registeredNodeIds: this.getManagedEntityNodeIds(),
            };
        } catch (error) {
            return { ok: false, error: error?.message ?? String(error) };
        } finally {
            this.managedEntityRegistryRefreshInFlight = false;
        }
    }

    startManagedEntityHealthMonitoring() {
        if (this.managedEntityHealthMonitoringTimer) {
            return { ok: true, alreadyRunning: true };
        }

        this.managedEntityHealthMonitoringTimer = setInterval(() => {
            this.refreshManagedEntityRegistry().catch(error => {
                logRuntimeError('Error refreshing managed entity registry:', error);
            });
        }, this.managedEntityRegistryValidityMs);

        return {
            ok: true,
            started: true,
            intervalMs: this.managedEntityRegistryValidityMs,
        };
    }

    stopManagedEntityHealthMonitoring() {
        if (!this.managedEntityHealthMonitoringTimer) {
            return { ok: true, alreadyStopped: true };
        }

        clearInterval(this.managedEntityHealthMonitoringTimer);
        this.managedEntityHealthMonitoringTimer = null;
        return { ok: true, stopped: true };
    }

    async checkObserverReachability() {
        if (this.observerHealthCheckInFlight) {
            return {
                ok: true,
                skipped: true,
            };
        }

        const correlationId = randomUUID();
        const healthCheckResult = this.pendingRequestRegistry.register({
            correlationId,
            operationType: MESSAGE_OPERATIONS.SUBSCRIBERS_REACHABILITY_CHECK_REQUEST,
            metadata: { observerIds: [] },
            onTimeout: pendingRequest => {
                const observerIds = Array.isArray(pendingRequest?.metadata?.observerIds)
                    ? pendingRequest.metadata.observerIds
                    : [];

                return { ok: true, observerIds };
            },
        });
        this.activeObserverHealthCheckCorrelationId = correlationId;
        this.observerHealthCheckInFlight = true;

        healthCheckResult
            .then(result => this.handleObserverHealthCheckCompletion(correlationId, result))
            .catch(error => {
                logRuntimeError('Error processing observer health check:', error);
            });

        const heartbeat = new Message(
            { nodeId: this.brokerId },
            { topic: 'broker/heartbeat' },
            null,
            MESSAGE_TYPES.EVENT,
            LOCAL_SYNC_STATE.SYNCED,
            MESSAGE_OPERATIONS.SUBSCRIBERS_REACHABILITY_CHECK_REQUEST,
            true,
            correlationId,
        );

        try {
            await this.publishEncryptedControlMessage(heartbeat);
            return { ok: true, correlationId };
        } catch (error) {
            this.stopObserverHealthMonitoring();
            this.pendingRequestRegistry.clear(correlationId, {
                ok: false,
                error: error?.message ?? String(error),
            });
            this.activeObserverHealthCheckCorrelationId = null;
            this.observerHealthCheckInFlight = false;
            throw error;
        }
    }

    async handleObserverHealthCheckCompletion(correlationId, result) {
        if (correlationId !== this.activeObserverHealthCheckCorrelationId) {
            return {
                ok: true,
                skipped: true,
            };
        }

        this.observerHealthCheckInFlight = false;
        this.activeObserverHealthCheckCorrelationId = null;

        if (!this.observerHealthMonitoringActive) {
            return { ok: true, skipped: true };
        }

        try {
            if (result?.ok !== true) {
                return {
                    ok: false,
                    error: result?.error ?? 'Observer health check did not complete',
                };
            }

            const reachabilityResult = await this.updateObserverReachabilityStatus(
                result.observerIds,
            );
            logRuntimeEvent('[' + this.brokerId + '] Resultado de monitorizacion', {
                status: reachabilityResult.status,
                observerIds: reachabilityResult.observerIds,
            });
            return reachabilityResult;
        } catch (error) {
            logRuntimeError('Error updating observer reachability:', error);
            return { ok: false, error: error?.message ?? String(error) };
        } finally {
            if (this.observerHealthMonitoringActive) {
                try {
                    await this.checkObserverReachability();
                } catch (error) {
                    logRuntimeError('Error starting next observer health check:', error);
                }
            }
        }
    }

    async updateObserverReachabilityStatus(observerIds = []) {
        const confirmedObserverIds = Array.isArray(observerIds) ? observerIds : [];
        const status = confirmedObserverIds.length > 0 ? 'REACHABLE' : 'UNREACHABLE';
        const hopCount = status === 'REACHABLE'
            ? (this.bridgeUpstreamHopCount ?? 0) + 1
            : null;
        const lastKnownHopCount = hopCount ?? this.observerReachability.lastKnownHopCount;
        const observedAt = new Date().toISOString();
        const statusChanged =
            this.observerReachability.status !== status ||
            this.observerReachability.hopCount !== hopCount;

        this.observerReachability = {
            status,
            hopCount,
            lastKnownHopCount,
            observedAt,
        };

        let notificationResult = { ok: true, skipped: !statusChanged };
        const stateChangeResult = statusChanged
            ? this.notifyObserverReachabilityChanged()
            : { ok: true, skipped: true };

        if (statusChanged) {
            notificationResult = await this.broadcastManagerStatusChanged();
        }

        return {
            ok: notificationResult.ok === true && stateChangeResult.ok === true,
            status,
            observedAt,
            observerIds: confirmedObserverIds,
            statusChanged,
            notificationResult,
            stateChangeResult,
        };
    }

    async setBridgeUpstreamHopCount(upstreamHopCount) {
        if (
            upstreamHopCount !== null &&
            (!Number.isInteger(upstreamHopCount) || upstreamHopCount <= 0)
        ) {
            return {
                ok: false,
                error: 'upstreamHopCount must be null or a positive integer',
            };
        }

        const changed = this.bridgeUpstreamHopCount !== upstreamHopCount;
        this.bridgeUpstreamHopCount = upstreamHopCount;

        return { ok: true, changed, upstreamHopCount };
    }

    async invalidateObserverReachability() {
        const previousReachability = this.observerReachability;

        this.observerReachability = {
            status: 'UNKNOWN',
            hopCount: null,
            lastKnownHopCount: previousReachability.lastKnownHopCount,
            observedAt: new Date().toISOString(),
        };

        const changed =
            previousReachability.status !== 'UNKNOWN' ||
            previousReachability.hopCount !== null;
        const stateChangeResult = changed
            ? this.notifyObserverReachabilityChanged()
            : { ok: true, skipped: true };
        const notificationResult = changed
            ? await this.broadcastManagerStatusChanged()
            : { ok: true, skipped: true };

        return {
            ok: notificationResult.ok === true && stateChangeResult.ok === true,
            changed,
            notificationResult,
            stateChangeResult,
        };
    }

    buildStatusReport(requestingNodeId = null) {
        const admissionResult = typeof requestingNodeId === 'string' && requestingNodeId.trim() !== ''
            ? this.canAcceptManagedEntity(requestingNodeId)
            : {
                accepted: this.managedEntityRegistry.nodeIds.size < this.maximumManagedEntityCount,
                managedEntityCount: this.managedEntityRegistry.nodeIds.size,
                maximumManagedEntityCount: this.maximumManagedEntityCount,
            };

        return {
            brokerId: this.brokerId,
            brokerType: this.brokerType,
            status: this.observerReachability.status,
            hopCount: this.observerReachability.hopCount,
            lastKnownHopCount: this.observerReachability.lastKnownHopCount,
            observedAt: this.observerReachability.observedAt,
            metrics: {
                batteryPct: this.getBatteryPct(),
            },
            admissionAccepted: admissionResult.accepted === true,
            managedEntityCount: admissionResult.managedEntityCount,
            maximumManagedEntityCount: admissionResult.maximumManagedEntityCount,
        };
    }

    getStatusReport() {
        return this.buildStatusReport();
    }

    getBatteryPct() {
        return this.batteryPct;
    }

    async handleObserverReachabilityStatusRequest({ message }) {
        const nodeId = message.source?.nodeId;
        const correlationId = message.getCorrelationId();

        if (typeof nodeId !== 'string' || nodeId.trim() === '') {
            return { ok: false, error: 'Status request has no source nodeId' };
        }

        if (typeof correlationId !== 'string' || correlationId.trim() === '') {
            return { ok: false, error: 'Status request has no correlationId' };
        }

        const registryRefreshResult = this.needToRefreshManagedEntityRegistry()
            ? await this.refreshManagedEntityRegistry()
            : { ok: true, skipped: true };
        const statusReport = this.buildStatusReport(nodeId);

        if (registryRefreshResult.ok !== true) {
            statusReport.admissionAccepted = false;
        }

        const responseTopic = `${nodeId}/response`;
        const response = new Message(
            { nodeId: this.brokerId },
            { nodeId, topic: responseTopic },
            statusReport,
            MESSAGE_TYPES.RESPONSE,
            LOCAL_SYNC_STATE.SYNCED,
            MESSAGE_OPERATIONS.PROBE_STATUS_RESPONSE,
            false,
            correlationId,
        );

        return this.publishEncryptedControlMessage(response);
    }

    handleObserverReachabilityResponse({ message }) {
        const correlationId = message.getCorrelationId();
        const observerId = message.getPayload()?.observerId;

        if (typeof correlationId !== 'string' || correlationId.trim() === '') {
            return { ok: false, error: 'Observer response has no correlationId' };
        }

        if (typeof observerId !== 'string' || observerId.trim() === '') {
            return { ok: false, error: 'Observer response has no observerId' };
        }

        if (correlationId !== this.activeObserverHealthCheckCorrelationId) {
            return {
                ok: true,
                skipped: true,
            };
        }

        const pending = this.pendingRequestRegistry.get(correlationId);

        if (pending?.operationType !==
            MESSAGE_OPERATIONS.SUBSCRIBERS_REACHABILITY_CHECK_REQUEST) {
            return {
                ok: true,
                skipped: true,
            };
        }

        const observerIds = Array.isArray(pending.metadata.observerIds)
            ? pending.metadata.observerIds
            : [];
        const normalizedObserverId = observerId.trim();

        if (!observerIds.includes(normalizedObserverId)) {
            this.pendingRequestRegistry.updateMetadata(correlationId, {
                observerIds: [...observerIds, normalizedObserverId],
            });
        }

        return { ok: true, correlationId, observerId: normalizedObserverId };
    }

    async handleManagedEntityJoinRequest({ message }) {
        if (message.operation !== MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_REQUEST) {
            return { ok: true, ignored: true };
        }

        const correlationId = message.getCorrelationId();
        const sourceNodeId = message.source?.nodeId;
        const replyTo = message.getPayload()?.replyTo;

        if (typeof correlationId !== 'string' || correlationId.trim() === '') {
            return { ok: false, error: 'Managed entity join request has no correlationId' };
        }

        if (typeof sourceNodeId !== 'string' || sourceNodeId.trim() === '') {
            return { ok: false, error: 'Managed entity join request has no source nodeId' };
        }

        if (typeof replyTo !== 'string' || replyTo.trim() === '') {
            return { ok: false, error: 'Managed entity join request has no replyTo' };
        }

        const joinResult = await this.enqueueManagedEntityJoin(async () =>
            this.applyManagedEntityJoin(sourceNodeId),
        );
        const responseTopic = replyTo.trim();
        const response = new Message(
            { nodeId: this.brokerId },
            { nodeId: sourceNodeId.trim(), topic: responseTopic },
            { accepted: joinResult.accepted === true },
            MESSAGE_TYPES.RESPONSE,
            LOCAL_SYNC_STATE.SYNCED,
            MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_RESPONSE,
            false,
            correlationId,
        );

        return this.publishEncryptedControlMessage(response);
    }

    handleBrokerGroupCheckResponse({ message }) {
        if (message.operation !== MESSAGE_OPERATIONS.BROKER_GROUP_CHECK_RESPONSE) {
            return { ok: true, ignored: true };
        }

        const correlationId = message.getCorrelationId();
        const sourceNodeId = message.source?.nodeId;
        const pendingRequest = this.pendingRequestRegistry.get(correlationId);

        if (!pendingRequest ||
            pendingRequest.operationType !== MESSAGE_OPERATIONS.BROKER_GROUP_CHECK_REQUEST) {
            return { ok: false, error: `No pending managed entity group check for ${correlationId}` };
        }

        if (typeof sourceNodeId !== 'string' || sourceNodeId.trim() === '') {
            return { ok: false, error: 'Managed entity group check response has no source nodeId' };
        }

        const normalizedNodeId = sourceNodeId.trim();
        const expectedNodeIds = pendingRequest.metadata.expectedNodeIds;
        const respondedNodeIds = pendingRequest.metadata.respondedNodeIds;

        if (!(expectedNodeIds instanceof Set) || !(respondedNodeIds instanceof Set)) {
            return { ok: false, error: 'Managed entity group check metadata is invalid' };
        }

        if (!expectedNodeIds.has(normalizedNodeId)) {
            return { ok: false, error: `Unexpected managed entity '${normalizedNodeId}'` };
        }

        const nextRespondedNodeIds = new Set(respondedNodeIds);
        nextRespondedNodeIds.add(normalizedNodeId);
        this.pendingRequestRegistry.updateMetadata(correlationId, {
            respondedNodeIds: nextRespondedNodeIds,
        });

        if (nextRespondedNodeIds.size === expectedNodeIds.size) {
            const checkResult = this.buildManagedEntityGroupCheckResult({
                ...pendingRequest,
                metadata: {
                    ...pendingRequest.metadata,
                    respondedNodeIds: nextRespondedNodeIds,
                },
            });
            this.pendingRequestRegistry.resolve(correlationId, checkResult);
            return checkResult;
        }

        return { ok: true, waitingForResponses: true };
    }

    async broadcastManagerStatusChanged() {
        const notification = new Message(
            { nodeId: this.brokerId },
            { topic: 'notify/observer/status' },
            this.buildStatusReport(),
            MESSAGE_TYPES.EVENT,
            LOCAL_SYNC_STATE.SYNCED,
            MESSAGE_OPERATIONS.MANAGER_STATUS_CHANGED,
        );

        return this.publishEncryptedControlMessage(notification);
    }

    handleUnexpectedControlSessionClose() {
        logRuntimeError('[' + this.brokerId + '] Sesión de control cerrada inesperadamente');
        this.stopObserverHealthMonitoring();
        this.stopManagedEntityHealthMonitoring();
        this.pendingRequestRegistry.clearAll({
            ok: false,
            error: 'Control session closed unexpectedly',
        });
        this.observerHealthCheckInFlight = false;
        this.activeObserverHealthCheckCorrelationId = null;
    }

    async destroy() {
        this.stopObserverHealthMonitoring();
        this.stopManagedEntityHealthMonitoring();
        this.pendingRequestRegistry.clearAll({
            ok: false,
            error: 'Control service destroyed',
        });
        this.observerHealthCheckInFlight = false;
        this.activeObserverHealthCheckCorrelationId = null;
        return this.brokerSession.destroy();
    }
}

export { ControlService };
