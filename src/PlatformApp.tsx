import React, { useEffect, useRef, useState } from 'react';
import {
  ActivityIndicator,
  Pressable,
  SafeAreaView,
  ScrollView,
  StyleSheet,
  Switch,
  Text,
  TextInput,
  View,
} from 'react-native';
import {
  LOCAL_SYNC_STATE,
  Message,
} from './middleware/Message';
import { EncryptedMessage } from './middleware/EncryptedMessage';
import {
  PARTITION_REPLICA_TOPIC,
  PlatformAdapter,
} from './middleware/PlatformAdapter';
import { BROKER_ROLE } from './middleware/BrokerContracts';
import { MessageManager } from './middleware/ReactNativeMessageManager';
import { RUNTIME_PROFILE } from './middleware/RuntimeProfiles';
import { SessionManager } from './middleware/SessionManager';
import { TermuxMosquittoManager } from './middleware/TermuxMosquittoManager';
import { requestTermuxRunCommandPermission } from './termux/TermuxCommandRunner';
import { loadHarnessTopology } from './harness/topologyStore';
import type { HarnessTopologyV1 } from './harness/types';
import {
  MESSAGE_SECURITY_KEY_ID,
  prepareMessageSecurity,
  type MessageSecurityState,
} from './messageSecurity';

type ViewName =
  | 'OPERATION'
  | 'LOCAL_BROKER'
  | 'MESSAGES'
  | 'SECURITY'
  | 'REEVALUATION'
  | 'RESYNC';
type ActionName = 'OPERATIONAL' | 'PUBLISH' | 'SUBSCRIBE';
type FilterName = 'PENDING' | 'ALL' | 'SYNCED';
type RuntimeProfile = 'PRIMARY_BROKER' | 'BASE_TEMPORARY' | 'BASE';
type ConfiguredBroker = {
  brokerId: string;
  brokerUrl: string;
  brokerType: string;
  ip: string;
  bridgePort: number | null;
};
type LocalBrokerStatus = {
  environmentStatus: string;
  brokerProcessStatus: string;
  usable: boolean;
  endpoint?: {
    brokerUrl?: string;
    protocol?: string;
    host?: string;
    ip?: string;
    port?: number;
  } | null;
};
type StoredMessage = {
  origin: string;
  encryptedMessage?: {
    messageId: string;
    source: { nodeId?: string | null } | null;
    targetTopic: string;
    syncState: string;
    status: string;
    encryptedContent: {
      keyId: string;
      nonce: string;
      ciphertext: string;
      tag: string;
    };
  };
  message?: {
    messageId: string;
    target: { topic?: string | null } | null;
    syncState: string;
    status: string;
  };
};
type IncomingMessage = {
  messageId: string;
  receivedAt: string;
  encryptedMessage: StoredMessage['encryptedMessage'] | null;
  decodedContent: unknown;
  topic: string | null;
};
type DecryptedMessageEvidence = {
  payload: unknown;
  sourceNodeId: string | null;
  timestamp: string | null;
};

function storedMessageId(entry: StoredMessage) {
  return entry.encryptedMessage?.messageId ?? entry.message?.messageId ?? '';
}

function storedMessageSyncState(entry: StoredMessage) {
  return entry.encryptedMessage?.syncState ?? entry.message?.syncState ?? '';
}

function storedMessageStatus(entry: StoredMessage) {
  return entry.encryptedMessage?.status ?? entry.message?.status ?? '';
}

function storedMessageTopic(entry: StoredMessage) {
  return entry.encryptedMessage?.targetTopic ?? entry.message?.target?.topic ?? '';
}
type TlsReadiness = {
  ok: boolean;
  configured: boolean;
  prepared: boolean;
  endpoint?: { brokerUrl?: string } | null;
  mqttsEndpoint?: { brokerUrl?: string } | null;
  ca?: { available?: boolean } | null;
  certificate?: {
    available?: boolean;
    subject?: string | null;
    issuer?: string | null;
    expiresAt?: string | null;
    sha256Fingerprint?: string | null;
    inspectionError?: string | null;
  } | null;
  certificateCurrent?: boolean;
  privateKey?: { available?: boolean } | null;
  bridges?: { brokerId: string; brokerUrl: string; tlsProtected: boolean }[];
  reason?: string;
  error?: { message?: string } | string;
};
type Report = {
  brokerId: string;
  brokerType: string;
  status: string;
  hopCount?: number | null;
  observedAt: string;
  sessionReachable: boolean;
  admissionAccepted?: boolean;
  managedEntityCount?: number;
  maximumManagedEntityCount?: number;
  metrics?: { batteryPct?: number };
  latencyMs?: number | null;
  selectionScore?: number;
};
type ReportResult = {
  brokerId: string;
  sessionReachable: boolean;
  report?: Report;
};
type AdaptationAction = {
  type?: string;
  ok: boolean;
  simulated?: boolean;
  skipped?: boolean;
  brokerId?: string | null;
};
type AdaptationSummary = {
  actions: AdaptationAction[];
  finalState?: {
    brokerId?: string | null;
    mode?: string | null;
    sessionId?: string | null;
    localBrokerActive?: boolean;
    managedBridgeState?: ManagedBridgeState | null;
  };
};
type ManagedBridgeState = {
  configured: boolean;
  upstreamBrokerId: string;
  upstreamHopCount: number;
  upstreamEndpoint: string;
  configuredAt: string;
};
type ResyncBatch = {
  status: 'EN_CURSO' | 'COMPLETADA' | 'FALLIDA';
  correlationId: string;
  sessionId: string;
  requestedMessageIds: string[];
  receivedMessageIds?: string[];
  startedAt: string;
  completedAt?: string;
};
type PendingSynchronizationResult = {
  ok: boolean;
  sessionId: string;
  targetMode: string;
  remainingPendingMessageCount: number;
  completedAt: string;
  processingResult?: {
    attemptedMessageCount: number;
    batchResults: ResyncBatch[];
  };
};
type ReevaluationState = {
  lastResult: {
    completedAt: string;
    collectionResult?: { reportResults: ReportResult[] };
    snapshot?: { orderedBrokers: Report[] };
    adaptationSummary?: AdaptationSummary;
  } | null;
  currentOperationalSessionId?: string | null;
  modeState?: {
    currentBrokerNodeId?: string | null;
    currentMode?: string | null;
  };
  reevaluationInFlight: boolean;
  periodicReevaluationActive: boolean;
  pendingSynchronizationInFlight?: boolean;
  periodicPendingSynchronizationActive?: boolean;
  currentPendingSynchronizationBatch?: ResyncBatch | null;
  lastPendingSynchronizationResult?: PendingSynchronizationResult | null;
  bridgeCapable?: boolean;
  managedBridgeState?: ManagedBridgeState | null;
  localBrokerControlReport?: Report | null;
};

const RUNTIME_PROFILE_LABEL: Record<RuntimeProfile, string> = {
  PRIMARY_BROKER: 'Gestor principal',
  BASE_TEMPORARY: 'Nodo base con gestor temporal',
  BASE: 'Nodo base',
};

function buildConfiguredBrokers(topology: HarnessTopologyV1): ConfiguredBroker[] {
  return [
    ...topology.primaryBrokers.map(broker => ({
      brokerId: broker.nodeId,
      brokerUrl: `wss://${broker.ip}:${broker.port}`,
      brokerType: BROKER_ROLE.PRIMARY,
      ip: broker.ip,
      bridgePort: broker.bridgePort ?? null,
    })),
    ...topology.temporaryBrokers.map(broker => ({
      brokerId: broker.nodeId,
      brokerUrl: `wss://${broker.ip}:${broker.port}`,
      brokerType: BROKER_ROLE.TEMPORARY,
      ip: broker.ip,
      bridgePort: broker.bridgePort ?? null,
    })),
  ];
}

function toLocalBrokerViewState(status: LocalBrokerStatus) {
  if (
    status.environmentStatus === 'AVAILABLE' &&
    status.brokerProcessStatus === 'RUNNING' &&
    status.usable
  ) {
    return 'OPERATIVO';
  }

  if (
    status.environmentStatus === 'AVAILABLE' &&
    status.brokerProcessStatus === 'STOPPED'
  ) {
    return 'DETENIDO';
  }

  return status.environmentStatus === 'UNAVAILABLE'
    ? 'NO DISPONIBLE'
    : 'ERROR';
}
function formatPayload(payload: unknown) {
  if (typeof payload === 'string') return payload;
  try {
    return JSON.stringify(payload);
  } catch {
    return String(payload);
  }
}

function formatDate(value: string | undefined) {
  if (!value) return 'Sin reevaluaciones';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function managerType(type: string) {
  return type === 'PRIMARY' ? 'PRIMARIO' : 'TEMPORAL';
}

function formatBatteryPct(value: number | undefined) {
  return typeof value === 'number' ? `${value}%` : 'SIN DATOS';
}

function formatLatencyMs(value: number | null | undefined) {
  return typeof value === 'number' ? `${value} ms` : 'SIN DATOS';
}

function formatSelectionScore(value: number | undefined) {
  return typeof value === 'number' ? value.toFixed(3) : 'SIN DATOS';
}

function formatHopCount(value: number | null | undefined) {
  return typeof value === 'number' ? String(value) : 'SIN ALCANCE';
}

function formatBridgeRouteState(
  bridgeState: ManagedBridgeState | null,
  localBrokerReport: Report | undefined,
) {
  if (!bridgeState?.configured) return 'NO CONFIGURADO';
  if (localBrokerReport?.status === 'UNKNOWN') return 'SIN CONFIRMAR';
  if (localBrokerReport?.status !== 'REACHABLE') return 'NO DISPONIBLE';

  return localBrokerReport.hopCount === bridgeState.upstreamHopCount + 1
    ? 'OPERATIVA'
    : 'SIN CONFIRMAR';
}

function formatAdmissionStatus(admissionAccepted: boolean | undefined) {
  if (admissionAccepted === true) return 'DISPONIBLE';
  if (admissionAccepted === false) return 'SIN CAPACIDAD';
  return 'SIN DATOS';
}

function formatAdmissionOccupancy(
  managedEntityCount: number | undefined,
  maximumManagedEntityCount: number | undefined,
) {
  if (
    typeof managedEntityCount !== 'number' ||
    typeof maximumManagedEntityCount !== 'number'
  ) {
    return 'SIN DATOS';
  }

  return `${managedEntityCount} / ${maximumManagedEntityCount}`;
}

function formatAvailableAdmissionCapacity(
  managedEntityCount: number | undefined,
  maximumManagedEntityCount: number | undefined,
) {
  if (
    typeof managedEntityCount !== 'number' ||
    typeof maximumManagedEntityCount !== 'number'
  ) {
    return 'SIN DATOS';
  }

  return String(Math.max(0, maximumManagedEntityCount - managedEntityCount));
}

function isUsableManager(manager: Report) {
  return (
    manager.sessionReachable === true &&
    manager.status === 'REACHABLE' &&
    manager.admissionAccepted === true
  );
}

function isPartitionTopic(topic: string | null | undefined) {
  return topic === PARTITION_REPLICA_TOPIC;
}

function adaptationActionTitle(action: AdaptationAction) {
  if (action.brokerId) {
    if (action.brokerId.startsWith('local-')) {
      return 'Conectar con gestor local';
    }

    return 'Conectar con gestor remoto';
  }

  const includesSkippedField = Object.prototype.hasOwnProperty.call(
    action,
    'skipped',
  );

  if (action.simulated && includesSkippedField) {
    return 'Activar gestor local';
  }

  if (action.simulated) {
    return 'Desactivar gestor local';
  }

  return 'Pasar a modo OFFLINE';
}

function formatAdaptationAction(action: AdaptationAction) {
  const title = adaptationActionTitle(action);
  const broker = action.brokerId ? ` (${action.brokerId})` : '';
  const outcome = action.skipped
    ? 'OMITIDA'
    : action.ok
    ? 'APLICADA'
    : 'FALLIDA';
  const simulation = action.simulated ? ' - SIMULADA' : '';

  return `${title}${broker} - ${outcome}${simulation}`;
}

export default function PlatformApp() {
  const sessionManagerRef = useRef<SessionManager | null>(null);
  const adapterRef = useRef<PlatformAdapter | null>(null);
  const localBrokerManagerRef = useRef<TermuxMosquittoManager | null>(null);
  const removeLocalBrokerStatusObserverRef = useRef<(() => boolean) | null>(
    null,
  );
  const mountedRef = useRef(true);
  const [nodeId, setNodeId] = useState('node-1');
  const [primaryManagerId, setPrimaryManagerId] = useState('broker-1');
  const [runtimeProfile, setRuntimeProfile] = useState<RuntimeProfile | null>(
    null,
  );
  const [bridgeCapable, setBridgeCapable] = useState(false);
  const [topology, setTopology] = useState<HarnessTopologyV1 | null>(null);
  const [configuredBrokers, setConfiguredBrokers] = useState<
    ConfiguredBroker[]
  >([]);
  const [topologyReady, setTopologyReady] = useState(false);
  const [localBrokerState, setLocalBrokerState] = useState('NO APLICA');
  const [localBrokerStatus, setLocalBrokerStatus] =
    useState<LocalBrokerStatus | null>(null);
  const [operationalSessionId, setOperationalSessionId] = useState<string | null>(
    null,
  );
  const [storageReady, setStorageReady] = useState(false);
  const [securityState, setSecurityState] = useState<MessageSecurityState | null>(null);
  const [tlsReadiness, setTlsReadiness] = useState<TlsReadiness | null>(null);
  const [tlsConnectionVerified, setTlsConnectionVerified] = useState(false);
  const [starting, setStarting] = useState(false);
  const [started, setStarted] = useState(false);
  const [view, setView] = useState<ViewName>('OPERATION');
  const [action, setAction] = useState<ActionName>('OPERATIONAL');
  const [filter, setFilter] = useState<FilterName>('PENDING');
  const [content, setContent] = useState('Informacion operativa de prueba');
  const [manualTopic, setManualTopic] = useState('');
  const [manualContent, setManualContent] = useState('');
  const [subscriptionTopic, setSubscriptionTopic] = useState('');
  const [storedMessages, setStoredMessages] = useState<StoredMessage[]>([]);
  const [decryptedStoredMessages, setDecryptedStoredMessages] = useState<
    Record<string, DecryptedMessageEvidence>
  >({});
  const [incomingMessages, setIncomingMessages] = useState<IncomingMessage[]>(
    [],
  );
  const [observerStatus, setObserverStatus] = useState('UNKNOWN');
  const [reevaluationState, setReevaluationState] = useState<ReevaluationState>(
    {
      lastResult: null,
      reevaluationInFlight: false,
      periodicReevaluationActive: false,
      pendingSynchronizationInFlight: false,
      periodicPendingSynchronizationActive: false,
      currentPendingSynchronizationBatch: null,
      lastPendingSynchronizationResult: null,
    },
  );
  const [reportsVisible, setReportsVisible] = useState(false);
  const [adaptationVisible, setAdaptationVisible] = useState(false);
  const [feedback, setFeedback] = useState<string | null>(null);

  async function refreshStoredMessages() {
    const manager = sessionManagerRef.current;
    if (!manager) return;
    setStoredMessages(
      (await manager.getStoredMessages()).map(
        (entry: { origin: string; encryptedMessage?: EncryptedMessage; message?: Message }) => (
          entry.encryptedMessage
            ? { origin: entry.origin, encryptedMessage: entry.encryptedMessage.toJSON() }
            : { origin: entry.origin, message: entry.message?.toJSON() }
        ),
      ) as StoredMessage[],
    );
  }

  function updateObserverStatus(state: ReevaluationState) {
    const operationalBrokerId = state.modeState?.currentBrokerNodeId;
    const operationalBrokerReport =
      state.lastResult?.snapshot?.orderedBrokers.find(
        report => report.brokerId === operationalBrokerId,
      );

    setObserverStatus(
      operationalBrokerId
        ? operationalBrokerReport?.status ?? 'SIN INFORME'
        : 'SIN GESTOR',
    );
  }

  function handleReevaluationState(state: ReevaluationState) {
    if (!mountedRef.current) return;
    setReevaluationState(state);
    setOperationalSessionId(state.currentOperationalSessionId ?? null);
    const activeSession = sessionManagerRef.current?.getSessionState(
      state.currentOperationalSessionId ?? '',
    );
    setTlsConnectionVerified(
      activeSession?.connected === true && activeSession.brokerUrl.startsWith('wss://'),
    );
    updateObserverStatus(state);
    if (
      state.pendingSynchronizationInFlight === false &&
      state.lastPendingSynchronizationResult
    ) {
      refreshStoredMessages().catch(() => undefined);
    }
  }

  useEffect(() => {
    async function initializeApplication() {
      try {
        const topologyResult = await loadHarnessTopology();

        if (topologyResult.status !== 'LOADED') {
          setFeedback(
            'No se ha cargado una topologia materializada para este dispositivo.',
          );
          return;
        }

        const messageSecurityResult = topologyResult.topology.messageSecurityRequired
          ? await prepareMessageSecurity()
          : null;
        setSecurityState(messageSecurityResult);

        if (topologyResult.topology.messageSecurityRequired &&
            (!messageSecurityResult?.ok || !messageSecurityResult.securityManager)) {
          setFeedback(
            `No se pudo preparar la seguridad de mensajes: ${messageSecurityResult?.error ?? 'error desconocido'}`,
          );
          return;
        }

        const manager = new SessionManager(
          new MessageManager(),
          null,
          0,
          true,
          (messageSecurityResult?.securityManager ?? null) as never,
          messageSecurityResult?.securityManager
            ? (MESSAGE_SECURITY_KEY_ID as never)
            : null,
        );
        sessionManagerRef.current = manager;

        setTopology(topologyResult.topology);
        setConfiguredBrokers(buildConfiguredBrokers(topologyResult.topology));
        setNodeId(topologyResult.topology.node.nodeId);
        setPrimaryManagerId(
          topologyResult.topology.assignedPrimaryBrokerId ?? '',
        );
        if (topologyResult.topology.node.initializationProfile !== 'SUBSCRIBER') {
          setRuntimeProfile(topologyResult.topology.node.initializationProfile);
        }
        setBridgeCapable(topologyResult.topology.node.bridgeCapable);
        setTopologyReady(true);

        const result = await manager.initialize();
        if (!result.ok) {
          setFeedback(
            `No se pudieron recuperar los mensajes: ${String(result.error)}`,
          );
          return;
        }
        setStorageReady(true);
        await refreshStoredMessages();
      } catch (error) {
        setFeedback(`No se pudo preparar la aplicacion: ${String(error)}`);
      }
    }

    initializeApplication().catch(() => undefined);

    return () => {
      mountedRef.current = false;
      removeLocalBrokerStatusObserverRef.current?.();
      removeLocalBrokerStatusObserverRef.current = null;
      localBrokerManagerRef.current = null;
      const adapter = adapterRef.current;
      if (adapter) {
        adapter.setReevaluationStateHandler(null);
        adapter.shutdown().catch(() => undefined);
      }
    };
  }, []);

  async function startAdapter() {
    const normalizedNodeId = nodeId.trim();
    const normalizedPrimaryId = primaryManagerId.trim();
    const manager = sessionManagerRef.current;
    const requiresAssignedPrimary = runtimeProfile !== 'PRIMARY_BROKER';
    const selectedPrimaryBroker = topology?.primaryBrokers.find(
      broker => broker.nodeId === normalizedPrimaryId,
    );

    if (
      !storageReady ||
      !manager ||
      (topology?.messageSecurityRequired === true && securityState?.ok !== true) ||
      !topologyReady ||
      !topology ||
      !runtimeProfile ||
      !normalizedNodeId ||
      (requiresAssignedPrimary && !selectedPrimaryBroker)
    ) {
      setFeedback(
        'Selecciona un perfil, indica el nodo y un gestor primario declarado cuando corresponda.',
      );
      return;
    }

    setStarting(true);
    try {
      const assignedPrimaryBrokerId =
        runtimeProfile === 'PRIMARY_BROKER'
          ? null
          : selectedPrimaryBroker?.nodeId ?? null;
      const adapter = new PlatformAdapter(
        manager,
        normalizedNodeId,
        assignedPrimaryBrokerId,
        5000,
        3000,
        bridgeCapable,
        true,
      );
      adapter.setRuntimeProfile(RUNTIME_PROFILE[runtimeProfile]);
      adapter.loadKnownBrokers({ brokers: configuredBrokers });

      if (runtimeProfile !== 'BASE') {
        const permissionResult = await requestTermuxRunCommandPermission();
        if (permissionResult.granted !== true) {
          throw new Error(
            permissionResult.errorMessage ??
              'No se concedio permiso para ejecutar Termux.',
          );
        }

        const localBrokerManager = new TermuxMosquittoManager(
          normalizedNodeId,
          'wss',
          '0.0.0.0',
          topology.node.localBroker.advertisedHost,
          topology.node.localBroker.port,
          null,
          null,
          null,
          null,
          null,
          null,
          true,
          false,
          5000,
          true,
        );
        localBrokerManagerRef.current = localBrokerManager;
        const readiness = await localBrokerManager.getTlsReadiness();
        setTlsReadiness(readiness);
        removeLocalBrokerStatusObserverRef.current =
          localBrokerManager.observeBrokerStatus((status: LocalBrokerStatus) => {
            if (mountedRef.current) {
              setLocalBrokerStatus(status);
              setLocalBrokerState(toLocalBrokerViewState(status));
            }
          });
        setLocalBrokerStatus(null);
        setLocalBrokerState('PENDIENTE');
        adapter.setLocalBrokerManager(
          localBrokerManager,
          runtimeProfile === 'PRIMARY_BROKER'
            ? BROKER_ROLE.PRIMARY
            : BROKER_ROLE.TEMPORARY,
        );
      }

      adapter.setReevaluationStateHandler(handleReevaluationState);
      adapterRef.current = adapter;
      const result = await adapter.initialize();

      if (localBrokerManagerRef.current) {
        const readiness = await localBrokerManagerRef.current.getTlsReadiness();
        setTlsReadiness(readiness);
      }

      if (result.profileInitializationResult?.ok === false) {
        const profileInitializationError =
          result.profileInitializationResult.error ??
          result.profileInitializationResult.activationResult?.error ??
          result.profileInitializationResult.statusMonitoringResult?.error;
        throw new Error(
          typeof profileInitializationError === 'string'
            ? profileInitializationError
            : 'No se pudo inicializar el perfil seleccionado.',
        );
      }

      if (runtimeProfile === 'PRIMARY_BROKER') {
        setPrimaryManagerId(normalizedNodeId);
      }
      setStarted(true);
      refreshStoredMessages();
      setFeedback(
        result.ok
          ? 'Adaptador iniciado.'
          : 'Adaptador iniciado con reevaluacion activa.',
      );
    } catch (error) {
      const adapter = adapterRef.current;
      adapterRef.current = null;
      adapter?.setReevaluationStateHandler(null);
      adapter?.shutdown().catch(() => undefined);
      removeLocalBrokerStatusObserverRef.current?.();
      removeLocalBrokerStatusObserverRef.current = null;
      localBrokerManagerRef.current = null;
      setLocalBrokerStatus(null);
      setLocalBrokerState('NO APLICA');
      setFeedback(`No se pudo iniciar el adaptador: ${String(error)}`);
    } finally {
      setStarting(false);
    }
  }

  function selectRuntimeProfile(profile: RuntimeProfile) {
    setRuntimeProfile(profile);

    if (profile === 'BASE') {
      setBridgeCapable(false);
    }
  }

  async function sendOperationalInformation() {
    const adapter = adapterRef.current;
    if (!adapter || !content.trim()) return;
    const result = await adapter.send(content.trim());
    await refreshStoredMessages();
    setFeedback(
      result.ok
        ? `Mensaje ${result.message.messageId} publicado; espera confirmacion.`
        : `El mensaje quedo pendiente: ${String(result.error)}`,
    );
    if (result.ok) setContent('');
  }

  async function publishManualMessage() {
    const adapter = adapterRef.current;
    if (!adapter || !manualTopic.trim() || !manualContent.trim()) return;
    if (!operationalSessionId) {
      setFeedback('No hay una sesion operacional activa.');
      return;
    }
    const result = await adapter.sendThroughSession(
      operationalSessionId,
      manualTopic.trim(),
      manualContent.trim(),
    );
    setFeedback(
      result.ok ? 'Publicacion manual enviada.' : String(result.error),
    );
  }

  async function subscribeToTopic() {
    const adapter = adapterRef.current;
    if (!adapter || !subscriptionTopic.trim()) return;
    if (!operationalSessionId) {
      setFeedback('No hay una sesion operacional activa.');
      return;
    }
    const result = await adapter.subscribe(
      operationalSessionId,
      subscriptionTopic.trim(),
      (
        message: Message,
        context: {
          topic?: string;
          encryptedMessage?: StoredMessage['encryptedMessage'];
        } = {},
      ) => {
        setIncomingMessages(messages => [
          {
            messageId: message.messageId,
            receivedAt: new Date().toISOString(),
            encryptedMessage: context.encryptedMessage ?? null,
            decodedContent: message.getPayload(),
            topic: context.topic ?? null,
          },
          ...messages,
        ]);
      },
    );
    setFeedback(
      result.ok
        ? `Suscripcion creada para ${subscriptionTopic.trim()}.`
        : String(result.error),
    );
  }

  async function inspectStoredEncryptedMessage(
    encryptedMessageData: NonNullable<StoredMessage['encryptedMessage']>,
  ) {
    if (!securityState?.securityManager) {
      setFeedback('La seguridad de mensajes no esta disponible.');
      return;
    }

    try {
      const encryptedMessage = EncryptedMessage.fromJSON(encryptedMessageData);
      const decodedMessage = await securityState.securityManager.decodeMessage(
        encryptedMessage,
        encryptedMessage.targetTopic,
      );
      setDecryptedStoredMessages(messages => ({
        ...messages,
        [encryptedMessage.messageId]: {
          payload: decodedMessage.getPayload(),
          sourceNodeId: decodedMessage.source?.nodeId ?? null,
          timestamp: decodedMessage.timestamp ?? null,
        },
      }));
    } catch (error) {
      setFeedback(`No se pudo descifrar el mensaje seleccionado: ${String(error)}`);
    }
  }

  async function refreshTlsReadiness() {
    const localBrokerManager = localBrokerManagerRef.current;

    if (!localBrokerManager) {
      setFeedback('Este perfil no administra un broker TLS local.');
      return;
    }

    try {
      setTlsReadiness(await localBrokerManager.getTlsReadiness());
    } catch (error) {
      setFeedback(`No se pudo comprobar el material TLS: ${String(error)}`);
    }
  }

  const pending = storedMessages.filter(
    entry => storedMessageSyncState(entry) === LOCAL_SYNC_STATE.PENDING_GLOBAL_SYNC,
  );
  const synced = storedMessages.filter(
    entry => storedMessageSyncState(entry) === LOCAL_SYNC_STATE.SYNCED,
  );
  const messages =
    filter === 'PENDING'
      ? pending
      : filter === 'SYNCED'
      ? synced
      : storedMessages;
  const reports = reevaluationState.reevaluationInFlight
    ? []
    : reevaluationState.lastResult?.collectionResult?.reportResults ?? [];
  const orderedManagers = reevaluationState.reevaluationInFlight
    ? []
    : reevaluationState.lastResult?.snapshot?.orderedBrokers ?? [];
  const usableManagers = orderedManagers.filter(isUsableManager);
  const adaptationSummary =
    reevaluationState.lastResult?.adaptationSummary ?? null;
  const hasLiveModeState = reevaluationState.modeState !== undefined;
  const operationalManagerId = hasLiveModeState
    ? reevaluationState.modeState?.currentBrokerNodeId ?? 'SIN GESTOR'
    : adaptationSummary?.finalState?.brokerId ??
      reevaluationState.currentOperationalSessionId ??
      operationalSessionId ??
      'SIN DATOS';
  const operatingMode = hasLiveModeState
    ? reevaluationState.modeState?.currentMode ?? 'SIN DATOS'
    : adaptationSummary?.finalState?.mode ?? 'SIN DATOS';
  const routeState = sessionManagerRef.current?.getSessionState(
    operationalManagerId === 'SIN DATOS' || operationalManagerId === 'SIN GESTOR'
      ? ''
      : operationalManagerId,
  );
  const resyncActive =
    reevaluationState.pendingSynchronizationInFlight === true ||
    reevaluationState.periodicPendingSynchronizationActive === true;
  const currentResyncBatch =
    reevaluationState.currentPendingSynchronizationBatch ?? null;
  const latestResyncResult =
    reevaluationState.lastPendingSynchronizationResult ?? null;
  const completedResyncBatches =
    latestResyncResult?.processingResult?.batchResults ?? [];
  const localBrokerReport =
    reevaluationState.localBrokerControlReport ??
    reports.find(result => result.brokerId === nodeId.trim())?.report;
  const managedBridgeState =
    reevaluationState.managedBridgeState ??
    adaptationSummary?.finalState?.managedBridgeState ??
    null;
  const bridgeConfigurationLabel = managedBridgeState?.configured
    ? 'CONFIGURADO'
    : 'NO CONFIGURADO';
  const bridgeRouteLabel = formatBridgeRouteState(
    managedBridgeState,
    localBrokerReport,
  );
  const localBrokerViewAvailable = runtimeProfile !== 'BASE';
  const localBrokerEndpoint = localBrokerStatus?.endpoint?.brokerUrl ?? 'SIN DATOS';
  const securityAvailability = topology?.messageSecurityRequired !== true
    ? 'NO REQUERIDA'
    : !securityState?.ok
    ? 'NO DISPONIBLE'
    : tlsConnectionVerified
    ? 'VERIFICADA'
    : 'PREPARADA';
  const messageEncryptionAvailability = topology?.messageSecurityRequired !== true
    ? 'NO REQUERIDO'
    : securityState?.ok
    ? 'DISPONIBLE'
    : 'NO DISPONIBLE';
  const localTlsAvailability = !localBrokerViewAvailable
    ? 'NO APLICA'
    : !tlsReadiness
    ? 'PENDIENTE DE COMPROBACIÓN'
    : tlsReadiness.configured && tlsReadiness.prepared
    ? 'PREPARADO'
    : 'NO DISPONIBLE';
  const navigationViews: ViewName[] = localBrokerViewAvailable
    ? ['OPERATION', 'LOCAL_BROKER', 'MESSAGES', 'SECURITY', 'REEVALUATION', 'RESYNC']
    : ['OPERATION', 'MESSAGES', 'SECURITY', 'REEVALUATION', 'RESYNC'];

  if (!started)
    return (
      <SafeAreaView style={styles.safeArea}>
        <ScrollView contentContainerStyle={styles.content}>
          <Text style={styles.title}>Plataforma de comunicación</Text>
          <Text style={styles.helpText}>
            Configuración inicial del nodo.
          </Text>
          <View style={styles.card}>
            <Text style={styles.fieldLabel}>Perfil de inicializacion</Text>
            <View style={styles.selector}>
              {(Object.keys(RUNTIME_PROFILE_LABEL) as RuntimeProfile[]).map(
                profile => (
                  <Pressable
                    key={profile}
                    disabled={starting}
                    onPress={() => selectRuntimeProfile(profile)}
                    style={
                      runtimeProfile === profile ? styles.selected : styles.secondary
                    }
                  >
                    <Text>{RUNTIME_PROFILE_LABEL[profile]}</Text>
                  </Pressable>
                ),
              )}
            </View>
            <Text style={styles.fieldLabel}>ID del nodo</Text>
            <TextInput
              value={nodeId}
              onChangeText={setNodeId}
              editable={!starting}
              placeholder="node-1"
              style={styles.input}
            />
            {runtimeProfile !== 'PRIMARY_BROKER' && (
              <>
                <Text style={styles.fieldLabel}>
                  Gestor primario asignado
                </Text>
                <TextInput
                  value={primaryManagerId}
                  onChangeText={setPrimaryManagerId}
                  editable={!starting}
                  placeholder="broker-1"
                  style={styles.input}
                />
              </>
            )}
            {(runtimeProfile === 'PRIMARY_BROKER' ||
              runtimeProfile === 'BASE_TEMPORARY') && (
              <View style={styles.bridgeCapabilityRow}>
                <Text style={styles.bridgeCapabilityLabel}>
                  Este nodo puede actuar como gestor puente
                </Text>
                <Switch
                  value={bridgeCapable}
                  onValueChange={setBridgeCapable}
                  disabled={starting}
                />
              </View>
            )}
            <Text style={styles.sectionTitle}>Gestores declarados</Text>
            {configuredBrokers.map(manager => (
              <View key={manager.brokerId} style={styles.row}>
                <Text>
                  {manager.brokerId} - {managerType(manager.brokerType)}
                </Text>
                <Text style={styles.helpText}>{manager.brokerUrl}</Text>
              </View>
            ))}
            <Pressable
              disabled={
                starting || !storageReady || !topologyReady || !runtimeProfile
              }
              onPress={startAdapter}
              style={styles.primaryButton}
            >
              {starting ? (
                <ActivityIndicator color="#ffffff" />
              ) : (
                <Text style={styles.primaryButtonText}>Iniciar adaptador</Text>
              )}
            </Pressable>
          </View>
          {feedback && <Text style={styles.feedback}>{feedback}</Text>}
        </ScrollView>
      </SafeAreaView>
    );

  const header = (
    <View style={styles.header}>
      <View style={styles.headerSection}>
        <Text style={styles.headerSectionTitle}>Estado de comunicacion</Text>
        <Text>
          Via del gestor:{' '}
          {routeState?.connected ? 'CONECTADA' : 'NO DISPONIBLE'}
        </Text>
        <Text>Via de observadores: {observerStatus}</Text>
        <Text>Modo operativo: {operatingMode}</Text>
      </View>
      <View style={styles.headerSection}>
        <Text style={styles.headerSectionTitle}>Identidad y configuracion</Text>
        <Text>Nodo: {nodeId.trim()}</Text>
        <Text>
          Perfil: {runtimeProfile ? RUNTIME_PROFILE_LABEL[runtimeProfile] : 'SIN DATOS'}
        </Text>
        <Text>Gestor primario asignado: {primaryManagerId.trim()}</Text>
        <Text>Gestor utilizado: {operationalManagerId}</Text>
      </View>
    </View>
  );
  const operation = (
    <>
      <View style={styles.card}>
        <Text style={styles.sectionTitle}>Acciones</Text>
        <View style={styles.selector}>
          {(['OPERATIONAL', 'PUBLISH', 'SUBSCRIBE'] as ActionName[]).map(
            item => (
              <Pressable
                key={item}
                onPress={() => setAction(item)}
                style={action === item ? styles.selected : styles.secondary}
              >
                <Text>
                  {item === 'OPERATIONAL'
                    ? 'Informacion operativa'
                    : item === 'PUBLISH'
                    ? 'Publicar'
                    : 'Suscribirse'}
                </Text>
              </Pressable>
            ),
          )}
        </View>
        {action === 'OPERATIONAL' && (
          <>
            <TextInput
              value={content}
              onChangeText={setContent}
              multiline
              placeholder="Contenido"
              style={[styles.input, styles.messageInput]}
            />
            <Pressable
              onPress={sendOperationalInformation}
              style={styles.primaryButton}
            >
              <Text style={styles.primaryButtonText}>
                Publicar informacion operativa
              </Text>
            </Pressable>
          </>
        )}
        {action === 'PUBLISH' && (
          <>
            <TextInput
              value={manualTopic}
              onChangeText={setManualTopic}
              placeholder="Topic de publicacion"
              style={styles.input}
            />
            <TextInput
              value={manualContent}
              onChangeText={setManualContent}
              placeholder="Contenido"
              style={styles.input}
            />
            <Pressable
              onPress={publishManualMessage}
              style={styles.primaryButton}
            >
              <Text style={styles.primaryButtonText}>Publicar</Text>
            </Pressable>
          </>
        )}
        {action === 'SUBSCRIBE' && (
          <>
            <TextInput
              value={subscriptionTopic}
              onChangeText={setSubscriptionTopic}
              placeholder="Topic de suscripcion"
              style={styles.input}
            />
            <Pressable onPress={subscribeToTopic} style={styles.primaryButton}>
              <Text style={styles.primaryButtonText}>Suscribirse</Text>
            </Pressable>
          </>
        )}
      </View>
      <View style={styles.card}>
        <Text style={styles.sectionTitle}>Mensajes recibidos</Text>
        {incomingMessages.length === 0 && (
          <Text style={styles.helpText}>No hay mensajes recibidos.</Text>
        )}
        {incomingMessages.map(message => (
          <View
            key={`${message.messageId}-${message.receivedAt}`}
            style={styles.row}
          >
            <Text>{message.messageId}</Text>
            <Text>{formatDate(message.receivedAt)}</Text>
            {message.encryptedMessage && (
              <>
                <Text>Cifrado: {message.encryptedMessage.encryptedContent.keyId}</Text>
                <Text>Ciphertext: {message.encryptedMessage.encryptedContent.ciphertext}</Text>
                <Text>Tag: {message.encryptedMessage.encryptedContent.tag}</Text>
              </>
            )}
            <Text>Descifrado: {formatPayload(message.decodedContent)}</Text>
            {isPartitionTopic(message.topic) && (
              <Text style={styles.partitionNotice}>
                Recibido mediante la particion local
              </Text>
            )}
          </View>
        ))}
      </View>
    </>
  );
  const localBrokerView = (
    <View style={styles.card}>
      <Text style={styles.sectionTitle}>Gestor local</Text>
      <Text>Identificador: {nodeId.trim()}</Text>
      <Text>
        Tipo: {runtimeProfile === 'PRIMARY_BROKER' ? 'Principal' : 'Temporal'}
      </Text>
      <Text>Estado: {localBrokerState}</Text>
      <Text>Entorno: {localBrokerStatus?.environmentStatus ?? 'SIN DATOS'}</Text>
      <Text>
        Proceso Mosquitto:{' '}
        {localBrokerStatus?.brokerProcessStatus ?? 'SIN DATOS'}
      </Text>
      <Text>
        Disponible para la plataforma:{' '}
        {localBrokerStatus?.usable === true ? 'SI' : 'NO'}
      </Text>
      <Text>Endpoint publicado: {localBrokerEndpoint}</Text>
      <Text>
        Alcance de observadores: {localBrokerReport?.status ?? 'SIN INFORME'}
      </Text>
      <Text>
        Informe de control: {formatDate(localBrokerReport?.observedAt)}
      </Text>
      {bridgeCapable && (
        <>
          <Text style={styles.sectionTitle}>Puente</Text>
          <Text>Configuración: {bridgeConfigurationLabel}</Text>
          <Text>
            Gestor remoto: {managedBridgeState?.upstreamBrokerId ?? 'SIN DATOS'}
          </Text>
          <Text>
            Endpoint remoto: {managedBridgeState?.upstreamEndpoint ?? 'SIN DATOS'}
          </Text>
          <Text>Ruta a observadores: {bridgeRouteLabel}</Text>
          <Text>
            Saltos confirmados: {formatHopCount(localBrokerReport?.hopCount)}
          </Text>
        </>
      )}
    </View>
  );
  const messageView = (
    <View style={styles.card}>
      <Text style={styles.sectionTitle}>Mensajes</Text>
      <Text>
        Almacenados: {storedMessages.length} - Pendientes: {pending.length} -
        Sincronizados: {synced.length}
      </Text>
      <View style={styles.selector}>
        {(['PENDING', 'ALL', 'SYNCED'] as FilterName[]).map(item => (
          <Pressable
            key={item}
            onPress={() => setFilter(item)}
            style={filter === item ? styles.selected : styles.secondary}
          >
            <Text>
              {item === 'PENDING'
                ? 'Pendientes'
                : item === 'ALL'
                ? 'Todos'
                : 'Sincronizados'}
            </Text>
          </Pressable>
        ))}
      </View>
      {messages.length === 0 && (
        <Text style={styles.helpText}>No hay mensajes para este filtro.</Text>
      )}
      {messages.map(entry => (
        <View key={storedMessageId(entry)} style={styles.row}>
          <Text>{storedMessageId(entry)}</Text>
          <Text>
            {entry.origin} - {storedMessageSyncState(entry)} - {storedMessageStatus(entry)}
          </Text>
          <Text>Topic: {storedMessageTopic(entry)}</Text>
          {entry.encryptedMessage ? (
            <>
              <Text>Clave: {entry.encryptedMessage.encryptedContent.keyId}</Text>
              <Text>Ciphertext: {entry.encryptedMessage.encryptedContent.ciphertext}</Text>
              <Text>Tag: {entry.encryptedMessage.encryptedContent.tag}</Text>
              <Pressable
                onPress={() => inspectStoredEncryptedMessage(entry.encryptedMessage!)}
                style={styles.secondary}
              >
                <Text>Comprobar descifrado</Text>
              </Pressable>
              {decryptedStoredMessages[entry.encryptedMessage.messageId] && (
                <>
              <Text>
                Descifrado por: {decryptedStoredMessages[entry.encryptedMessage.messageId].sourceNodeId ?? 'SIN DATOS'}
              </Text>
              <Text>
                Contenido: {formatPayload(decryptedStoredMessages[entry.encryptedMessage.messageId].payload)}
              </Text>
                </>
              )}
            </>
          ) : (
            <Text>Sin cifrado: perfil funcional histórico.</Text>
          )}
        </View>
      ))}
    </View>
  );
  const securityView = (
    <View style={styles.card}>
      <Text style={styles.sectionTitle}>Seguridad</Text>
      <Text>Estado general: {securityAvailability}</Text>
      <Text style={styles.sectionTitle}>Cifrado de mensajes</Text>
      <Text>Cifrado: {messageEncryptionAvailability}</Text>
      {securityState?.cleanupWarning && (
        <Text style={styles.partitionNotice}>
          El bootstrap de clave no se pudo eliminar: {securityState.cleanupWarning}
        </Text>
      )}
      {securityState?.error && (
        <Text style={styles.partitionNotice}>{securityState.error}</Text>
      )}
      <Text style={styles.sectionTitle}>TLS local</Text>
      <Text>TLS local: {localTlsAvailability}</Text>
      {!localBrokerViewAvailable && (
        <Text style={styles.helpText}>
          La confianza TLS se verifica al conectar por WSS.
        </Text>
      )}
      {tlsReadiness?.certificate?.inspectionError && (
        <Text style={styles.partitionNotice}>
          No se pudo comprobar TLS local: {tlsReadiness.certificate.inspectionError}
        </Text>
      )}
      {localBrokerViewAvailable && (
        <Pressable onPress={refreshTlsReadiness} style={styles.secondary}>
          <Text>Actualizar material TLS</Text>
        </Pressable>
      )}
      <Text style={styles.sectionTitle}>Confianza remota</Text>
      <Text>Sesión WSS: {tlsConnectionVerified ? 'VERIFICADA' : 'PENDIENTE'}</Text>
    </View>
  );
  const reevaluation = (
    <>
      <View style={styles.reevaluationInfo}>
        <Text>
          Ultima reevaluacion:{' '}
          {formatDate(reevaluationState.lastResult?.completedAt)}
        </Text>
        <Text>
          Comprobacion periodica:{' '}
          {reevaluationState.periodicReevaluationActive ? 'ACTIVA' : 'INACTIVA'}
        </Text>
        {reevaluationState.reevaluationInFlight && (
          <Text>Reevaluacion en curso. Los informes anteriores se ocultan.</Text>
        )}
      </View>
      <View style={styles.card}>
        <Text style={styles.sectionTitle}>Ultima adaptacion</Text>
        <Text>Gestor utilizado: {operationalManagerId}</Text>
        <Text>Modo operativo: {operatingMode}</Text>
        {!adaptationSummary && (
          <Text style={styles.helpText}>
            La plataforma no ha proporcionado acciones de adaptacion.
          </Text>
        )}
        {adaptationSummary && (
          <>
            <Text>
              Sesion operacional:{' '}
              {adaptationSummary.finalState?.sessionId ?? 'SIN DATOS'}
            </Text>
            <Text>
              Broker local:{' '}
              {adaptationSummary.finalState?.localBrokerActive === true
                ? 'ACTIVO'
                : adaptationSummary.finalState?.localBrokerActive === false
                ? 'INACTIVO'
                : 'SIN DATOS'}
            </Text>
            <Pressable
              onPress={() => setAdaptationVisible(value => !value)}
              style={styles.secondary}
            >
              <Text>
                {adaptationVisible
                  ? 'Ocultar acciones realizadas'
                  : 'Ver acciones realizadas'}
              </Text>
            </Pressable>
            {adaptationVisible && adaptationSummary.actions.length === 0 && (
              <Text style={styles.helpText}>
                No se realizaron acciones de adaptacion.
              </Text>
            )}
            {adaptationVisible &&
              adaptationSummary.actions.map((adaptationAction, index) => (
                <View
                  key={`${adaptationAction.brokerId ?? 'adaptation'}-${index}`}
                  style={styles.row}
                >
                  <Text>{formatAdaptationAction(adaptationAction)}</Text>
                </View>
              ))}
          </>
        )}
      </View>
      <View style={styles.card}>
        <Pressable
          onPress={() => setReportsVisible(value => !value)}
          style={styles.secondary}
        >
          <Text>{reportsVisible ? 'Ocultar reportes' : 'Ver reportes'}</Text>
        </Pressable>
        {reportsVisible && (
          <Text style={styles.helpText}>
            La ocupacion incluye al gestor anfitrion dentro del limite de
            entidades admitidas.
          </Text>
        )}
        {reportsVisible && reports.length === 0 && (
          <Text style={styles.helpText}>No hay reportes disponibles.</Text>
        )}
        {reportsVisible &&
          reports.map(result => (
            <View key={result.brokerId} style={styles.row}>
              <Text>{result.brokerId}</Text>
              <Text>
                Tipo:{' '}
                {result.report
                  ? managerType(result.report.brokerType)
                  : 'SIN INFORME'}
              </Text>
              <Text>
                Sesion alcanzable: {result.sessionReachable ? 'SI' : 'NO'}
              </Text>
              <Text>
                Observadores: {result.report?.status ?? 'SIN INFORME'}
              </Text>
              <Text>
                Saltos: {formatHopCount(result.report?.hopCount)}
              </Text>
              <Text>
                Admision: {formatAdmissionStatus(result.report?.admissionAccepted)}
              </Text>
              <Text>
                Ocupacion: {formatAdmissionOccupancy(
                  result.report?.managedEntityCount,
                  result.report?.maximumManagedEntityCount,
                )}
              </Text>
              <Text>
                Plazas disponibles: {formatAvailableAdmissionCapacity(
                  result.report?.managedEntityCount,
                  result.report?.maximumManagedEntityCount,
                )}
              </Text>
              <Text>
                Bateria: {formatBatteryPct(result.report?.metrics?.batteryPct)}
              </Text>
              <Text>Latencia: {formatLatencyMs(result.report?.latencyMs)}</Text>
              <Text>Fecha: {formatDate(result.report?.observedAt)}</Text>
            </View>
          ))}
      </View>
      <View style={styles.card}>
        <Text style={styles.sectionTitle}>
          Candidatos utilizables por prioridad
        </Text>
        {usableManagers.length === 0 && (
          <Text style={styles.helpText}>
            No hay candidatos utilizables en la ultima reevaluacion.
          </Text>
        )}
        {usableManagers.map((manager, index) => (
          <View key={manager.brokerId} style={styles.ordered}>
            <Text>
              {index + 1}. {manager.brokerId}
            </Text>
            <View>
              <Text>{managerType(manager.brokerType)}</Text>
              <Text>
                Saltos a observadores: {formatHopCount(manager.hopCount)}
              </Text>
              <Text>Bateria: {formatBatteryPct(manager.metrics?.batteryPct)}</Text>
              <Text>Latencia: {formatLatencyMs(manager.latencyMs)}</Text>
              <Text>Puntuacion: {formatSelectionScore(manager.selectionScore)}</Text>
              <Text>
                Ocupacion: {formatAdmissionOccupancy(
                  manager.managedEntityCount,
                  manager.maximumManagedEntityCount,
                )}
              </Text>
              <Text>
                Plazas disponibles: {formatAvailableAdmissionCapacity(
                  manager.managedEntityCount,
                  manager.maximumManagedEntityCount,
                )}
              </Text>
            </View>
          </View>
        ))}
      </View>
    </>
  );
  const resynchronization = (
    <>
      <View style={styles.reevaluationInfo}>
        <Text>
          Estado de resincronizacion: {resyncActive ? 'ACTIVA' : 'INACTIVA'}
        </Text>
        <Text>
          Mensajes pendientes actuales: {pending.length}
        </Text>
      </View>
      <View style={styles.card}>
        <Text style={styles.sectionTitle}>Lote en curso</Text>
        {!currentResyncBatch && (
          <Text style={styles.helpText}>No hay ningun lote en curso.</Text>
        )}
        {currentResyncBatch && (
          <>
            <Text>Estado: {currentResyncBatch.status}</Text>
            <Text>Mensajes del lote: {currentResyncBatch.requestedMessageIds.length}</Text>
            <Text>Inicio: {formatDate(currentResyncBatch.startedAt)}</Text>
            <Text>Sesion: {currentResyncBatch.sessionId}</Text>
          </>
        )}
      </View>
      <View style={styles.card}>
        <Text style={styles.sectionTitle}>Ultimo intento completado</Text>
        {!latestResyncResult && (
          <Text style={styles.helpText}>
            Aun no se ha completado ningun intento de resincronizacion.
          </Text>
        )}
        {latestResyncResult && (
          <>
            <Text>
              Mensajes que permanecen pendientes:{' '}
              {latestResyncResult.remainingPendingMessageCount}
            </Text>
            <Text>Finalizado: {formatDate(latestResyncResult.completedAt)}</Text>
            {completedResyncBatches.length === 0 && (
              <Text style={styles.helpText}>
                No se enviaron lotes en este intento.
              </Text>
            )}
            {completedResyncBatches.map(batch => (
              <View key={batch.correlationId} style={styles.row}>
                <Text>Estado: {batch.status}</Text>
                <Text>Mensajes del lote: {batch.requestedMessageIds.length}</Text>
                <Text>
                  Mensajes confirmados: {batch.receivedMessageIds?.length ?? 0}
                </Text>
                <Text>Inicio: {formatDate(batch.startedAt)}</Text>
                <Text>Finalizacion: {formatDate(batch.completedAt)}</Text>
              </View>
            ))}
          </>
        )}
      </View>
    </>
  );
  return (
    <SafeAreaView style={styles.safeArea}>
      <ScrollView contentContainerStyle={styles.content}>
        {header}
        <View style={styles.navigation}>
          {navigationViews.map(
            item => (
              <Pressable
                key={item}
                onPress={() => {
                  setView(item);
                  if (item === 'MESSAGES') {
                    refreshStoredMessages().catch(() => undefined);
                  }
                }}
                style={[
                  styles.navigationButton,
                  view === item && styles.activeView,
                ]}
              >
                <Text>
                  {item === 'OPERATION'
                    ? 'Operacion'
                    : item === 'LOCAL_BROKER'
                    ? 'Gestor local'
                    : item === 'MESSAGES'
                    ? 'Mensajes'
                    : item === 'SECURITY'
                    ? 'Seguridad'
                    : item === 'REEVALUATION'
                    ? 'Reevaluacion'
                    : 'Resincronizacion'}
                </Text>
              </Pressable>
            ),
          )}
        </View>
        {view === 'OPERATION' && operation}
        {view === 'LOCAL_BROKER' && localBrokerViewAvailable && localBrokerView}
        {view === 'MESSAGES' && messageView}
        {view === 'SECURITY' && securityView}
        {view === 'REEVALUATION' && reevaluation}
        {view === 'RESYNC' && resynchronization}
        {feedback && <Text style={styles.feedback}>{feedback}</Text>}
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safeArea: { flex: 1, backgroundColor: '#f3f4f6' },
  content: { gap: 12, padding: 16, paddingBottom: 24 },
  title: { color: '#1f2937', fontSize: 24, fontWeight: '700' },
  helpText: { color: '#4b5563' },
  header: {
    backgroundColor: '#ffffff',
    borderRadius: 12,
    flexDirection: 'row',
    gap: 12,
    padding: 14,
  },
  headerSection: { flex: 1, gap: 4 },
  headerSectionTitle: { color: '#1f2937', fontWeight: '700' },
  card: { backgroundColor: '#ffffff', borderRadius: 12, gap: 8, padding: 14 },
  sectionTitle: { color: '#1f2937', fontSize: 17, fontWeight: '700' },
  fieldLabel: { color: '#1f2937', fontWeight: '600' },
  input: {
    borderColor: '#cbd5e1',
    borderRadius: 8,
    borderWidth: 1,
    color: '#111827',
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  messageInput: { minHeight: 72, textAlignVertical: 'top' },
  primaryButton: {
    alignItems: 'center',
    backgroundColor: '#1d4ed8',
    borderRadius: 8,
    paddingVertical: 11,
  },
  primaryButtonText: { color: '#ffffff', fontWeight: '700' },
  secondary: {
    alignItems: 'center',
    backgroundColor: '#e2e8f0',
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 9,
  },
  selected: {
    alignItems: 'center',
    backgroundColor: '#dbeafe',
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 9,
  },
  activeView: {
    backgroundColor: '#1d4ed8',
  },
  navigationButton: {
    alignItems: 'center',
    backgroundColor: '#e2e8f0',
    borderRadius: 8,
    flexBasis: '48%',
    flexGrow: 1,
    paddingHorizontal: 8,
    paddingVertical: 10,
  },
  selector: { flexDirection: 'row', flexWrap: 'wrap', gap: 8 },
  navigation: { flexDirection: 'row', flexWrap: 'wrap', gap: 8 },
  bridgeCapabilityRow: {
    alignItems: 'center',
    borderTopColor: '#e2e8f0',
    borderTopWidth: 1,
    flexDirection: 'row',
    gap: 12,
    justifyContent: 'space-between',
    paddingTop: 12,
  },
  bridgeCapabilityLabel: { flex: 1 },
  row: { borderTopColor: '#e2e8f0', borderTopWidth: 1, gap: 3, paddingTop: 8 },
  ordered: {
    borderTopColor: '#e2e8f0',
    borderTopWidth: 1,
    flexDirection: 'row',
    justifyContent: 'space-between',
    paddingTop: 10,
  },
  reevaluationInfo: { gap: 2, paddingHorizontal: 4 },
  partitionNotice: { color: '#a16207', fontWeight: '600' },
  feedback: { color: '#047857' },
});
