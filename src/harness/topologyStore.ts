import RNFS from 'react-native-fs';

import type {
  HarnessTopologyV1,
  NodeRoleValue,
  RegisteredNodeEntry,
  TopologyLoadResult,
  TopologyNodeEntry,
  TopologySaveResult,
  TopologyValidationResult,
} from './types';

const TOPOLOGY_FILE_NAME = 'harness_topology.json';
const TOPOLOGY_FILE_PATH = `${RNFS.DocumentDirectoryPath}/${TOPOLOGY_FILE_NAME}`;
const TOPOLOGY_TEMP_FILE_PATH = `${TOPOLOGY_FILE_PATH}.tmp`;
const TOPOLOGY_BACKUP_FILE_PATH = `${TOPOLOGY_FILE_PATH}.backup`;

const SUPPORTED_PROFILES: NodeRoleValue[] = [
  'BASE',
  'BASE_TEMPORARY',
  'PRIMARY_BROKER',
  'SUBSCRIBER',
];
const DEFAULT_OPERATIONAL_TOPIC_FILTERS = ['operational/#'];

const DEFAULT_TOPOLOGY_BY_PROFILE: Record<
  NodeRoleValue,
  Omit<HarnessTopologyV1, 'savedAt'>
> = {
  BASE: {
    schemaVersion: 1,
    node: {
      nodeId: 'node-3',
      initializationProfile: 'BASE',
      bridgeCapable: false,
      localBroker: {
        advertisedHost: '10.0.2.18',
        port: 9001,
      },
    },
    assignedPrimaryBrokerId: 'node-1',
    messageSecurityRequired: false,
    primaryBrokers: [
      {
        nodeId: 'node-1',
        ip: '10.0.2.16',
        port: 9001,
      },
    ],
    temporaryBrokers: [],
    primarySubscribers: [
      {
        nodeId: 'node-2',
        ip: '10.0.2.17',
        port: 9001,
      },
    ],
    operationalTopicFilters: [...DEFAULT_OPERATIONAL_TOPIC_FILTERS],
  },
  BASE_TEMPORARY: {
    schemaVersion: 1,
    node: {
      nodeId: 'node-3',
      initializationProfile: 'BASE_TEMPORARY',
      bridgeCapable: false,
      localBroker: {
        advertisedHost: '10.0.2.18',
        port: 9001,
      },
    },
    assignedPrimaryBrokerId: 'node-1',
    messageSecurityRequired: false,
    primaryBrokers: [
      {
        nodeId: 'node-1',
        ip: '10.0.2.16',
        port: 9001,
      },
    ],
    temporaryBrokers: [],
    primarySubscribers: [],
    operationalTopicFilters: [...DEFAULT_OPERATIONAL_TOPIC_FILTERS],
  },
  PRIMARY_BROKER: {
    schemaVersion: 1,
    node: {
      nodeId: 'node-1',
      initializationProfile: 'PRIMARY_BROKER',
      bridgeCapable: false,
      localBroker: {
        advertisedHost: '10.0.2.16',
        port: 9001,
      },
    },
    assignedPrimaryBrokerId: null,
    messageSecurityRequired: false,
    primaryBrokers: [],
    temporaryBrokers: [
      {
        nodeId: 'node-3',
        ip: '10.0.2.18',
        port: 9001,
      },
    ],
    primarySubscribers: [
      {
        nodeId: 'node-2',
        ip: '10.0.2.17',
        port: 9001,
      },
    ],
    operationalTopicFilters: [...DEFAULT_OPERATIONAL_TOPIC_FILTERS],
  },
  SUBSCRIBER: {
    schemaVersion: 1,
    node: {
      nodeId: 'node-2',
      initializationProfile: 'SUBSCRIBER',
      bridgeCapable: false,
      localBroker: {
        advertisedHost: '10.0.2.17',
        port: 9001,
      },
    },
    assignedPrimaryBrokerId: null,
    messageSecurityRequired: false,
    primaryBrokers: [
      {
        nodeId: 'node-1',
        ip: '10.0.2.16',
        port: 9001,
      },
    ],
    temporaryBrokers: [
      {
        nodeId: 'node-3',
        ip: '10.0.2.18',
        port: 9001,
      },
    ],
    primarySubscribers: [],
    operationalTopicFilters: [...DEFAULT_OPERATIONAL_TOPIC_FILTERS],
  },
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function normalizeString(value: unknown) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizePort(value: unknown) {
  const port = typeof value === 'number' ? value : Number(value);
  return Number.isInteger(port) && port >= 1 && port <= 65535 ? port : null;
}

function cloneEntries(entries: TopologyNodeEntry[]) {
  return entries.map((entry) => ({ ...entry }));
}

function normalizeOperationalTopicFilters(
  value: unknown,
  errors: string[],
) {
  if (value === undefined) {
    return [...DEFAULT_OPERATIONAL_TOPIC_FILTERS];
  }

  if (!Array.isArray(value) || value.length === 0) {
    errors.push('operationalTopicFilters must be a non-empty array.');
    return [];
  }

  const normalizedFilters: string[] = [];
  const seenFilters = new Set<string>();

  value.forEach((entry, index) => {
    const topicFilter = normalizeString(entry);

    if (!topicFilter) {
      errors.push(
        `operationalTopicFilters[${index}] must be a non-empty string.`,
      );
      return;
    }

    if (topicFilter.startsWith('control/')) {
      errors.push(
        `operationalTopicFilters[${index}] cannot target control traffic.`,
      );
      return;
    }

    if (!seenFilters.has(topicFilter)) {
      seenFilters.add(topicFilter);
      normalizedFilters.push(topicFilter);
    }
  });

  return normalizedFilters;
}

function normalizeTopologyEntry(
  value: unknown,
  path: string,
  errors: string[],
): TopologyNodeEntry | null {
  if (!isRecord(value)) {
    errors.push(`${path} must be an object.`);
    return null;
  }

  const nodeId = normalizeString(value.nodeId);
  const ip = normalizeString(value.ip);
  const port = normalizePort(value.port);
  const bridgePort =
    value.bridgePort === undefined ? undefined : normalizePort(value.bridgePort);

  if (!nodeId) {
    errors.push(`${path}.nodeId must be a non-empty string.`);
  }

  if (!ip) {
    errors.push(`${path}.ip must be a non-empty string.`);
  }

  if (port === null) {
    errors.push(`${path}.port must be an integer between 1 and 65535.`);
  }

  if (bridgePort === null) {
    errors.push(`${path}.bridgePort must be an integer between 1 and 65535.`);
  }

  if (!nodeId || !ip || port === null || bridgePort === null) {
    return null;
  }

  return bridgePort === undefined
    ? { nodeId, ip, port }
    : { nodeId, ip, port, bridgePort };
}

function normalizeTopologyEntries(
  value: unknown,
  path: string,
  errors: string[],
  allowMissing = false,
) {
  if (allowMissing && value === undefined) {
    return [];
  }

  if (!Array.isArray(value)) {
    errors.push(`${path} must be an array.`);
    return [];
  }

  return value
    .map((entry, index) =>
      normalizeTopologyEntry(entry, `${path}[${index}]`, errors),
    )
    .filter((entry): entry is TopologyNodeEntry => entry !== null);
}

function isPeerReachableHost(host: string) {
  const normalizedHost = host.toLowerCase();
  return ![
    'localhost',
    '127.0.0.1',
    '0.0.0.0',
    '::1',
  ].includes(normalizedHost);
}

export function validateHarnessTopology(
  value: unknown,
): TopologyValidationResult {
  const errors: string[] = [];

  if (!isRecord(value)) {
    return {
      ok: false,
      errors: ['Topology must be a JSON object.'],
    };
  }

  if (value.schemaVersion !== 1) {
    errors.push('schemaVersion must be 1.');
  }

  const node = isRecord(value.node) ? value.node : null;
  if (!node) {
    errors.push('node must be an object.');
  }

  const nodeId = normalizeString(node?.nodeId);
  const initializationProfile = normalizeString(
    node?.initializationProfile,
  ) as NodeRoleValue;
  const bridgeCapable = node?.bridgeCapable === true;
  const localBroker = isRecord(node?.localBroker) ? node.localBroker : null;
  const advertisedHost = normalizeString(localBroker?.advertisedHost);
  const localBrokerPort = normalizePort(localBroker?.port);

  if (!nodeId) {
    errors.push('node.nodeId must be a non-empty string.');
  }

  if (!SUPPORTED_PROFILES.includes(initializationProfile)) {
    errors.push('node.initializationProfile is not supported.');
  }

  if (!localBroker) {
    errors.push('node.localBroker must be an object.');
  }

  if (!advertisedHost) {
    errors.push('node.localBroker.advertisedHost must be a non-empty string.');
  }

  if (
    advertisedHost &&
    initializationProfile !== 'SUBSCRIBER' &&
    !isPeerReachableHost(advertisedHost)
  ) {
    errors.push(
      'Broker-capable profiles require a peer-reachable advertisedHost.',
    );
  }

  if (localBrokerPort === null) {
    errors.push(
      'node.localBroker.port must be an integer between 1 and 65535.',
    );
  }

  const primaryBrokers = normalizeTopologyEntries(
    value.primaryBrokers,
    'primaryBrokers',
    errors,
  );
  const temporaryBrokers = normalizeTopologyEntries(
    value.temporaryBrokers,
    'temporaryBrokers',
    errors,
  );
  const primarySubscribers = normalizeTopologyEntries(
    value.primarySubscribers,
    'primarySubscribers',
    errors,
    true,
  );
  const operationalTopicFilters = normalizeOperationalTopicFilters(
    value.operationalTopicFilters,
    errors,
  );

  const assignedPrimaryBrokerIdValue = value.assignedPrimaryBrokerId;
  const messageSecurityRequired = value.messageSecurityRequired === undefined
    ? false
    : value.messageSecurityRequired;

  if (typeof messageSecurityRequired !== 'boolean') {
    errors.push('messageSecurityRequired must be a boolean.');
  }
  const assignedPrimaryBrokerId =
    assignedPrimaryBrokerIdValue === null ||
    assignedPrimaryBrokerIdValue === undefined
      ? null
      : normalizeString(assignedPrimaryBrokerIdValue);

  if (
    assignedPrimaryBrokerIdValue !== null &&
    assignedPrimaryBrokerIdValue !== undefined &&
    !assignedPrimaryBrokerId
  ) {
    errors.push(
      'assignedPrimaryBrokerId must be null or a non-empty string.',
    );
  }

  if (
    (initializationProfile === 'BASE' ||
      initializationProfile === 'BASE_TEMPORARY') &&
    !assignedPrimaryBrokerId
  ) {
    errors.push(
      'BASE and BASE_TEMPORARY topologies require an assignedPrimaryBrokerId.',
    );
  }

  if (
    (initializationProfile === 'BASE' ||
      initializationProfile === 'BASE_TEMPORARY') &&
    assignedPrimaryBrokerId &&
    !primaryBrokers.some(
      (entry) => entry.nodeId === assignedPrimaryBrokerId,
    )
  ) {
    errors.push(
      'assignedPrimaryBrokerId must reference an entry in primaryBrokers.',
    );
  }

  const seenNodeIds = new Set<string>();
  if (nodeId) {
    seenNodeIds.add(nodeId);
  }

  for (const [collectionName, entries] of [
    ['primaryBrokers', primaryBrokers],
    ['temporaryBrokers', temporaryBrokers],
    ['primarySubscribers', primarySubscribers],
  ] as const) {
    for (const entry of entries) {
      if (seenNodeIds.has(entry.nodeId)) {
        errors.push(
          `Node ID '${entry.nodeId}' is duplicated in the topology (${collectionName}).`,
        );
      } else {
        seenNodeIds.add(entry.nodeId);
      }
    }
  }

  const savedAtValue = normalizeString(value.savedAt);
  const savedAt = Number.isFinite(Date.parse(savedAtValue))
    ? new Date(savedAtValue).toISOString()
    : new Date().toISOString();

  if (
    errors.length > 0 ||
    !nodeId ||
    !SUPPORTED_PROFILES.includes(initializationProfile) ||
    !advertisedHost ||
    localBrokerPort === null
  ) {
    return { ok: false, errors };
  }

  return {
    ok: true,
    topology: {
      schemaVersion: 1,
      savedAt,
      node: {
        nodeId,
        initializationProfile,
        bridgeCapable,
        localBroker: {
          advertisedHost,
          port: localBrokerPort,
        },
      },
      assignedPrimaryBrokerId,
      messageSecurityRequired: messageSecurityRequired === true,
      primaryBrokers,
      temporaryBrokers,
      primarySubscribers,
      operationalTopicFilters,
    },
  };
}

export function buildDefaultHarnessTopology(
  profile: NodeRoleValue,
): HarnessTopologyV1 {
  const defaults = DEFAULT_TOPOLOGY_BY_PROFILE[profile];

  return {
    ...defaults,
    savedAt: new Date().toISOString(),
    node: {
      ...defaults.node,
      localBroker: {
        ...defaults.node.localBroker,
      },
    },
    primaryBrokers: cloneEntries(defaults.primaryBrokers),
    temporaryBrokers: cloneEntries(defaults.temporaryBrokers),
    primarySubscribers: cloneEntries(defaults.primarySubscribers),
    operationalTopicFilters: [...defaults.operationalTopicFilters],
  };
}

export function toRuntimeRegisteredNode(
  entry: TopologyNodeEntry,
): RegisteredNodeEntry {
  return {
    ...entry,
    brokerUrl: `wss://${entry.ip}:${entry.port}`,
  };
}

async function recoverInterruptedTopologyWrite() {
  const topologyExists = await RNFS.exists(TOPOLOGY_FILE_PATH);
  const backupExists = await RNFS.exists(TOPOLOGY_BACKUP_FILE_PATH);

  if (!topologyExists && backupExists) {
    await RNFS.moveFile(
      TOPOLOGY_BACKUP_FILE_PATH,
      TOPOLOGY_FILE_PATH,
    );
  }

  await removeFileIfPresent(TOPOLOGY_TEMP_FILE_PATH);
}

export async function loadHarnessTopology(): Promise<TopologyLoadResult> {
  try {
    await recoverInterruptedTopologyWrite();
    const exists = await RNFS.exists(TOPOLOGY_FILE_PATH);

    if (!exists) {
      return {
        ok: true,
        status: 'MISSING',
        filePath: TOPOLOGY_FILE_PATH,
      };
    }

    const serializedTopology = await RNFS.readFile(
      TOPOLOGY_FILE_PATH,
      'utf8',
    );

    let parsedTopology: unknown;

    try {
      parsedTopology = JSON.parse(serializedTopology);
    } catch (error) {
      return {
        ok: false,
        status: 'INVALID',
        error:
          error instanceof Error ? error.message : 'Topology JSON is invalid.',
        filePath: TOPOLOGY_FILE_PATH,
      };
    }

    const validationResult = validateHarnessTopology(parsedTopology);

    if (!validationResult.ok) {
      return {
        ok: false,
        status: 'INVALID',
        error: 'Persisted topology validation failed.',
        validationErrors: validationResult.errors,
        filePath: TOPOLOGY_FILE_PATH,
      };
    }

    return {
      ok: true,
      status: 'LOADED',
      topology: validationResult.topology,
      filePath: TOPOLOGY_FILE_PATH,
    };
  } catch (error) {
    return {
      ok: false,
      status: 'READ_ERROR',
      error:
        error instanceof Error
          ? error.message
          : 'Unexpected topology read error.',
      filePath: TOPOLOGY_FILE_PATH,
    };
  }
}

async function removeFileIfPresent(filePath: string) {
  if (await RNFS.exists(filePath)) {
    await RNFS.unlink(filePath);
  }
}

export async function saveHarnessTopology(
  topology: HarnessTopologyV1,
): Promise<TopologySaveResult> {
  const candidate = {
    ...topology,
    savedAt: new Date().toISOString(),
  };
  const validationResult = validateHarnessTopology(candidate);

  if (!validationResult.ok) {
    return {
      ok: false,
      error: 'Topology validation failed.',
      validationErrors: validationResult.errors,
      filePath: TOPOLOGY_FILE_PATH,
    };
  }

  const serializedTopology = JSON.stringify(
    validationResult.topology,
    null,
    2,
  );
  let backupCreated = false;

  try {
    await recoverInterruptedTopologyWrite();
    await removeFileIfPresent(TOPOLOGY_BACKUP_FILE_PATH);
    await RNFS.writeFile(TOPOLOGY_TEMP_FILE_PATH, serializedTopology, 'utf8');

    if (await RNFS.exists(TOPOLOGY_FILE_PATH)) {
      await RNFS.moveFile(TOPOLOGY_FILE_PATH, TOPOLOGY_BACKUP_FILE_PATH);
      backupCreated = true;
    }

    await RNFS.moveFile(TOPOLOGY_TEMP_FILE_PATH, TOPOLOGY_FILE_PATH);

    if (backupCreated) {
      try {
        await removeFileIfPresent(TOPOLOGY_BACKUP_FILE_PATH);
      } catch {
        // The new topology is already committed; stale backup cleanup can retry.
      }
    }

    return {
      ok: true,
      topology: validationResult.topology,
      filePath: TOPOLOGY_FILE_PATH,
    };
  } catch (error) {
    try {
      await removeFileIfPresent(TOPOLOGY_TEMP_FILE_PATH);

      if (
        backupCreated &&
        !(await RNFS.exists(TOPOLOGY_FILE_PATH)) &&
        (await RNFS.exists(TOPOLOGY_BACKUP_FILE_PATH))
      ) {
        await RNFS.moveFile(
          TOPOLOGY_BACKUP_FILE_PATH,
          TOPOLOGY_FILE_PATH,
        );
      }
    } catch {
      // Preserve the original storage error as the operation result.
    }

    return {
      ok: false,
      error:
        error instanceof Error
          ? error.message
          : 'Unexpected topology write error.',
      filePath: TOPOLOGY_FILE_PATH,
    };
  }
}
