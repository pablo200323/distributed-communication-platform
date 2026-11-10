export type NodeRoleValue =
  | 'BASE'
  | 'BASE_TEMPORARY'
  | 'PRIMARY_BROKER'
  | 'SUBSCRIBER';

export type TopologyNodeEntry = {
  nodeId: string;
  ip: string;
  port: number;
  bridgePort?: number;
};

export type RegisteredNodeEntry = TopologyNodeEntry & {
  brokerUrl: string;
};

export type HarnessTopologyV1 = {
  schemaVersion: 1;
  savedAt: string;
  node: {
    nodeId: string;
    initializationProfile: NodeRoleValue;
    bridgeCapable: boolean;
    localBroker: {
      advertisedHost: string;
      port: number;
    };
  };
  assignedPrimaryBrokerId: string | null;
  messageSecurityRequired: boolean;
  primaryBrokers: TopologyNodeEntry[];
  temporaryBrokers: TopologyNodeEntry[];
  primarySubscribers: TopologyNodeEntry[];
  operationalTopicFilters: string[];
};

export type TopologyValidationResult =
  | {
      ok: true;
      topology: HarnessTopologyV1;
    }
  | {
      ok: false;
      errors: string[];
    };

export type TopologyLoadResult =
  | {
      ok: true;
      status: 'LOADED';
      topology: HarnessTopologyV1;
      filePath: string;
    }
  | {
      ok: true;
      status: 'MISSING';
      filePath: string;
    }
  | {
      ok: false;
      status: 'INVALID' | 'READ_ERROR';
      error: string;
      validationErrors?: string[];
      filePath: string;
    };

export type TopologySaveResult =
  | {
      ok: true;
      topology: HarnessTopologyV1;
      filePath: string;
    }
  | {
      ok: false;
      error: string;
      validationErrors?: string[];
      filePath: string;
    };

export type RuntimeLifecycle =
  | 'NOT_CREATED'
  | 'CREATING'
  | 'INITIALIZING'
  | 'READY'
  | 'FAILED'
  | 'RESETTING';

export type HarnessScreen =
  | 'overview'
  | 'scenarios'
  | 'options'
  | 'registry'
  | 'messages'
  | 'diagnostics'
  | 'advanced';
