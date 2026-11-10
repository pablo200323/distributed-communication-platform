import { v4 as randomUUID } from 'uuid';

export const MESSAGE_TYPES = {
  EVENT: "EVENT",
  RESPONSE: "RESPONSE",
  ERROR: "ERROR",
};

export const MESSAGE_STATUS = {
  CREATED: "CREATED",
  PUBLISHED: "PUBLISHED",
  PROCESSING: "PROCESSING",
  PROCESSED: "PROCESSED",
  ERROR: "ERROR"
};

export const LOCAL_SYNC_STATE = {
  SYNCED: 'SYNCED',
  PENDING_GLOBAL_SYNC: 'PENDING_GLOBAL_SYNC',
};

export const MESSAGE_OPERATIONS = {
  GET_VIEW_GROUP: "GET_VIEW_GROUP",
  UPDATE_VIEW_GROUP: "UPDATE_VIEW_GROUP",
  GET_PRIMARY: "GET_PRIMARY",
  SET_PRIMARY: "SET_PRIMARY",
  JOIN_GROUP: "JOIN_GROUP",
  LEAVE_GROUP: "LEAVE_GROUP",
  NODE_AVAILABLE: "NODE_AVAILABLE",
  NODE_UNAVAILABLE: "NODE_UNAVAILABLE",
  FORWARD_ACTION: "FORWARD_ACTION",
  APPLY_ACTION: "APPLY_ACTION",
  REPLICATE_EVENT: "REPLICATE_EVENT",
  DELIVER_MESSAGE: "DELIVER_MESSAGE",
  PROCESS_ORDER: "PROCESS_ORDER",
  STATE_SNAPSHOT: "STATE_SNAPSHOT",
  STATE_DELTA: "STATE_DELTA",
  LOG_REPLAY: "LOG_REPLAY",
  RECOVER_STATE: "RECOVER_STATE",

  PROBE_STATUS_REQUEST: 'PROBE_STATUS_REQUEST',
  PROBE_STATUS_RESPONSE: 'PROBE_STATUS_RESPONSE',
  BROKER_GROUP_CHECK_REQUEST: 'BROKER_GROUP_CHECK_REQUEST',
  BROKER_GROUP_CHECK_RESPONSE: 'BROKER_GROUP_CHECK_RESPONSE',
  MANAGED_ENTITY_JOIN_REQUEST: 'MANAGED_ENTITY_JOIN_REQUEST',
  MANAGED_ENTITY_JOIN_RESPONSE: 'MANAGED_ENTITY_JOIN_RESPONSE',
  SUBSCRIBERS_REACHABILITY_CHECK_REQUEST: 'SUBSCRIBERS_REACHABILITY_CHECK_REQUEST',
  SUBSCRIBERS_REACHABILITY_CHECK_RESPONSE: 'SUBSCRIBERS_REACHABILITY_CHECK_RESPONSE',
  MANAGER_STATUS_CHANGED: 'MANAGER_STATUS_CHANGED',
  RESYNC_STATUS_REQUEST: "RESYNC_STATUS_REQUEST",
  RESYNC_STATUS_RESPONSE: "RESYNC_STATUS_RESPONSE",
  RESYNC_BATCH_REQUEST: "RESYNC_BATCH_REQUEST",
  RESYNC_BATCH_RESPONSE: "RESYNC_BATCH_RESPONSE",
  EVENT_RECEIPT_CONFIRMATION: "EVENT_RECEIPT_CONFIRMATION",
};

const NON_OPERATIONAL_MESSAGE_OPERATION_SET = new Set([
  MESSAGE_OPERATIONS.PROBE_STATUS_REQUEST,
  MESSAGE_OPERATIONS.PROBE_STATUS_RESPONSE,
  MESSAGE_OPERATIONS.BROKER_GROUP_CHECK_REQUEST,
  MESSAGE_OPERATIONS.BROKER_GROUP_CHECK_RESPONSE,
  MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_REQUEST,
  MESSAGE_OPERATIONS.MANAGED_ENTITY_JOIN_RESPONSE,
  MESSAGE_OPERATIONS.SUBSCRIBERS_REACHABILITY_CHECK_REQUEST,
  MESSAGE_OPERATIONS.SUBSCRIBERS_REACHABILITY_CHECK_RESPONSE,
  MESSAGE_OPERATIONS.MANAGER_STATUS_CHANGED,
  MESSAGE_OPERATIONS.RESYNC_STATUS_REQUEST,
  MESSAGE_OPERATIONS.RESYNC_STATUS_RESPONSE,
  MESSAGE_OPERATIONS.RESYNC_BATCH_REQUEST,
  MESSAGE_OPERATIONS.RESYNC_BATCH_RESPONSE,
  MESSAGE_OPERATIONS.EVENT_RECEIPT_CONFIRMATION,
]);

export function isOperationalMessage({ topic = null, operation = null } = {}) {
  const normalizedTopic = typeof topic === 'string' ? topic.trim() : '';

  if(!normalizedTopic || normalizedTopic.startsWith('control/')){
    return false;
  }

  return !NON_OPERATIONAL_MESSAGE_OPERATION_SET.has(operation);
}


export class Message{
  constructor(source = null, target = null, payload = null, type = null, syncState = LOCAL_SYNC_STATE.PENDING_GLOBAL_SYNC, operation = null, requiresResponse = false, correlationId = null){
    this.messageId = randomUUID();
    this.payload = payload;
    this.timestamp = new Date().toISOString();
    this.updatedAt = new Date().toISOString();
    this.source = source;
    this.target = target;
    this.type = type;
    this.status = MESSAGE_STATUS.CREATED;
    this.syncState = syncState;
    this.operation = operation;
    this.requiresResponse = requiresResponse;
    this.correlationId = correlationId;
    this.error = null;
    this.resyncTrace = null;
  }

  static assertValidateMessage(data){
    const {
      messageId,
      timestamp,
      updatedAt,
      source,
      target,
      type,
      syncState,
      operation,
      requiresResponse,
      correlationId,
      status,
    } = data;

    if(typeof messageId !== "string"){
      throw new TypeError("Attribute 'messageId' must be a string");
    }

    if(typeof timestamp !== "string"){
      throw new TypeError("Attribute 'timestamp' must be a string");
    }

    if(typeof updatedAt !== "string"){
      throw new TypeError("Attribute 'updatedAt' must be a string");
    }

    if(source !== null && (typeof source !== "object" || Array.isArray(source))){
      throw new TypeError("Attribute 'source' must be an object or null");
    }

    if(target !== null && (typeof target !== "object" || Array.isArray(target))){
      throw new TypeError("Attribute 'target' must be an object or null");
    }

    if(typeof type !== "string"){
      throw new TypeError("Attribute 'type' must be a string");
    }

    if(syncState !== null && !Object.values(LOCAL_SYNC_STATE).includes(syncState)){
      throw new TypeError("Attribute 'syncState' must be a string");
    }

    if(!Object.values(MESSAGE_TYPES).includes(type)){
      throw new TypeError(`Invalid message type: '${String(type)}'`);
    }

    if(operation !== null && typeof operation !== "string"){
      throw new TypeError("Attribute 'operation' must be a string or null");
    }

    if(operation !== null && !Object.values(MESSAGE_OPERATIONS).includes(operation)){
      throw new TypeError(`Invalid message operation: '${String(operation)}'`);
    }

    if(typeof requiresResponse !== "boolean"){
      throw new TypeError("Attribute 'requiresResponse' must be a boolean");
    }

    if(correlationId !== null && typeof correlationId !== "string"){
      throw new TypeError("Attribute 'correlationId' must be a string or null");
    }

    if(status !== null && !Object.values(MESSAGE_STATUS).includes(status)){
      throw new TypeError(`Invalid message operation: '${String(operation)}'`);
    }

  }

  statusToPublished(syncState = LOCAL_SYNC_STATE.PENDING_GLOBAL_SYNC){
    this.status = MESSAGE_STATUS.PUBLISHED;
    if(this.syncState !== LOCAL_SYNC_STATE.SYNCED){
      this.syncState = syncState;
    }
    this.updatedAt = new Date().toISOString();
  }

  statusToProcessing(){
    this.status = MESSAGE_STATUS.PROCESSING;
    this.updatedAt = new Date().toISOString();
  }

  statusToProcessed(){
    this.status = MESSAGE_STATUS.PROCESSED;
    this.updatedAt = new Date().toISOString();
  }

  statusToError(error){
    this.status = MESSAGE_STATUS.ERROR;
    if(this.syncState !== LOCAL_SYNC_STATE.SYNCED){
      this.syncState = LOCAL_SYNC_STATE.PENDING_GLOBAL_SYNC;
    }
    this.error = error;
    this.updatedAt = new Date().toISOString();
  }

  markAsSynced(){
    this.syncState = LOCAL_SYNC_STATE.SYNCED;
    this.updatedAt = new Date().toISOString();
  }

  getMessageId(){
    return this.messageId;
  }

  getStatus(){
    return this.status;
  }

  getType(){
    return this.type;
  }

  getOperation(){
    return this.operation;
  }

  getPayload(){
    return this.payload;
  }

  getCorrelationId(){
    return this.correlationId
  }

  markResyncedByNode(resyncedByNodeId){
    if(typeof resyncedByNodeId !== 'string' || resyncedByNodeId.trim() === ''){
      return { ok: false, error: 'resyncedByNodeId must be a string' };
    }

    this.resyncTrace = {
      resyncedByNodeId,
      resyncedAt: new Date().toISOString(),
    };

    this.updatedAt = new Date().toISOString();

    return { ok: true };
  }

  toJSON(){
    return{
      messageId: this.messageId,
      payload: this.payload,
      timestamp: this.timestamp,
      updatedAt: this.updatedAt,
      source: this.source,
      target: this.target,
      type: this.type,
      syncState: this.syncState,
      operation: this.operation,
      requiresResponse: this.requiresResponse,
      correlationId: this.correlationId,
      status: this.status,
      error: this.error,
      resyncTrace: this.resyncTrace,
    };
  }

  static fromJSON(messageData){
    const message = new Message();

    Message.assertValidateMessage(messageData);

    message.messageId = messageData.messageId;
    message.payload = messageData.payload;
    message.timestamp = messageData.timestamp;
    message.updatedAt = messageData.updatedAt,
    message.source = messageData.source;
    message.target = messageData.target;
    message.type = messageData.type;
    message.syncState = messageData.syncState;
    message.operation = messageData.operation;
    message.requiresResponse = messageData.requiresResponse;
    message.correlationId = messageData.correlationId;
    message.status = messageData.status;
    message.error = messageData.error ?? null;
    message.resyncTrace = messageData.resyncTrace ?? null;

    return message;
  }

  static errorMessage(payload, type = MESSAGE_TYPES.EVENT, operation = MESSAGE_OPERATIONS.APPLY_ACTION, requiresResponse = false, correlationalId = null, error){
    const message = new Message();
    message.payload = payload;
    message.operation = operation;
    message.requiresResponse = requiresResponse;
    message.correlationId = correlationalId;
    message.statusToError(error);
    return message;
  }
}
