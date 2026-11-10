import mqtt from "mqtt";

export const SESSION_STATUS = {
    IDLE: 'IDLE',
    ERROR: 'ERROR',
    CONNECTING: 'CONNECTING',
    CONNECTED: 'CONNECTED',
    DISCONNECTED: 'DISCONNECTED',
    DESTROYED: 'DESTROYED'
} ;

class BrokerSession{
    constructor({ brokerUrl, sessionId, clientId, onUnexpectedClose = null, onConnected = null, handleIncomingMessage = null, reconnectPeriodMs = 0, logConnectionEvents = true }){
        if (!brokerUrl) {
            throw new Error('brokerUrl is required');
        }

        if (!clientId) {
            throw new Error('clientId is required');
        }

        if (!sessionId) {
            throw new Error('sessionId is required');
        }

        this.brokerUrl = brokerUrl;
        this.sessionId = sessionId;
        this.clientId = clientId;
        this.onUnexpectedClose = onUnexpectedClose;
        this.onConnected = onConnected;
        this.handleIncomingMessage = handleIncomingMessage;
        this.reconnectPeriodMs = reconnectPeriodMs;
        this.logConnectionEvents = logConnectionEvents;

        this.client = null;
        this.status = SESSION_STATUS.IDLE;
        this.subscriptions = new Map();
        this.expectedClose = false;
        this.unexpectedCloseNotified = false;
    }

    markExpectedClose(){
        this.expectedClose = true;
    }

    clearExpectedClose(){
        this.expectedClose = false;
    }

    notifyUnexpectedClose(sourceEvent){
        const notificationContext = {
            sessionId: this.sessionId,
            brokerUrl: this.brokerUrl,
            sourceEvent,
            status: this.status,
            expectedClose: this.expectedClose,
            reconnectPeriodMs: this.reconnectPeriodMs,
        };

        if(
            this.expectedClose ||
            this.unexpectedCloseNotified ||
            typeof this.onUnexpectedClose !== 'function'
        ){
            if (this.logConnectionEvents) {
                console.log('[BrokerSession] Unexpected-close notification suppressed', {
                    ...notificationContext,
                    reason: this.expectedClose
                        ? 'expected-close'
                        : this.unexpectedCloseNotified
                            ? 'already-notified'
                            : 'no-handler',
                });
            }
            return;
        }

        this.unexpectedCloseNotified = true;
        if (this.logConnectionEvents) {
            console.log('[BrokerSession] Emitting unexpected-close notification', notificationContext);
        }
        Promise.resolve(this.onUnexpectedClose({
            sessionId: this.sessionId,
            brokerUrl: this.brokerUrl,
            sourceEvent,
        })).catch(error => {
            console.error('Error handling unexpected session close:', error);
        });
    }

    async connect(){
        if (this.status === SESSION_STATUS.DESTROYED){
            throw new Error('BrokerSession has been destroyed');
        }

        if(this.isConnected()){
            return;
        }

        this.status = SESSION_STATUS.CONNECTING;

        const connectingClient = mqtt.connect(this.brokerUrl, {
            clientId: this.clientId,
            reconnectPeriod: this.reconnectPeriodMs,
            resubscribe: true,
        });
        this.client = connectingClient;

        this.bindClientEvents();

        await new Promise((resolve, reject) => {
            let settled = false;

            const cleanup = () => {
                connectingClient.off('connect', onConnect);
                connectingClient.off('error', onError);
                connectingClient.off('close', onClose);
            };

            const settleOnce = (completion, value) => {
                if (settled) {
                    return;
                }

                settled = true;
                cleanup();
                completion(value);
            };

            const onConnect = () => {
                settleOnce(resolve);
            };

            const onError = err => {
                settleOnce(reject, err);
            };

            const onClose = () => {
                if (this.reconnectPeriodMs > 0 && !this.expectedClose) {
                    return;
                }

                settleOnce(
                    reject,
                    new Error(
                        `Connection closed before connecting to '${this.brokerUrl}'`,
                    ),
                );
            };

            connectingClient.once('connect', onConnect);
            connectingClient.once('error', onError);
            connectingClient.once('close', onClose);
        });

        try{
            for (const [topicFilter, subscription] of this.subscriptions) {
                await connectingClient.subscribeAsync(topicFilter, subscription.options);
            }
        }
        catch(error){
            this.status = SESSION_STATUS.ERROR;
            throw error;
        }
    }

    bindClientEvents(){
        this.client.on('connect', () => {
            if(this.status === SESSION_STATUS.DESTROYED){
                return;
            }
            this.status = SESSION_STATUS.CONNECTED;
            this.unexpectedCloseNotified = false;
            if (this.logConnectionEvents) {
                console.log('Client ', this.clientId, ' connected!, status: ', this.status);
            }

            if(typeof this.onConnected === 'function'){
                Promise.resolve(this.onConnected({
                    sessionId: this.sessionId,
                    brokerUrl: this.brokerUrl,
                })).catch(error => {
                    console.error('Error handling MQTT session connection:', error);
                });
            }
        });

        this.client.on('error', (err) => {
            if(this.status === SESSION_STATUS.DESTROYED){
                return;
            }
            this.status = SESSION_STATUS.ERROR;
            if (this.logConnectionEvents) {
                console.log('[BrokerSession] MQTT error event', {
                    sessionId: this.sessionId,
                    brokerUrl: this.brokerUrl,
                    error: err?.message ?? String(err),
                    expectedClose: this.expectedClose,
                    reconnectPeriodMs: this.reconnectPeriodMs,
                });
            }
            this.notifyUnexpectedClose('error');
        });

        this.client.on('close', () => {
            if(this.status === SESSION_STATUS.DESTROYED){
                return;
            }

            this.status = SESSION_STATUS.DISCONNECTED;
            if (this.logConnectionEvents) {
                console.log('[BrokerSession] MQTT close event', {
                    sessionId: this.sessionId,
                    brokerUrl: this.brokerUrl,
                    expectedClose: this.expectedClose,
                    reconnectPeriodMs: this.reconnectPeriodMs,
                });
            }

            if(this.expectedClose){
                this.clearExpectedClose();
                return;
            }

            if (this.reconnectPeriodMs > 0) {
                if (this.logConnectionEvents) {
                    console.log(`Client ${this.clientId} disconnected. Automatic reconnect enabled every ${this.reconnectPeriodMs}ms`);
                }
                return;
            }

            this.notifyUnexpectedClose('close');
        });

        this.client.on('message', async (topic, messageJSON) => {
            if(this.status === SESSION_STATUS.DESTROYED){
                return;
            }

            if(typeof this.handleIncomingMessage !== 'function'){
                return;
            }

            try{
                await this.handleIncomingMessage(this.sessionId, topic, messageJSON);
            }
            catch(err){
                console.error('Error handling incoming MQTT message:', err);
            }
        });
    }

    getSuscriptions(){
        return this.subscriptions;
    }

    matchesTopicFilter(topicFilter, topic){
        const filterLevels = topicFilter.split('/');
        const topicLevels = topic.split('/');

        let i = 0;
        let j = 0;

        while(i < filterLevels.length && j < topicLevels.length){
            const filterLevel = filterLevels[i];
            const topicLevel = topicLevels[j];

            if(filterLevel === '#'){
                return true;
            }

            if(filterLevel === '+'){
                i++;
                j++;
                continue;
            }

            if(filterLevel !== topicLevel){
                return false;
            }

            i++;
            j++;
        }

        if(i === filterLevels.length && j === topicLevels.length){
            return true;
        }

        if(i === filterLevels.length - 1 && filterLevels[i] === '#'){
            return true;
        }

        return false;
    }

    async subscribe(topicFilter, handler, options = {}){
        if(this.status !== SESSION_STATUS.CONNECTED || !this.client){
            throw new Error('BrokerSession is not connected');
        }

        if(!topicFilter){
            throw new Error('topicFilter is required');
        }

        if(typeof handler !== 'function'){
            throw new Error('handler must be a function');
        }

        const subscribeOptions = {
            qos: options.qos ?? 0
        };

        const granted = await this.client.subscribeAsync(topicFilter, subscribeOptions);

        this.subscriptions.set(topicFilter, {handler, options: subscribeOptions});


        return {ok: true, topicFilter, granted};
    }

    async unsubscribe(topicFilter){
        if(this.status !== SESSION_STATUS.CONNECTED || !this.client){
            throw new Error('BrokerSession is not connected');
        }

        const entry = this.subscriptions.get(topicFilter);

        if(!entry){
            return { ok: false, topicFilter, error: "No topic found..." };
        }

        await this.client.unsubscribeAsync(topicFilter);
        this.subscriptions.delete(topicFilter);

        return { ok: true };
    }

    normalizePublishPayload(message){
        if(message && typeof message.toJSON === 'function'){
            return message.toJSON();
        }

        if(message && typeof message === 'object' && !Array.isArray(message)){
            return message;
        }

        throw new Error('message must be a valid Message instance or serializable object');
    }

    async publish(topic, message, options = {}){
        if(!this.client || this.status !== SESSION_STATUS.CONNECTED){
            throw new Error('BrokerSession is not connected');
        }

        if(!topic){
            throw new Error('topic is required');
        }

        if(!message){
            throw new Error('message is required');
        }

        const publishOptions = {
            qos: options.qos ?? 1,
            retain: options.retain ?? false
        };

        let normalizedMessage = JSON.stringify(this.normalizePublishPayload(message));

        await this.client.publishAsync(topic, normalizedMessage, publishOptions);

        return { ok: true };
    }

    async disconnect(force = false, expected = true){
        if(this.status === SESSION_STATUS.DESTROYED){
            return {ok: true, alreadyDestroyed: true};
        }

        if(!this.client){
            this.status = SESSION_STATUS.DISCONNECTED;
            return {ok: true, alreadyDisconnected: true};
        }

        try{
            if(expected){
                this.markExpectedClose();
            }
            await this.client.endAsync(force);
            this.status = SESSION_STATUS.DISCONNECTED;

            return {ok: true};
        }catch(err){
            this.status = SESSION_STATUS.ERROR;
            throw err;
        }
    }

    async destroy(force = false, expected = true){
        if(this.status === SESSION_STATUS.DESTROYED){
            return { ok: true, alreadyDestroyed: true };
        }

        const client = this.client;

        if(expected){
            this.markExpectedClose();
        }

        this.status = SESSION_STATUS.DESTROYED;
        this.subscriptions.clear();
        this.client = null;

        try{
            if(client){
                await client.endAsync(force);
            }

            return { ok: true };
        }
        catch(error){
            this.status = SESSION_STATUS.ERROR;
            return { ok: false, destroyed: true, error: error.message ?? String(error) };
        }
    }

    isConnected(){
        return (this.client && this.status === SESSION_STATUS.CONNECTED && this.client.connected);
    }
}

export { BrokerSession };
