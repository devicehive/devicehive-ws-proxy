'use strict';

const WebSocket = require('./ws-extension'),
    kafkaext = require('./kafka-extension'),
    EventEmitter = require('events'),
    Msg = require('./msg'),
    debug = require('debug')('ws-kafka:connector');

class WSKafkaConnector extends EventEmitter {

    constructor(kc, wsc, prod_c, consumer_c) {
        super();

        if(arguments.length === 1){
            const config = kc;
            this.kafka_config = Object.assign({}, config.kafka_config);
            this.websocket_config = Object.assign({}, config.websocket_config);
            this.consumer_config = Object.assign({}, config.consumer_config);
            this.producer_config = Object.assign({}, config.producer_config);
        }else{
            this.kafka_config = Object.assign({}, kc);
            this.websocket_config = Object.assign({}, wsc);
            this.producer_config = Object.assign({}, prod_c);
            this.consumer_config = Object.assign({}, consumer_c);
        }

        if (!this.websocket_config.port) {
            throw new ReferenceError('WebSocket Server port is empty');
        }
    }

    start() {
        this.wss = new WebSocket.Server(this.websocket_config);

        this.wss
            .on('connection', (ws, req) => {
                ws.cnt = this.wss.clients.size;
                try {
                    ws.on('close', (code, reason) => {
                        debug(`closing web socket connection (code: ${code}, reason: "${reason}") ${ws.cnt}`);
                        this.emit('ws-close', ws);
                        ws.shutDown(WSKafkaConnector._errorOrDataCallback);
                    }).on('pong', () => {
                        try {
                            ws.isAlive = true;
                            debug(`Pong received from ws ${ws.cnt}`);
                        } catch (e) {
                            this._raiseError(e);
                        }
                    }).on('message', msg => {
                        this._handleWebSocketMessage(msg, ws);
                    });

                    ws.pause();
                    this._initProducer(ws);

                } catch (e) {
                    ws.resume();
                    throw e;
                }

                this.emit('ws-connection', ws, req);
            })
            .on('error', e => this._raiseError(e))
            .on('listening', () => this.emit('wss-ready', this.wss));

        this.ping_interval = setInterval(function () {
            this.wss.clients.forEach(ws => {
                if (ws.isAlive === false) return ws.terminate();

                ws.isAlive = false;
                debug(`Pinging ws ${ws.cnt}`);
                ws.ping('', false, true);
            });
        }.bind(this), 60000);
    }

    _handleWebSocketMessage(msg, ws) {
        debug(msg);
        this.emit('ws-message', msg);
        try {

            let m = Msg.fromJSON(msg);
            if(Array.isArray(m)){
                for(let i in m){
                    this._handleSingleMsg(m[i], ws);
                }
            }else {
                this._handleSingleMsg(m, ws);
            }
        }
        catch (e) {
            this._raiseError(e);
            if (e instanceof SyntaxError) {
                debug(`Invalid JSON string '${msg}'`);
            }

            debug(`error ${e}`)
        }
    }

    _handleSingleMsg(m, ws) {
        if (m.isTypeTopic) {
            this._handleTopicMessage(ws, m);
        }
        else if (m.isNotification) {
            ws.producer.queueMsg(m);
        } else if (m.isHealthCheck) {
            this._handleHealthCheck(ws, m);
        }
    }

    stop() {
        debug("stopping connector");

        clearInterval(this.ping_interval);
        if (this.wss) {
            this.wss.close();
        }
    }

    _initProducer(ws) {
        debug(`Init producer on: ${this.kafka_config.kafkaHost}`);

        if (this.kafka_config.no_zookeeper_client === undefined || this.kafka_config.no_zookeeper_client === false)
            throw Error('no_zookeeper_client should be set to "true" while we don\'t support connection through ZooKeeper');

        const client = new kafkaext.KafkaClient(Object.assign({}, this.kafka_config));

        let producer = new kafkaext.ExtProducer(client, this.producer_config);
        ws.producer = producer;

        producer
            .on('ready', () => {
                WSKafkaConnector._resumeWsWhenKafkaReady(ws);
                debug('Producer is ready');
                this.emit('producer-ready', ws.producer);
            })
            .on('error', e => this._raiseError('producer-error', e))
            .on('send-batch', function (data) {
                this._sendMessagesToKafka(producer, data);
            }.bind(this));

        if (this.producer_config.mq_interval) {
            producer.setQueueCheckInterval(this.producer_config.mq_interval);
            producer.mq.limit = this.producer_config.mq_limit || 1;
        } else {
            producer.mq.limit = 1;
        }
    }

    _sendMessagesToKafka(producer, mq){
        let map = {};

        mq.forEach(msg => {
            let t = (map[msg.payload.t] = map[msg.payload.t] || {topic: msg.payload.t});
            (t.messages = t.messages || []).push(msg.payload.m);
        });

        mq.length = 0;

        let payload = Object.values(map);
        payload.attributes = 0;

        debug(payload);
        producer.send(payload, WSKafkaConnector._errorOrDataCallback);
    }

    _raiseError(e, et = 'error') {
        debug(e);
        this.emit(et, e);
    }

    static _resumeWsWhenKafkaReady(ws) {
        if (ws.producer.ready) {
            ws.resume();
            debug(`Resuming ws ${ws.cnt}`);
        }
    }

    static _errorOrDataCallback(e, d) {
        if (e) console.error(e);
        if (d) debug(d);
    }

    _handleTopicMessage(ws, msg) {
        let rm = Msg.createReplyMessage(msg);
        rm.status = Msg.STATUS_FAIL;

        switch (msg.action) {
            case Msg.ACTION_CREATE:
                this._handleCreateTopics(ws, msg, rm);
                break;
            case Msg.ACTION_LIST:
                this._handleListTopics(ws, msg, rm);
                break;
            case Msg.ACTION_SUBSCRIBE:
                this._handleSubscribeTopics(ws, msg, rm);
                break;
            case Msg.ACTION_UNSUBSCRIBE:
                this._handleUnsubscribeTopics(ws, msg, rm);
                break;
        }
    }

    _setPayloadAndSend(p, rm, ws){
        rm.payload = p;
        ws.send(rm.toString());
    }

    _handleUnsubscribeTopics(ws, msg, rm) {
        ws.shutDownConsumer(WSKafkaConnector._errorOrDataCallback);
        rm.status = Msg.STATUS_SUCCESS;
        ws.send(rm.toString());
    }

    _handleSubscribeTopics(ws, msg, rm) {
        try {
            this._initConsumerAndSubscribeTopics(ws, msg);
            debug(`Subscribing to topics: ${msg.payload}`);
        } catch (e) {
            this._setPayloadAndSend(e, rm, ws);
            throw e;
        }
    }

    _handleListTopics(ws, msg, rm) {
        this._getTopicMetadata(ws.producer.client)
            .then(data => {
                rm.status = Msg.STATUS_SUCCESS;
                this._setPayloadAndSend(data, rm, ws);
            }).catch(e => {
            this._setPayloadAndSend(e, rm, ws);
            this._raiseError(e);
        });
    }

    _handleCreateTopics(ws, msg, rm) {
        if (!msg.hasPayload) {
            this._setPayloadAndSend('specify topics list in the "payload"', rm, ws);
        } else {
            ws.producer.createTopics(msg.payload, (e, d) => {
                if (e) {
                    this._setPayloadAndSend(e, rm, ws);
                    this._raiseError(e);
                }
                else {
                    rm.status = Msg.STATUS_SUCCESS;
                    this._setPayloadAndSend(d, rm, ws);
                    debug(`Topic created ${msg.payload}`);
                }
            })
        }
    }

    _getTopicMetadata(client, topics){
        return new Promise((resolve, reject) => {
            client.loadMetadataForTopics(topics || [], (e, res) => {
                if (e){
                    return  reject(e);
                }else if (res && res.length > 1) {
                    let payload = [];

                    topics = topics || Object.keys(res[1].metadata).filter(n => !n.startsWith('__'));
                    topics.forEach(tn => {
                        if(res[1].metadata[tn]){
                            let partitions = Object.keys(res[1].metadata[tn]);
                            partitions.forEach(p => { payload.push({topic:tn, partition:parseInt(p, 10)}) })
                        }
                    });

                    return resolve(payload);
                }
                return reject({error: 'can not parse results', result: res});
            });
        });
    }

    _initConsumerAndSubscribeTopics(ws, msg) {
        const payload = msg.payload;

        let topicsPayload = [];

        if (Array.isArray(payload)) {
            topicsPayload = payload.map(t => {
                return {topic: t}
            });
        } else {

            let rm = Msg.createReplyMessage(msg);
            rm.status = Msg.STATUS_FAIL;
            rm.payload = "Topics should be an array";
            ws.send(rm.toString());
            return;
        }

        ws.shutDownConsumer();

        const topic_names = topicsPayload.map(t=> t.topic);

        this._createConsumerGroup(ws, topic_names, msg);
    }

    _createConsumerGroup(ws, topics, msg){
        const consumer_config_copy = Object.assign({}, this.consumer_config);
        consumer_config_copy.groupId = msg.payload.consumer_group || consumer_config_copy.groupId;

        debug(`Init consumer (consumer group: ${consumer_config_copy.groupId}) on: ${this.kafka_config.kafkaHost}`);

        ws.consumer = new kafkaext.ExtConsumerGroup(
            consumer_config_copy, topics);

        let consumer = ws.consumer;

        consumer.response_msg = Msg.createReplyMessage(msg);
        consumer.subscribed_topics = topics;
        ws.consumer.response_msg.payload = topics;

        consumer.client.on('ready', function () {
            if (ws.consumer.response_msg) {
                if (ws.readyState === 1) {
                    let rm = ws.consumer.response_msg;
                    rm.status = Msg.STATUS_SUCCESS;
                    ws.send(rm.toString());
                }

                delete ws.consumer.response_msg;
            }

            this.emit('consumer-ready', ws.consumer);
            debug('Consumer is ready');
        });
        consumer
            .on('error', e => {
                if (ws.consumer && ws.consumer.response_msg) {
                    let rm = ws.consumer.response_msg;
                    rm.status = Msg.STATUS_FAIL;
                    rm.payload = e;
                    ws.send(rm.toString());
                    delete ws.consumer.response_msg;
                }

                this._raiseError('consumer-error', e);
            })
            .on('offsetOutOfRange', e => {
                this._raiseError(e);
            })
            .on('message', function (data) {
                try {
                    debug('message from kafka received');
                    this.emit('consumer-message', data);
                    consumer.queueMsg(data.value);
                } catch (e) {
                    this._raiseError(e);
                }
            }.bind(this))
            .on('send-batch', function (mq) {
                try{
                    const payload = JSON.stringify(mq.map(item => {
                        return new Msg({
                            id:Msg.guid(),
                            t: Msg.TYPE_NOTIFICAIOTN,
                            p: item
                        });
                    }));

                    mq.length = 0;
                    debug(payload);
                    ws.send(payload);
                }catch(e){
                    this._raiseError(e);
                }
            }.bind(this));

        if (consumer_config_copy.mq_interval) {
            consumer.mq.limit = consumer_config_copy.mq_limit || 1;
            consumer.setQueueCheckInterval(consumer_config_copy.mq_interval);
        } else {
            consumer.mq.limit = 1;
        }
    }

    _handleHealthCheck(ws, m) {
        let rm = Msg.createReplyMessage(m);
        rm.status = Msg.STATUS_SUCCESS;
        rm.payload = {consumers:[]};

        this.wss.clients.forEach(ws => {
            if (ws.isAlive === false) return ws.terminate();

            rm.payload.consumers.push({ws: ws.cnt, topics: ws.consumer? ws.consumer.subscribed_topics : []});
        });
        ws.send(rm.toString());
    }
}

module.exports.WSKafkaConnector = WSKafkaConnector;