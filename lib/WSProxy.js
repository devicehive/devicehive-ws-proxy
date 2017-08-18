'use strict';

const WebSocket = require(`./wsExtension`);
const EventEmitter = require(`events`);
const Msg = require(`./msg`);
const debug = require(`debug`)(`ws-kafka:proxy`);
const pino = require(`pino`)({level: process.env.LOG_LEVEL || 'warn'});
const broker = require(`./broker`);

class WSProxy extends EventEmitter {

  /**
   * Creates an instance of WSProxy.
   * @param {Object} clientConfig 
   * @param {Object} webSocketConfig 
   * @param {Object} producerConfig 
   * @param {Object} consumerConfig 
   * @param {String} brokerType
   * @memberof WSProxy
   */
  constructor(clientConfig, webSocketConfig, producerConfig, consumerConfig, brokerType) {
    super();
    if (arguments.length === 1) {
      const config = clientConfig;
      this.clientConfig = Object.assign({}, config.clientConfig);
      this.webSocketConfig = Object.assign({}, config.webSocketConfig);
      this.producerConfig = Object.assign({}, config.producerConfig);
      this.consumerConfig = Object.assign({}, config.consumerConfig);
      this.broker = broker.getBroker(config.brokerType);
    } else {
      this.clientConfig = Object.assign({}, clientConfig);
      this.webSocketConfig = Object.assign({}, webSocketConfig);
      this.producerConfig = Object.assign({}, producerConfig);
      this.consumerConfig = Object.assign({}, consumerConfig);
      this.broker = broker.getBroker(brokerType);
    }

    if (!this.webSocketConfig.port) {
      throw new ReferenceError(`WebSocket Server port is empty`);
    }

    this.__status = `available`;

    this.__client = new this.broker.Client({});
    this.__producer = new this.broker.Producer(this.__client, {});

    this.__producer
      .on(`ready`, () => {
        debug(`local producer ready`);
        this.__int = setInterval(() => {
          this.__producer.send([{
            topic : `__health__`,
            messages : [`ping`]
          }], (err, data) => {
            if (err){
              this.status = WSProxy.STATUS_FAILED;
              debug(err);
            }
          })
        }, 1000);
      })
      .on(`error`, error => {
        this.status = WSProxy.STATUS_FAILED;
        debug(`local producer error ${error}`)
      });
    
    this.__consumer = new this.broker.Consumer({}, [`__health__`]);

    this.__consumer.client
      .on(`ready`, () => {
        debug(`local consumer ready`);
      });
    this.__consumer
      .on(`message`, () => {
        debug(`local ping`);
        this.status = WSProxy.STATUS_AVAILABLE;
      })
  }

  get status(){
    return this.__status;
  }

  set status(value){
    this.__status = value;
  }

  static get STATUS_AVAILABLE(){
    return `available`;
  }

  static get STATUS_FAILED(){
    return `failed`;
  }

  get isAvailable(){
    return this.status === WSProxy.STATUS_AVAILABLE;
  }

  /**
   * Start webSocket server
   * 
   * @memberof WSProxy
   */
  start() {
    this.webSocketServer = new WebSocket.Server(this.webSocketConfig);

    this.webSocketServer
      .on(`connection`, (ws, req) => {
        ws.count = this.webSocketServer.clients.size;
        try {
          ws
          .on(`close`, (code, reason) => {
            debug(`closing web socket connection (code: ${code}, reason: "${reason}") ${ws.count}`);
            this.emit(`ws-close`, ws);
            ws.shutDown(WSProxy._errorOrDataCallback);
          })
          .on(`pong`, () => {
            try {
              ws.isAlive = true;
              debug(`Pong received from ws ${ws.count}`);
            } catch (error) {
              this._raiseError(error);
            }
          })
          .on(`message`, message => {
            this._handleWebSocketMessage(message, ws);
          });

          ws.pause();
          this._initProducer(ws);

        } catch (error) {
          ws.resume();
          throw error;
        }

        this.emit(`ws-connection`, ws, req);
      })
      .on(`error`, error => this._raiseError(error))
      .on(`listening`, () => this.emit(`wss-ready`, this.wss));

    this.pingInterval = setInterval(() => {
      this.webSocketServer.clients.forEach(ws => {
        if (ws.isAlive === false) {
          return ws.terminate();
        }

        ws.isAlive = false;
        debug(`Pinging ws ${ws.count}`);
        ws.ping(``, false, true);
      });
    }, 60000);
  }

  /**
   * Internal! WebSocket message handler
   * 
   * @param {Object} message 
   * @param {WebSocket} ws 
   * @memberof WSProxy
   */
  _handleWebSocketMessage(message, ws) {
    if (this.isAvailable){
      const replyMessage = Msg.createReplyMessage({
        t : Msg.TYPE_NOTIFICATION,
        a : Msg.ACTION_ACK
      });
      replyMessage.status = Msg.STATUS_SUCCESS;
      this._setPayloadAndSend({}, replyMessage, ws);
      // debug(message);
      this.emit(`ws-message`, message);
      try {
        const messageData = Msg.fromJSON(message);
        if (Array.isArray(messageData)) {
          for (const i in messageData) {
            this._handleSingleMsg(messageData[i], ws);
          }
        } else {
          this._handleSingleMsg(messageData, ws);
        }
      } catch (error) {
        this._raiseError(error);
        if (error instanceof SyntaxError) {
          pino.info(`Invalid JSON string '${message}'`);
        }

        pino.error(`error ${error}`)
      }
    } else {
      const replyMessage = Msg.createReplyMessage({
        t : Msg.TYPE_NOTIFICATION,
        a : Msg.ACTION_ACK
      });
      replyMessage.status = Msg.STATUS_FAIL;
      this._setPayloadAndSend(`Broker not available`, replyMessage, ws);
    }
  }

  /**
   * Internal! Single message handler
   * 
   * @param {Object} message 
   * @param {WebSocket} ws 
   * @memberof WSProxy
   */
  _handleSingleMsg(message, ws) {
    if (message.isTypeTopic) {
      this._handleTopicMessage(ws, message);
    } else if (message.isNotification) {
      // ws.producer.queueMsg(message);
      this._handleNotificationMessage(ws, message);
    } else if (message.isHealthCheck) {
      this._handleHealthCheck(ws, message);
    }
  }

  /**
   * Stop webSocket server
   * 
   * @memberof WSProxy
   */
  stop() {
    debug(`stopping connector`);
    clearInterval(this.__int);
    clearInterval(this.pingInterval);
    // if (this.webSocketServer) {
    //   this.webSocketServer.close();
    // }
  }

  /**
   * Internal! Init producer
   * 
   * @param {WebSocket} ws 
   * @memberof WSProxy
   */
  _initProducer(ws) {

    if (this.clientConfig.no_zookeeper_client === undefined || this.clientConfig.no_zookeeper_client === false){
      throw Error(`no_zookeeper_client should be set to "true" while we don't support connection through ZooKeeper`);
    }

    const client = new this.broker.Client(Object.assign({}, this.clientConfig));

    const producer = new this.broker.Producer(client, this.producerConfig);
    ws.producer = producer;

    producer
      .on(`ready`, () => {
        WSProxy._resumeWsWhenProducerReady(ws);
        this.emit(`producer-ready`, ws.producer);
      })
      .on(`error`, error => this._raiseError(`producer-error`, error))
      .on(`send-batch`, (data) => {
        this._sendMessagesToBroker(producer, data);
      });

    if (this.producerConfig.mq_interval) {
      producer.setQueueCheckInterval(this.producerConfig.mq_interval);
    } 
    producer.mq.limit = this.producerConfig.mq_limit || 1;
  }

  /**
   * Internal! Send message queue to Broker
   * 
   * @param {Object} producer 
   * @param {Mq} mq 
   * @memberof WSProxy
   */
  _sendMessagesToBroker(producer, mq) {
    const map = {};
    const mqCopy = Array.from(mq);
    mq.forEach(message => {
      const t = (map[message.payload.t] = map[message.payload.t] || { topic : message.payload.t });
      (t.messages = t.messages || []).push(message.payload.m);
    });

    mq.length = 0;

    const payload = Object.values(map);
    payload.attributes = 0;

    // debug(payload);
    producer.send(payload, (error, data) => {
      if (error){
        debug(`Producer error ${error}`);
        debug(`lost mq length ${mqCopy.length}, ${mqCopy[0]}`);
        this.status = WSProxy.STATUS_FAILED;
        console.log(this);
      } else {
        debug(`success`);
      }
    });
  }

  /**
   * Internal! Rise error if happend
   * 
   * @param {Object} error 
   * @param {String} [errorType=`error`] 
   * @memberof WSProxy
   */
  _raiseError(error, errorType = `error`) {
    pino.error(error);
    this.emit(errorType, error);
  }

  /**
   * Internal! Resume WS on producer ready
   * 
   * @static
   * @param {WebSocket} ws 
   * @memberof WSProxy
   */
  static _resumeWsWhenProducerReady(ws) {
    if (ws.producer.ready) {
      ws.resume();
    }
  }

  /**
   * Internal! Called on error or data
   * 
   * @static
   * @param {Object} error 
   * @param {Object} data 
   * @memberof WSProxy
   */
  static _errorOrDataCallback(error, data) {
    if (error) {
      console.error(error);
    }
    if (data) {
      debug(data);
      //pino.info(data);
    }
  }

  /**
   * Internal! TOPIC message handler
   * 
   * @param {WebSocket} ws 
   * @param {Object} message 
   * @memberof WSProxy
   */
  _handleTopicMessage(ws, message) {
    const replyMessage = Msg.createReplyMessage(message);
    replyMessage.status = Msg.STATUS_FAIL;
    switch (message.action) {
    case Msg.ACTION_CREATE:
      this._handleCreateTopics(ws, message, replyMessage);
      break;
    case Msg.ACTION_LIST:
      this._handleListTopics(ws, message, replyMessage);
      break;
    case Msg.ACTION_SUBSCRIBE:
      this._handleSubscribeTopics(ws, message, replyMessage);
      break;
    case Msg.ACTION_UNSUBSCRIBE:
      this._handleUnsubscribeTopics(ws, message, replyMessage);
      break;
    }
  }

  /**
   * Internal! NOTIFICATION message handler
   * 
   * @param {WebSocket} ws 
   * @param {Object} message 
   * @memberof WSProxy
   */
  _handleNotificationMessage(ws, message){
    ws.producer.queueMsg(message);
  }

  /**
   * Internal! Sets message payload
   * 
   * @param {Object} payload 
   * @param {Object} replyMessage 
   * @param {WebSocket} ws 
   * @memberof WSProxy
   */
  _setPayloadAndSend(payload, replyMessage, ws) {
    replyMessage.payload = payload;
    ws.send(replyMessage.toString());
  }

  /**
   * Internal! UNSUBSCRIBE message handler
   * 
   * @param {WebSocket} ws 
   * @param {Object} message 
   * @param {Object} replyMessage 
   * @memberof WSProxy
   */
  _handleUnsubscribeTopics(ws, message, replyMessage) {
    ws.shutDownConsumer(WSProxy._errorOrDataCallback);
    replyMessage.status = Msg.STATUS_SUCCESS;
    ws.send(replyMessage.toString());
  }

  /**
   * Internal! SUBSCRIBE message handler
   * 
   * @param {WebSocket} ws 
   * @param {Object} message 
   * @param {Object} replyMessage 
   * @memberof WSProxy
   */
  _handleSubscribeTopics(ws, message, replyMessage) {
    try {
      this._initConsumerAndSubscribeTopics(ws, message);
    } catch (error) {
      this._setPayloadAndSend(error, replyMessage, ws);
      throw error;
    }
  }

  /**
   * Internal! LIST message handler
   * 
   * @param {WebSocket} ws 
   * @param {Object} message 
   * @param {Object} replyMessage 
   * @memberof WSProxy
   */
  _handleListTopics(ws, message, replyMessage) {
    this._getTopicMetadata(ws.producer.client)
      .then(data => {
        replyMessage.status = Msg.STATUS_SUCCESS;
        this._setPayloadAndSend(data, replyMessage, ws);
      })
      .catch(error => {
        this._setPayloadAndSend(error, replyMessage, ws);
        this._raiseError(error);
      });
  }

  /**
   * Internal! CREATE message handler
   * 
   * @param {WebSocket} ws 
   * @param {Object} message 
   * @param {Object} replyMessage 
   * @memberof WSProxy
   */
  _handleCreateTopics(ws, message, replyMessage) {
    if (!message.hasPayload) {
      this._setPayloadAndSend(`specify topics list in the "payload"`, replyMessage, ws);
    } else {
      ws.producer.createTopics(message.payload, (error, data) => {
        if (error) {
          this._setPayloadAndSend(error, replyMessage, ws);
          this._raiseError(error);
        } else {
          replyMessage.status = Msg.STATUS_SUCCESS;
          this._setPayloadAndSend(data, replyMessage, ws);
          debug(`Topic created ${message.payload}`);
        }
      })
    }
  }

  /**
   * Internal! Returns topic metadata
   * 
   * @param {Object} client 
   * @param {Array} topics 
   * @returns 
   * @memberof WSProxy
   */
  _getTopicMetadata(client, topics) {
    return new Promise((resolve, reject) => {
      client.loadMetadataForTopics(topics || [], (error, res) => {
        if (error) {
          return reject(error);
        } else if (res && res.length > 1) {
          const payload = [];

          topics = topics || Object.keys(res[1].metadata).filter(n => !n.startsWith(`__`));
          topics.forEach(topicName => {
            if (res[1].metadata[topicName]) {
              const partitions = Object.keys(res[1].metadata[topicName]);
              partitions.forEach(p => { 
                payload.push({ topic : topicName, partition : parseInt(p, 10) });
              })
            }
          });

          return resolve(payload);
        }
        return reject({ error : `can not parse results`, result : res });
      });
    });
  }

  /**
   * Internal! Inits consumer and subscribes it on topics
   * 
   * @param {WebSocket} ws 
   * @param {Object} message 
   * @returns 
   * @memberof WSProxy
   */
  _initConsumerAndSubscribeTopics(ws, message) {
    const payload = message.payload;

    if (!Array.isArray(payload.t)) {

      const replyMessage = Msg.createReplyMessage(message);
      replyMessage.status = Msg.STATUS_FAIL;
      replyMessage.payload = `Topics should be an array`;
      ws.send(replyMessage.toString());
      return;
    }

    ws.shutDownConsumer();
    this._createConsumerGroup(ws, payload.t, message);
  }

  /**
   * Internal! Consumer group creator
   * 
   * @param {WebSocket} ws 
   * @param {Array} topics 
   * @param {Object} message 
   * @memberof WSProxy
   */
  _createConsumerGroup(ws, topics, message) {
    const consumerConfigCopy = Object.assign({}, this.consumerConfig);
    consumerConfigCopy.groupId = message.payload.consumer_group || consumerConfigCopy.groupId;

    // debug(`Init consumer (consumer group: ${consumerСonfigСopy.groupId}) on: ${this.clientConfig.kafkaHost}`);

    ws.consumer = new this.broker.Consumer(
      consumerConfigCopy, topics);

    debug(`Subscribing to topics: ${topics}`);

    const consumer = ws.consumer;

    consumer.replyMessage = Msg.createReplyMessage(message);
    consumer.subscribedTopics = topics;
    ws.consumer.replyMessage.payload = topics;

    consumer.client.on(`ready`, function () {
      if (ws.consumer.replyMessage) {
        if (ws.readyState === 1) {
          const replyMessage = ws.consumer.replyMessage;
          replyMessage.status = Msg.STATUS_SUCCESS;
          ws.send(replyMessage.toString());
        }

        delete ws.consumer.replyMessage;
      }

      this.emit(`consumer-ready`, ws.consumer);
      debug(`Consumer is ready`);
    });
    consumer
      .on(`error`, error => {
        if (ws.consumer && ws.consumer.replyMessage) {
          const replyMessage = ws.consumer.replyMessage;
          replyMessage.status = Msg.STATUS_FAIL;
          replyMessage.payload = error;
          ws.send(replyMessage.toString());
          delete ws.consumer.replyMessage;
        }

        this._raiseError(`consumer-error`, error);
      })
      .on(`offsetOutOfRange`, error => {
        this._raiseError(error);
      })
      .on(`message`, (data) => {
        try {
          // debug(`message from kafka received`);
          this.emit(`consumer-message`, data);
          consumer.queueMsg(data.value);
        } catch (error) {
          this._raiseError(error);
        }
      })
      .on(`send-batch`, (mq) => {
        try {
          // consumer.pause();
          const payload = JSON.stringify(mq.map(item => {
            return new Msg({
              id : Msg.guid(),
              t : Msg.TYPE_NOTIFICATION,
              p : item
            });
          }));
          mq.length = 0;
          // debug(payload);
          ws.send(payload);
          // ws.send(payload, function dataSent(e){
          //   if(e) {
          //     pino.error(e);
          //   }
          //   // consumer.resume();
          // });
        } catch (error) {
          this._raiseError(error);
            // consumer.resume();
        }
      });

    if (consumerConfigCopy.mq_interval) {
      consumer.mq.limit = consumerConfigCopy.mq_limit || 1;
      consumer.setQueueCheckInterval(consumerConfigCopy.mq_interval);
    } else {
      consumer.mq.limit = 1;
    }
  }

  /**
   * Internal! HEALTH message handler
   * 
   * @param {WebSocket} ws 
   * @param {Object} message 
   * @memberof WSProxy
   */
  _handleHealthCheck(ws, message) {
    const replyMessage = Msg.createReplyMessage(message);
    replyMessage.status = Msg.STATUS_SUCCESS;
    replyMessage.payload = { consumers : [] };

    this.wss.clients.forEach(ws => {
      if (ws.isAlive === false){ 
        return ws.terminate();
      }

      replyMessage.payload.consumers.push({ ws : ws.count, topics : ws.consumer ? ws.consumer.subscribedTopics : [] });
    });
    ws.send(replyMessage.toString());
  }
}

module.exports.WSProxy = WSProxy;