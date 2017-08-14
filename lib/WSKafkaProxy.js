'use strict';

const WebSocket = require(`./wsExtension`);
const EventEmitter = require(`events`);
const Msg = require(`./msg`);
const debug = require(`debug`)(`ws-kafka:proxy`);
const pino = require(`pino`)();
const brocker = require(`./brocker`);

class WSProxy extends EventEmitter {

  constructor(clientConfig, webSocketConfig, producerConfig, consumerConfig, brockerType) {
    super();
    if (arguments.length === 1) {
      const config = clientConfig;
      this.clientConfig = Object.assign({}, config.clientConfig);
      this.webSocketConfig = Object.assign({}, config.webSocketConfig);
      this.producerConfig = Object.assign({}, config.producerConfig);
      this.consumerConfig = Object.assign({}, config.consumerConfig);
      this.brocker = brocker.getBrocker(config.brockerType);
    } else {
      this.clientConfig = Object.assign({}, clientConfig);
      this.webSocketConfig = Object.assign({}, webSocketConfig);
      this.producerConfig = Object.assign({}, producerConfig);
      this.consumerConfig = Object.assign({}, consumerConfig);
      this.brocker = brocker.getBrocker(brockerType);
    }

    if (!this.webSocketConfig.port) {
      throw new ReferenceError(`WebSocket Server port is empty`);
    }
  }

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

    this.pingInterval = setInterval(function () {
      this.webSocketServer.clients.forEach(ws => {
        if (ws.isAlive === false) {
          return ws.terminate();
        }

        ws.isAlive = false;
        debug(`Pinging ws ${ws.count}`);
        ws.ping(``, false, true);
      });
    }.bind(this), 60000);
  }

  _handleWebSocketMessage(message, ws) {
    debug(message);
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

      pino.info(`error ${error}`)
    }
  }

  _handleSingleMsg(message, ws) {
    if (message.isTypeTopic) {
      this._handleTopicMessage(ws, message);
    } else if (message.isNotification) {
      ws.producer.queueMsg(message);
    } else if (message.isHealthCheck) {
      this._handleHealthCheck(ws, message);
    }
  }

  stop() {
    debug(`stopping connector`);

    clearInterval(this.pingInterval);
    if (this.webSocketServer) {
      this.webSocketServer.close();
    }
  }

  _initProducer(ws) {

    if (this.clientConfig.no_zookeeper_client === undefined || this.clientConfig.no_zookeeper_client === false){
      throw Error(`no_zookeeper_client should be set to "true" while we don't support connection through ZooKeeper`);
    }

    const client = new this.brocker.Client(Object.assign({}, this.clientConfig));

    const producer = new this.brocker.Producer(client, this.producerConfig);
    ws.producer = producer;

    producer
      .on(`ready`, () => {
        WSProxy._resumeWsWhenKafkaReady(ws);
        this.emit(`producer-ready`, ws.producer);
      })
      .on(`error`, error => this._raiseError(`producer-error`, error))
      .on(`send-batch`, function (data) {
        this._sendMessagesToKafka(producer, data);
      }.bind(this));

    if (this.producerConfig.mq_interval) {
      producer.setQueueCheckInterval(this.producerConfig.mq_interval);
      producer.mq.limit = this.producerConfig.mq_limit || 1;
    } else {
      producer.mq.limit = 1;
    }
  }

  _sendMessagesToKafka(producer, mq) {
    const map = {};

    mq.forEach(message => {
      const t = (map[message.payload.t] = map[message.payload.t] || { topic : message.payload.t });
      (t.messages = t.messages || []).push(message.payload.m);
    });

    mq.length = 0;

    const payload = Object.values(map);
    payload.attributes = 0;

    debug(payload);
    producer.send(payload, WSProxy._errorOrDataCallback);
  }

  _raiseError(error, errorType = `error`) {
    debug(error);
    this.emit(errorType, error);
  }

  static _resumeWsWhenKafkaReady(ws) {
    if (ws.producer.ready) {
      ws.resume();
    }
  }

  static _errorOrDataCallback(error, data) {
    if (error) {
      console.error(error);
    }
    if (data) {
      pino.info(data);
    }
  }

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

  _setPayloadAndSend(payload, replyMessage, ws) {
    replyMessage.payload = payload;
    ws.send(replyMessage.toString());
  }

  _handleUnsubscribeTopics(ws, message, replyMessage) {
    ws.shutDownConsumer(WSProxy._errorOrDataCallback);
    replyMessage.status = Msg.STATUS_SUCCESS;
    ws.send(replyMessage.toString());
  }

  _handleSubscribeTopics(ws, message, replyMessage) {
    try {
      this._initConsumerAndSubscribeTopics(ws, message);
    } catch (error) {
      this._setPayloadAndSend(error, replyMessage, ws);
      throw error;
    }
  }

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

  _createConsumerGroup(ws, topics, message) {
    const consumerConfigCopy = Object.assign({}, this.consumerСonfig);
    consumerConfigCopy.groupId = message.payload.consumer_group || consumerConfigCopy.groupId;

    // debug(`Init consumer (consumer group: ${consumerСonfigСopy.groupId}) on: ${this.clientConfig.kafkaHost}`);

    ws.consumer = new this.brocker.Consumer(
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
    let counter = 0;
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
      .on(`message`, function (data) {
        try {
          debug(`message from kafka received`);
          this.emit(`consumer-message`, data);
          consumer.queueMsg(data.value);
        } catch (error) {
          this._raiseError(error);
        }
      }.bind(this))
      .on(`send-batch`, function (mq) {
        try {
          if (counter === 0){
            pino.info(``);
          }
          const payload = JSON.stringify(mq.map(item => {
            return new Msg({
              id : Msg.guid(),
              t : Msg.TYPE_NOTIFICATION,
              p : item
            });
          }));
          counter += mq.length;
          mq.length = 0;
          debug(payload);
          ws.send(payload);
          
          if (counter === 1000000){
            pino.info(``);
          }
        } catch (error) {
          this._raiseError(error);
        }
      }.bind(this));

    if (consumerConfigCopy.mq_interval) {
      consumer.mq.limit = consumerConfigCopy.mq_limit || 1;
      consumer.setQueueCheckInterval(consumerConfigCopy.mq_interval);
    } else {
      consumer.mq.limit = 1;
    }
  }

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