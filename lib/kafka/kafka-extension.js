'use strict';

const kafka = require(`kafka-node`);
const debug = require(`debug`)(`ws-kafka:kafka-extension`);
const Mq = require(`../Mq`);
const pino = require(`pino`)();

class ExtConsumerGroup extends kafka.ConsumerGroup {

  /**
   * Creates an instance of ExtConsumerGroup.
   * @param {Object} config 
   * @param {Array} topics 
   * @memberof ExtConsumerGroup
   */
  constructor(config, topics) {
    super(config, topics);
    this._mq = new Mq();
  }

  /**
   * Close consumer
   * 
   * @param {Function} resultHandler 
   * @memberof ExtConsumerGroup
   */
  close(resultHandler) {
    this._mq.shutdown();
    super.close(resultHandler)
  }
  
  /**
   * Set message queue check interval
   * 
   * @param {Timeout} timeout 
   * @memberof ExtConsumerGroup
   */
  setQueueCheckInterval(timeout) {
    if (this.mq.interval) {
      clearInterval(this.mq.interval);
    }

    this.mq.interval = setInterval(this.checkQueueMsg.bind(this), timeout);
  }

  /**
   * Returns message queue
   * 
   * @readonly
   * @memberof ExtConsumerGroup
   */
  get mq() {
    return this._mq;
  }

  /**
   * Push message to queue
   * 
   * @param {Object} data 
   * @memberof ExtConsumerGroup
   */
  queueMsg(data) {
    this._mq.push(data)
    if (this._mq.length >= this._mq.limit) {
      this._fireSendMsgEvent();
    }
  }

  /**
   * Check message queue on fulfillment
   * 
   * @returns 
   * @memberof ExtConsumerGroup
   */
  checkQueueMsg() {
    if (this.mq.length === 0 || !this.ready){
      return;
    }
    this._fireSendMsgEvent();
  }

  /**
   * Internal! Fires event to send message queue as batch
   * 
   * @memberof ExtConsumerGroup
   */
  _fireSendMsgEvent() {
    this.emit(`send-batch`, this._mq);
  }
}

class ExtProducer extends kafka.Producer {
  /**
   * Creates an instance of ExtProducer.
   * @param {Object} client 
   * @param {Object} producerConfig 
   * @memberof ExtProducer
   */
  constructor(client, producerConfig) {
    super(client, producerConfig);
    this._mq = new Mq();
  }

  /**
   * Close producer
   * 
   * @param {any} resultHandler 
   * @memberof ExtProducer
   */
  close(resultHandler) {
    this._mq.shutdown();
    super.close(resultHandler)
  }

  /**
   * Set message queue check interval 
   * 
   * @param {Timeout} timeout 
   * @memberof ExtProducer
   */
  setQueueCheckInterval(timeout) {
    if (this.mq.interval) {
      clearInterval(this.mq.interval);
    }

    this.mq.interval = setInterval(this.checkQueueMsg.bind(this), timeout);
  }

  /**
   * Returns message queue
   * 
   * @readonly
   * @memberof ExtProducer
   */
  get mq() {
    return this._mq;
  }

  /**
   * Push message to queue
   * 
   * @param {Object} data 
   * @memberof ExtProducer
   */
  queueMsg(data) {
    this._mq.push(data)
    if (this._mq.length >= this._mq.limit) {
      this._fireSendMsgEvent();
    }
  }

  /**
   * Check message queue on fulfillment
   * 
   * @returns 
   * @memberof ExtProducer
   */
  checkQueueMsg() {
    pino.info(`${JSON.stringify(this.ready)}`);
    if (this.mq.length === 0 || !this.ready) {
      return;
    }
    this._fireSendMsgEvent();
  }

  /**
   * Internal! Fires event to send message queue as batch
   * 
   * @memberof ExtProducer
   */
  _fireSendMsgEvent() {
    this.emit(`send-batch`, this._mq);
  }

}

const KafkaClient = kafka.KafkaClient;

module.exports = {
  Consumer : ExtConsumerGroup,
  Producer : ExtProducer,
  Client : KafkaClient
};