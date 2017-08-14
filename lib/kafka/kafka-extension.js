'use strict';

const kafka = require(`kafka-node`);
const debug = require(`debug`)(`ws-kafka:kafka-extension`);
const Mq = require(`../Mq`);

class ExtConsumerGroup extends kafka.ConsumerGroup {

  constructor(config, topics) {
    super(config, topics);
    this._mq = new Mq();
  }

  close(resultHandler) {
    this._mq.shutdown();
    super.close(resultHandler)
  }

  setQueueCheckInterval(timeout) {
    if (this.mq.interval) {
      clearInterval(this.mq.interval);
    }

    this.mq.interval = setInterval(this.checkQueueMsg.bind(this), timeout);
  }

  get mq() {
    return this._mq;
  }

  queueMsg(data) {
    this._mq.push(data)
    if (this._mq.length >= this._mq.limit) {
      this._fireSendMsgEvent();
    }
  }

  checkQueueMsg() {
    if (this.mq.length === 0 || !this.ready){
      return;
    }
    this._fireSendMsgEvent();
  }

  _fireSendMsgEvent() {
    this.emit(`send-batch`, this._mq);
  }
}

class ExtProducer extends kafka.Producer {
  constructor(client, producer_config) {
    super(client, producer_config);
    this._mq = new Mq();
  }

  close(resultHandler) {
    this._mq.shutdown();
    super.close(resultHandler)
  }

  setQueueCheckInterval(timeout) {
    if (this.mq.interval) {
      clearInterval(this.mq.interval);
    }

    this.mq.interval = setInterval(this.checkQueueMsg.bind(this), timeout);
  }

  get mq() {
    return this._mq;
  }

  queueMsg(data) {
    this._mq.push(data)
    if (this._mq.length >= this._mq.limit) {
      this._fireSendMsgEvent();
    }
  }

  checkQueueMsg() {
    if (this.mq.length === 0 || !this.ready) {
      return;
    }
    this._fireSendMsgEvent();
  }

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