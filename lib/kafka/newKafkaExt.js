const kafka = require(`kafka-node`);
const debug = require(`debug`)(`ws-kafka:kafka-extension`);
const Mq = require(`../Mq`);
const pino = require(`pino`)({level: process.env.LOG_LEVEL || 'warn'});

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
  constructor(client, producerConfig){
    super(client, producerConfig);
    this._mq = new Mq();
    this._buffer = new Mq();
    this._status = `AVAILABLE`;
  }

  get status(){
    return this._status;
  }

  set status(value){
    this._status = value;
  }

  static get STATUS_AVAILABLE(){
    return `AVAILABLE`;
  }

  static get STATUS_PROCESSING(){
    return `PROCESSING`;
  }

  static get STATUS_FULL(){
    return `FULL`;
  }

  get isAvailable(){
    return this.status === ExtProducer.STATUS_AVAILABLE;
  }

  get isProcessing(){
    return this.status === ExtProducer.STATUS_PROCESSING;
  }

  get isFull(){
    return this.status === ExtProducer.STATUS_FULL;
  }

  get mq(){
    return this._mq;
  }

  get buffer(){
    return this._buffer;
  }

  close(resultHandler){
    this._mq.shutdown();
    super.close(resultHandler);
  }

  setQueueCheckInterval(timeout){
    if (this.mq.interval){
      clearInterval(this.mq.interval);
    }

    this.mq.interval = setInterval(this.checkQueueMsg.bind(this), timeout);
  }

  queueMsg(data){
    this.mq.push(data);
    if (this.mq.length >= this.mq.limit){
      this._putMqToBuffer();
    }
  }

  checkQueueMsg(){
    if (this.mq.length === 0 || !this.ready){
      return ;
    }
    this._putMqToBuffer();
  }

  _putMqToBuffer(){
    console.log(this.buffer.length, this.buffer.limit);
    console.log(this.status);
    this.buffer.push(this.mq.slice());
    this.mq.length = 0;
    if (this.isAvailable){
      debug(`will dispatch`);
      // setTimeout(this._fireSendMsgEvent.bind(this), 40000); // for highload emulation
      this._fireSendMsgEvent();
    }
    if (this.buffer.length === this.buffer.limit){
      this.status = ExtProducer.STATUS_FULL;
    }
  }

  _sendingSuccess(){
    debug(`success`);
    console.log(this.buffer.length, this.buffer.limit);
    console.log(this.status);
    this.buffer.shift();
    this.status = ExtProducer.STATUS_AVAILABLE;
    this._fireSendMsgEvent();
  }

  _fireSendMsgEvent(){
    console.log(this.buffer.length, this.buffer.limit);
    console.log(this.status);
    if (this.buffer.length > 0){
      debug(`sending`);
      if (!this.isProcessing){
        this.emit(`send-batch`, this.buffer[0].slice());
        this.status = ExtProducer.STATUS_PROCESSING;
      }
    }
  }
}

const KafkaClient = kafka.KafkaClient;

module.exports = {
  Consumer : ExtConsumerGroup,
  Producer : ExtProducer,
  Client : KafkaClient
};