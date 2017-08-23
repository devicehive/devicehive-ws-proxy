const kafka = require(`kafka-node`);
const debug = require(`debug`)(`ws-kafka:proxy`);
const Guid = require(`guid`);
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
    this.id = Guid.create().value;
    this._wsAlive = true;
  }

  get status(){
    return this._status;
  }

  set status(value){
    this._status = value;
  }

  get wsAlive(){
    return this._wsAlive;
  }

  set wsAlive(value){
    this._wsAlive = value;
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
    debug(`close buffer length ${this.buffer.length}`)
    if (this.buffer.length > 0){
      this.wsAlive = false;
      debug(this.wsAlive);
    } else {
      this._buffer.shutdown();
      super.close(resultHandler);
    }
  }

  setQueueCheckInterval(timeout){
    if (this.mq.interval){
      clearInterval(this.mq.interval);
    }

    this.mq.interval = setInterval(this.checkQueueMsg.bind(this), timeout);
  }

  setBufferCheckInterval(timeout){
    if (this.buffer.interval){
      clearInterval(this.buffer.interval);
    }

    this.buffer.interval = setInterval(this.checkBuffer.bind(this), timeout);
  }

  queueMsg(data){
    this.mq.push(data);
    if (this.mq.length >= this.mq.limit){
      this._putMqToBuffer();
    }
  }

  checkQueueMsg(){
    if (this.mq.length === 0 || !this.ready){
      return;
    }
    this._putMqToBuffer();
  }

  checkBuffer(){
    debug(`check Buffer ${this.status} ${this.id} ${this.buffer.length}`)
    if (this.buffer.length === 0 || !this.isAvailable){
      return;
    }

    this._fireSendMsgEvent();
  }

  _putMqToBuffer(){
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

  _sendingCallback(error, data){
    this.status = ExtProducer.STATUS_AVAILABLE;
    if (!error){
      debug(`success ${this.id}`);
      this.buffer.shift();
      debug(`alive : ${this.wsAlive}, length :${this._buffer.length}`)
      if (this._buffer.length === 0 && !this.wsAlive){
        this._buffer.shutdown();
        super.close();
      } else {
        this._fireSendMsgEvent();
      }
    } else {
      debug(`error ${this.id}`);
      debug(`alive : ${this.wsAlive}, length :${this._buffer.length}`)
    }
    debug(`${this.mq.length}`);
    debug(`${this.status}`)
  }

  _fireSendMsgEvent(){
    console.log(this.buffer.length, this.buffer.limit);
    console.log(this.status);
    if (this.buffer.length > 0){
      debug(`sending`);
      debug(this.id);
      if (!this.isProcessing){
        this.status = ExtProducer.STATUS_PROCESSING;
        this.emit(`send-batch`, this.buffer[0].slice());
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