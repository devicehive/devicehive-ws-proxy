'use strict';

const Guid = require(`guid`);

class Msg {

  // id; //id
  // refid; //refid
  //
  // t; //type
  // a; //action
  // p; //payload
  // s; //status

  /**
   * Creates an instance of Msg.
   * @param {Object} data 
   * @memberof Msg
   */
  constructor(data) {
    Object.assign(this, data);
  }

  /**
   * Returns message status
   * 
   * @memberof Msg
   */
  get status() {
    return this.s;
  }

  /**
   * Sets message status
   * 
   * @memberof Msg
   */
  set status(value) {
    this.s = value;
  }

  /**
   * Returns message type
   * 
   * @memberof Msg
   */
  get type() {
    return this.t;
  }

  /**
   * Sets message type
   * 
   * @memberof Msg
   */
  set type(value) {
    this.t = value;
  }

  /**
   * Returns message action
   * 
   * @memberof Msg
   */
  get action() {
    return this.a;
  }

  /**
   * Sets message action
   * 
   * @memberof Msg
   */
  set action(value) {
    this.a = value;
  }

  /**
   * Returns message payload
   * 
   * @memberof Msg
   */
  get payload() {
    return this.p;
  }

  /**
   * Sets message payload
   * 
   * @memberof Msg
   */
  set payload(value) {
    this.p = value;
  }

  /**
   * Returns message id
   * 
   * @memberof Msg
   */
  get ID() {
    return this.id;
  }

  /**
   * Sets message id
   * 
   * @memberof Msg
   */
  set ID(value) {
    this.id = value;
  }
  

  /**
   * Message NOTIFICATION type
   * 
   * @readonly
   * @static
   * @memberof Msg
   */
  static get TYPE_NOTIFICATION() { 
    return `notif`; 
  }

  /**
   * Message TOPIC type
   * 
   * @readonly
   * @static
   * @memberof Msg
   */
  static get TYPE_TOPIC() {
    return `topic`; 
  }
  /**
   * Message HEALTH type
   * 
   * @readonly
   * @static
   * @memberof Msg
   */
  static get TYPE_HEALTH() { 
    return `health`; 
  }

  /**
   * Message ACK action
   *
   * @readonly
   * @static
   * @memberof Msg
   */
  static get TYPE_ACK(){
    return `ack`;
  }

  /**
   * Message CREATE action
   * 
   * @readonly
   * @static
   * @memberof Msg
   */
  static get ACTION_CREATE() { 
    return `create`;
  }

  /**
   * Message LIST action
   * 
   * @readonly
   * @static
   * @memberof Msg
   */
  static get ACTION_LIST() { 
    return `list`; 
  }

  /**
   * Message SUBSCRIBE action
   * 
   * @readonly
   * @static
   * @memberof Msg
   */
  static get ACTION_SUBSCRIBE() { 
    return `subscribe`;
  }

  /**
   * Message UNSUBSCRIBE action
   * 
   * @readonly
   * @static
   * @memberof Msg
   */
  static get ACTION_UNSUBSCRIBE() {
    return `unsubscribe`;
  }

  /**
   * Message SUCCESS status
   * 
   * @readonly
   * @static
   * @memberof Msg
   */
  static get STATUS_SUCCESS() { 
    return 0; 
  }

  /**
   * Message FAIL status
   * 
   * @readonly
   * @static
   * @memberof Msg
   */
  static get STATUS_FAIL() { 
    return 1; 
  }

  /**
   * Serialize message to string
   * 
   * @returns 
   * @memberof Msg
   */
  toString() {
    return JSON.stringify(this, (k, v) => {
      if (v === undefined || v === null) {
        return undefined;
      }
      return v;
    });
  }

  /**
   * Check if payload exists
   * 
   * @readonly
   * @memberof Msg
   */
  get hasPayload() {
    return this.p !== undefined && this.p !== null;
  }

  /**
   * Check if type is TOPIC
   * 
   * @readonly
   * @memberof Msg
   */
  get isTypeTopic() {
    return this.t === Msg.TYPE_TOPIC;
  }

  /**
   * Check if type is NOTIFICATION
   * 
   * @readonly
   * @memberof Msg
   */
  get isNotification() {
    return this.type === Msg.TYPE_NOTIFICATION;
  }

  /**
   * Check if type is HEALTH
   * 
   * @readonly
   * @memberof Msg
   */
  get isHealthCheck() {
    return this.type === Msg.TYPE_HEALTH;
  }

  /**
   * Get messages from JSON string
   * 
   * @static
   * @param {String} json 
   * @returns 
   * @memberof Msg
   */
  static fromJSON(json) {
    const obj = JSON.parse(json);
    if (Array.isArray(obj)) {
      return obj.map(o => new Msg(o));
    }
    return new Msg(obj);
  }

  /**
   * Create guid
   * 
   * @static
   * @returns 
   * @memberof Msg
   */
  static guid() {
    return Guid.create().value;
  }

  /**
   * Create reply message
   * 
   * @static
   * @param {Object} message
   * @returns 
   * @memberof Msg
   */
  static createReplyMessage(message, params = {}) {
    return new Msg(Object.assign({
      id : message.id,
      t : message.t,
      a : message.a
    }, params));
  }
}

module.exports = Msg;