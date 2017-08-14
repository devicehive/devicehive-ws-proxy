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

  constructor(data) {
    Object.assign(this, data);
  }

  // constructor(type, action, payload) {
  //
  //     this.id = null;
  //     this.refid = null;
  //     this.t = type;
  //     this.a = action;
  //     this.p = payload;
  //     this.s = null;
  // }

  // Properties
  get status() {
    return this.s;
  }

  set status(value) {
    this.s = value;
  }

  get type() {
    return this.t;
  }

  set type(value) {
    this.t = value;
  }

  get action() {
    return this.a;
  }

  set action(value) {
    this.a = value;
  }

  get payload() {
    return this.p;
  }

  set payload(value) {
    this.p = value;
  }

  get ID() {
    return this.id;
  }

  set ID(value) {
    this.id = value;
  }

  get RefID() {
    return this.refid;
  }

  set RefID(value) {
    this.refid = value;
  }
  // End Properties

  static get TYPE_NOTIFICATION() { 
    return `notif`; 
  }

  static get TYPE_TOPIC() {
    return `topic`; 
  }

  static get TYPE_HEALTH() { 
    return `health`; 
  }

  static get ACTION_CREATE() { 
    return `create`;
  }

  static get ACTION_LIST() { 
    return `list`; 
  }

  static get ACTION_SUBSCRIBE() { 
    return `subscribe`;
  }

  static get ACTION_UNSUBSCRIBE() {
    return `unsubscribe`;
  }

  static get STATUS_SUCCESS() { 
    return 0; 
  }

  static get STATUS_FAIL() { 
    return 1; 
  }

  toString() {
    return JSON.stringify(this, (k, v) => {
      if (v === undefined || v === null) {
        return undefined;
      }
      return v;
    });
  }

  get hasPayload() {
    return this.p !== undefined && this.p !== null;
  }

  get isTypeTopic() {
    return this.t === Msg.TYPE_TOPIC;
  }

  get isNotification() {
    return this.type === Msg.TYPE_NOTIFICATION;
  }

  get isHealthCheck() {
    return this.type === Msg.TYPE_HEALTH;
  }

  static fromJSON(json) {
    const obj = JSON.parse(json);
    if (Array.isArray(obj)) {
      return obj.map(o => new Msg(o));
    }
    return new Msg(obj);
  }

  static guid() {
    return Guid.create().value;
  }

  static createReplyMessage(m) {
    return new Msg({
      id : Msg.guid(),
      refid : m.id,
      t : m.t,
      a : m.a
    });
  }
}

module.exports = Msg;