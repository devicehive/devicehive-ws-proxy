'use strict';

const WebSocket = require(`ws`);
const debug = require(`debug`)(`ws-kafka:ws-extension`);

/**
 * Shut down webSocket
 * 
 * @param {Function} resultHandler
 * @returns 
 */
WebSocket.prototype.shutDown = function (resultHandler) {
  // debug(`Shutting down ws & kafka ${this.cnt}`);
  this.shutDownProducer(resultHandler);
  this.shutDownConsumer(resultHandler);
};

/**
 * Shut down Producer
 * 
 * @param {Function} resultHandler
 * @returns 
 */
WebSocket.prototype.shutDownProducer = function (resultHandler) {
  this._shutDownClient(`producer`, resultHandler);
};

/**
 * Shut down Consumer
 * 
 * @param {Function} resultHandler
 * @returns 
 */
WebSocket.prototype.shutDownConsumer = function (resultHandler) {
  this._shutDownClient(`consumer`, resultHandler);
};

/**
 * Internal! Shut down Client
 * 
 * @param {Function} resultHandler
 * @returns 
 */
WebSocket.prototype._shutDownClient = function (type, resultHandler = null) {
  if (this[type]) {
    this[type].close(resultHandler);
    // debug(`shutting down ${type}`);
    this[type] = null;
  }
};

module.exports = WebSocket;