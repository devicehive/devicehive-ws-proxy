'use strict';

const   WebSocket = require('ws'),
        debug = require('debug')('ws-kafka:ws-extension');

WebSocket.prototype.shutDown = function (cb) {

    try {
        debug(`Shutting down ws & kafka ${this.cnt}`);
        this.shutDownProducer(cb);
        this.shutDownConsumer(cb);
    } catch (e) {
        console.error(e);
    }
};

WebSocket.prototype.shutDownProducer = function(cb) {
    this._shutDownKafkaClient(false, cb);
};

WebSocket.prototype.shutDownConsumer = function(cb){
    this._shutDownKafkaClient(true, cb);
};

WebSocket.prototype._shutDownKafkaClient = function (is_consumer=true, cb=null){
    const item = is_consumer? 'consumer': 'producer';

    if (this[item]) {
        this[item].close(cb);
        debug(`shutting down ${item}`);
        this[item] = null;
    }
};

module.exports = WebSocket;