const Config = require(`../../config`).messageBuffer;
const Utils = require(`../../utils`);
const FIFO = require(`fifo`);
const EventEmitter = require(`events`);
const FullMessageBufferError = require(`../../lib/errors/messageBuffer/FullMessageBufferError`);
const sizeof = require('object-sizeof');
const debug = require(`debug`)(`messagebuffer`);


/**
 * Message buffer class. Implements FIFO stack functionality
 */
class MessageBuffer extends EventEmitter {

    static get POLL_EVENT() { return `poll` };

    /**
     * Creates new MessageBuffer
     */
    constructor() {
        super();

        const me = this;

        me.fifo = new FIFO();
        me.maxDataSizeB = me.freeMemory = Config.MAX_SIZE_MB * Utils.B_IN_MB;
        me.dataSize = 0;
        me._pollingIntervalHandler = null;
        me._isPollingInStop = true;
        me._enablePolling = false;

        debug(`Maximum size of message buffer: ${Config.MAX_SIZE_MB} Mb`);
    }

    /**
     * Returns length on MessageBuffer
     * @returns {number}
     */
    get length() {
        const me = this;

        return me.fifo.length;
    }

    /**
     * Pushes new message to MessageBuffer
     * Emits next events:
     *      - notEmpty
     * @param message
     */
    push(message) {
        const me = this;
        const sizeOfMessage = sizeof(message.message);

        if (me.getFreeMemory() < sizeOfMessage) {
            throw new FullMessageBufferError(message.message);
        }

        message.size = sizeOfMessage;

        me.fifo.push(message);
        me._incrementDataSize(sizeOfMessage);

        if (me._isPollingInStop === true && me._enablePolling === true) {
            me.startPolling();
        }

        debug(`Pushed new message, length: ${me.length}`);
    }

    /**
     * Inserts message in a first position of MessageBuffer
     * Emits next events:
     *      - notEmpty
     * @param message
     */
    unshift(message) {
        const me = this;
        const sizeOfMessage = sizeof(message.message);

        if (me.getFreeMemory() < sizeOfMessage) {
            throw new FullMessageBufferError(message.message);
        }

        me.fifo.unshift(message);
        me._incrementDataSize(sizeOfMessage);

        if (me._isPollingInStop === true && me._enablePolling === true) {
            me.startPolling();
        }

        debug(`Unshifted new message, length: ${me.length}`);
    }

    /**
     * Returns an earliest message from MessageBuffer and removes it
     * Emits next events:
     *      - empty
     * @returns {*}
     */
    shift() {
        const me = this;
        const message = me.fifo.shift();

        me._decrementDataSize(sizeof(message));

        debug(`Shifted message, length: ${me.length}`);

        return message;
    }

    /**
     *
     * @returns {Array}
     */
    getBatch() {
        const me = this;
        const result = [];

        while (me.length) {
            result.push(me.shift());
        }

        return result;
    }

    /**
     * Iterates over each item of MessageBuffer
     * @param cb
     */
    forEach(cb) {
        const me = this;

        me.fifo.forEach(cb);
    }

    /**
     * Clears MessageBuffer
     * Emits next events:
     *      - empty
     */
    clear() {
        const me = this;

        me.fifo.clear();
        me._resetDataSize();

        debug(`Buffer cleared`);
    }

    /**
     * Returns free memory in bytes
     * @returns {Number}
     */
    getFreeMemory() {
        const me = this;

        return me.freeMemory;
    }

    /**
     * Returns fill percentage
     * @returns {String}
     */
    getFillPercentage() {
        const me = this;

        return (me.dataSize * 100 / me.maxDataSizeB).toFixed(2);
    }

    /**
     * Start buffer polling
     */
    startPolling() {
       const me = this;

       me._initPollingInterval();
       me._isPollingInStop = false;

        debug(`Polling started`);
    }

    /**
     * Stop buffer polling
     */
    stopPolling() {
        const me = this;

        clearInterval(me._pollingIntervalHandler);
        me._isPollingInStop = true;

        debug(`Polling stopped`);
    }

    /**
     * Restart buffer polling
     */
    restartPolling() {
        const me = this;

        me.stopPolling();
        me.startPolling();
    }

    /**
     *
     */
    enablePolling() {
        const me = this;

        me._enablePolling = true;
    }

    /**
     *
     */
    disablePolling() {
        const me = this;

        me._enablePolling = false;
    }

    /**
     * Increment data size counter
     * @param bytesAmount
     * @private
     */
    _incrementDataSize(bytesAmount) {
        const me = this;

        me.dataSize += bytesAmount;
        me._checkMemoryUsage();
    }

    /**
     * Decrement data size counter
     * @param bytesAmount
     * @private
     */
    _decrementDataSize(bytesAmount) {
        const me = this;

        me.dataSize -= bytesAmount;
        me._checkMemoryUsage();
    }

    /**
     * Reset to 0 data size counter
     * @private
     */
    _resetDataSize() {
        const me = this;

        me.dataSize = 0;
        me._checkMemoryUsage();
    }

    /**
     * Monitoring memory usage and firing corresponding events
     * @private
     */
    _checkMemoryUsage() {
        const me = this;

        me.freeMemory = me.maxDataSizeB - me.dataSize;
    }

    /**
     * Initialize polling interval
     * @event poll
     * @private
     */
    _initPollingInterval() {
        const me = this;

        me._pollingIntervalHandler = setInterval(() => {
            if (me.length > 0) {
                me.emit(MessageBuffer.POLL_EVENT, me.getBatch());
            } else {
                me.stopPolling();
            }
        }, 0);
    }
}


module.exports = MessageBuffer;