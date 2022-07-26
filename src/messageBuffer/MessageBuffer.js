const Config = require(`../../config`).messageBuffer;
const Utils = require(`../../utils`);
const FIFO = require(`fifo`);
const EventEmitter = require(`events`);
const FullMessageBufferError = require(`../../lib/errors/messageBuffer/FullMessageBufferError`);
const sizeof = require("object-sizeof");
const debug = require(`debug`)(`messagebuffer`);

/**
 * Message buffer class. Implements FIFO stack functionality
 */
class MessageBuffer extends EventEmitter {
    /**
     * @return {string}
     */
    static get POLL_EVENT() {
        return `poll`;
    }

    /**
     * Creates new MessageBuffer
     * @constructor
     */
    constructor() {
        super();

        this.fifo = new FIFO();
        this.maxDataSizeB = this.freeMemory =
            Config.MAX_SIZE_MB * Utils.B_IN_MB;
        this.dataSize = 0;
        this._pollingIntervalHandler = null;
        this._isPollingInStop = true;
        this._enablePolling = false;

        debug(`Maximum size of message buffer: ${Config.MAX_SIZE_MB} Mb`);
    }

    /**
     * Returns length on MessageBuffer
     * @return {number}
     */
    get length() {
        return this.fifo.length;
    }

    /**
     * Pushes new message to MessageBuffer
     * Emits next events:
     *      - notEmpty
     * @param {Object} message
     */
    push(message) {
        const sizeOfMessage = sizeof(message.message);

        if (this.getFreeMemory() < sizeOfMessage) {
            throw new FullMessageBufferError(message.message);
        }

        message.size = sizeOfMessage;

        this.fifo.push(message);
        this._incrementDataSize(sizeOfMessage);

        if (this._isPollingInStop === true && this._enablePolling === true) {
            this.startPolling();
        }

        debug(`Pushed new message, length: ${this.length}`);
    }

    /**
     * Inserts message in a first position of MessageBuffer
     * Emits next events:
     *      - notEmpty
     * @param {Object} message
     */
    unshift(message) {
        const sizeOfMessage = sizeof(message.message);

        if (this.getFreeMemory() < sizeOfMessage) {
            throw new FullMessageBufferError(message.message);
        }

        this.fifo.unshift(message);
        this._incrementDataSize(sizeOfMessage);

        if (this._isPollingInStop === true && this._enablePolling === true) {
            this.startPolling();
        }

        debug(`Unshifted new message, length: ${this.length}`);
    }

    /**
     * Returns the earliest message from MessageBuffer and removes it
     * Emits next events:
     *      - empty
     * @return {Object}
     */
    shift() {
        const message = this.fifo.shift();

        this._decrementDataSize(sizeof(message));

        debug(`Shifted message, length: ${this.length}`);

        return message;
    }

    /**
     *
     * @return {Array}
     */
    getBatch() {
        const result = [];

        while (this.length) {
            result.push(this.shift());
        }

        return result;
    }

    /**
     * Iterates over each item of MessageBuffer
     * @param {Function} cb
     */
    forEach(cb) {
        this.fifo.forEach(cb);
    }

    /**
     * Clears MessageBuffer
     * Emits next events:
     *      - empty
     */
    clear() {
        this.fifo.clear();
        this._resetDataSize();

        debug(`Buffer cleared`);
    }

    /**
     * Returns free memory in bytes
     * @return {Number}
     */
    getFreeMemory() {
        return this.freeMemory;
    }

    /**
     * Returns fill percentage
     * @return {String}
     */
    getFillPercentage() {
        return ((this.dataSize * 100) / this.maxDataSizeB).toFixed(2);
    }

    /**
     * Start buffer polling
     */
    startPolling() {
        this._initPollingInterval();
        this._isPollingInStop = false;

        debug(`Polling started`);
    }

    /**
     * Stop buffer polling
     */
    stopPolling() {
        clearInterval(this._pollingIntervalHandler);
        this._isPollingInStop = true;

        debug(`Polling stopped`);
    }

    /**
     * Restart buffer polling
     */
    restartPolling() {
        this.stopPolling();
        this.startPolling();
    }

    /**
     *
     */
    enablePolling() {
        this._enablePolling = true;
    }

    /**
     *
     */
    disablePolling() {
        this._enablePolling = false;
    }

    /**
     * Increment data size counter
     * @param {number} bytesAmount
     * @private
     */
    _incrementDataSize(bytesAmount) {
        this.dataSize += bytesAmount;
        this._checkMemoryUsage();
    }

    /**
     * Decrement data size counter
     * @param {number} bytesAmount
     * @private
     */
    _decrementDataSize(bytesAmount) {
        this.dataSize -= bytesAmount;
        this._checkMemoryUsage();
    }

    /**
     * Reset to 0 data size counter
     * @private
     */
    _resetDataSize() {
        this.dataSize = 0;
        this._checkMemoryUsage();
    }

    /**
     * Monitoring memory usage and firing corresponding events
     * @private
     */
    _checkMemoryUsage() {
        this.freeMemory = this.maxDataSizeB - this.dataSize;
    }

    /**
     * Initialize polling interval
     * @event poll
     * @private
     */
    _initPollingInterval() {
        this._pollingIntervalHandler = setInterval(() => {
            if (this.length > 0) {
                this.emit(MessageBuffer.POLL_EVENT, this.getBatch());
            } else {
                this.stopPolling();
            }
        }, 0);
    }
}

module.exports = MessageBuffer;
