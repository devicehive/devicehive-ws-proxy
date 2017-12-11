const FIFO = require(`fifo`);
const EventEmitter = require(`events`);
const FullMessageBufferError = require(`../lib/errors/messageBuffer/FullMessageBufferError`);
const sizeof = require('object-sizeof');
const debug = require(`debug`)(`messagebuffer`);


/**
 * Message buffer class. Implements FIFO stack functionality
 */
class MessageBuffer extends EventEmitter {

    /**
     * Creates new MessageBuffer
     * @param maxDataSizeMB maximum messages data size in buffer (in MB)
     */
    constructor(maxDataSizeMB) {
        super();

        const me = this;

        me.fifo = new FIFO();
        me.maxDataSizeB = me.freeMemory = maxDataSizeMB * 1048576;
        me.dataSize = 0;

        debug(`Maximum size of message buffer: ${maxDataSizeMB} Mb`);
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
        const sizeOfMessage = sizeof(message);

        if (me.getFreeMemory() < sizeOfMessage) {
            throw new FullMessageBufferError(message.message);
        }

        me.fifo.push(message);
        me._incrementDataSize(sizeOfMessage);

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
        const sizeOfMessage = sizeof(message);

        if (me.getFreeMemory() < sizeOfMessage) {
            throw new FullMessageBufferError(message.message);
        }

        me.fifo.unshift(message);
        me._incrementDataSize(sizeOfMessage);

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

    getFreeMemory() {
        const me = this;

        return me.freeMemory;
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
}


module.exports = MessageBuffer;