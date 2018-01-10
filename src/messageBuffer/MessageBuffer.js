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
        me.pollingIntervalHandler = null;

        debug(`Maximum size of message buffer: ${Config.MAX_SIZE_MB} Mb`);
        debug(`Polling interval: ${Config.BUFFER_POLLING_INTERVAL_MS} ms`);
        debug(`Message amount per polling cycle: ${Config.BUFFER_POLLING_MESSAGE_AMOUNT}`);
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

        message.size = sizeOfMessage;

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
        const sizeOfMessage = sizeof(message.message);

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
     *
     * @returns {Array}
     */
    getBatch() {
        const me = this;
        const result = [];
        let batchSize = 0;

        while (batchSize < Config.MAX_BATCH_SIZE) {
            const message = me.shift();

            batchSize += message.size;
            result.push(message);
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

       me.__initPollingInterval();
    }

    /**
     * Stop buffer polling
     */
    stopPolling() {
        const me = this;

        clearInterval(me.pollingIntervalHandler);
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
    __initPollingInterval() {
        const me = this;

        me.pollingIntervalHandler = setInterval(() => {
            if (me.length > 0) {
                const counter = me.length < Config.BUFFER_POLLING_MESSAGE_AMOUNT ?
                    me.length : Config.BUFFER_POLLING_MESSAGE_AMOUNT;

                for (let messageCounter = 0; messageCounter < counter; messageCounter++) {
                    me.emit(MessageBuffer.POLL_EVENT);
                }
            }
        }, Config.BUFFER_POLLING_INTERVAL_MS);
    }
}


module.exports = MessageBuffer;