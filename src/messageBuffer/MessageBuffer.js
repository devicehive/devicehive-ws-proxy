const Config = require(`../../config`).messageBuffer;
const Utils = require(`../../utils`);
const FIFO = require(`fifo`);
const EventEmitter = require(`events`);
const FullMessageBufferError = require(`../../lib/errors/messageBuffer/FullMessageBufferError`);
const sizeof = require('object-sizeof');
const debug = require(`debug`)(`messagebuffer`);
const CircularBuffer = require("circular-buffer");


/**
 * Message buffer class. Implements FIFO stack functionality
 */
class MessageBuffer extends EventEmitter {

    static get POLL_EVENT() { return `poll` };
    static get LOAD_CHANGED_EVENT() { return `loadChanged` };

    /**
     * TODO
     * @param loadSize
     * @param loadStep
     * @returns {number}
     */
    static calculatePollingInterval(loadSize, loadStep) {
        const result =  loadStep * 10;

        return !result ?
            result : 5 > Config.BUFFER_POLLING_MESSAGE_AMOUNT ?
                Config.BUFFER_POLLING_MESSAGE_AMOUNT : result;
    }

    /**
     * TODO
     * @param loadSize
     * @param timeInterval
     */
    static calculateMaxBatchSize(loadSize, timeInterval) {
        return Math.floor(!loadSize ? loadSize : loadSize / (Utils.MS_IN_S / timeInterval) / 100);
    }

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
        me.isPollingInStop = true;
        me.enablePolling = false;
        me.loadAnalyzerHandler = null;
        me.loadSizeCounter = 0;
        me.memoryLoadPerSec = 0;
        me.bufferPollingInterval = Config.BUFFER_POLLING_INTERVAL_MS;
        me.maxBatchSize = 0;

        debug(`Maximum size of message buffer: ${Config.MAX_SIZE_MB} Mb`);
        debug(`Polling interval: ${Config.BUFFER_POLLING_INTERVAL_MS} ms`);
        debug(`Message amount per polling cycle: ${Config.BUFFER_POLLING_MESSAGE_AMOUNT}`);

        me.on(MessageBuffer.LOAD_CHANGED_EVENT, (loadSize, loadStep) => {
            me.bufferPollingInterval = 50;// MessageBuffer.calculatePollingInterval(loadSize, loadStep);
            me.maxBatchSize = 0; //MessageBuffer.calculateMaxBatchSize(loadSize, me.bufferPollingInterval);

            me.restartPolling();

            debug(`New polling metrics: Polling Interval: ${me.bufferPollingInterval}, Maximum Batch Size: ${me.maxBatchSize}`);
        });

        me._initLoadAnalyzerInterval();
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

        if (me.isPollingInStop === true && me.enablePolling === true) {
            me.startPolling();
        }

        //debug(`Pushed new message, length: ${me.length}`);
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

        if (me.isPollingInStop === true && me.enablePolling === true) {
            me.startPolling();
        }

        //debug(`Unshifted new message, length: ${me.length}`);
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

        //debug(`Shifted message, length: ${me.length}`);

        return message;
    }

    /**
     *
     * @param maxBatchSize
     * @returns {Array}
     */
    getBatch(maxBatchSize) {
        const me = this;
        const result = [];
        let batchSize = 0;

        if (maxBatchSize !== 0) {
            while (batchSize < maxBatchSize) {
                if (me.length === 0) {
                    break;
                } else {
                    const message = me.shift();

                    batchSize += message.size;
                    result.push(message);
                }
            }
        } else {
            if (me.length > 0) {
                const counter = me.length < Config.BUFFER_POLLING_MESSAGE_AMOUNT ?
                    me.length : Config.BUFFER_POLLING_MESSAGE_AMOUNT;

                for (let messageCounter = 0; messageCounter < counter; messageCounter++) {
                    result.push(me.shift());
                }
            }
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

    getMemoryLoadPerSec() {
        const me = this;

        return me.memoryLoadPerSec;
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
       me.isPollingInStop = false;

        debug(`Polling started`);
    }

    /**
     * Stop buffer polling
     */
    stopPolling() {
        const me = this;

        clearInterval(me.pollingIntervalHandler);
        me.isPollingInStop = true;

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
     * Increment data size counter
     * @param bytesAmount
     * @private
     */
    _incrementDataSize(bytesAmount) {
        const me = this;

        me.dataSize += bytesAmount;
        me._increaseLoadSizeCounter(bytesAmount);
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

        me.pollingIntervalHandler = setInterval(() => {
            if (me.length > 0) {
                me.emit(MessageBuffer.POLL_EVENT, me.getBatch(me.maxBatchSize));
            } else {
                me.stopPolling();
            }
        }, me.bufferPollingInterval);
    }

    /**
     *
     * @param value
     * @private
     */
    _increaseLoadSizeCounter(value) {
        const me = this;

        me.loadSizeCounter += value;
    }

    /**
     *
     * @private
     */
    _resetLoadSizeCounter() {
        const me = this;

        me.loadSizeCounter = 0;
    }

    /**
     *
     * @returns {number}
     * @private
     */
    _getLoadSizeCounter() {
        const me = this;

        return me.loadSizeCounter;
    }

    /**
     *
     * @param value
     * @private
     */
    _setMemoryLoadPerSec(value) {
        const me = this;

        me.memoryLoadPerSec = value;
    }

    /**
     * Initialize load analyzer interval
     * @event poll
     * @private
     */
    _initLoadAnalyzerInterval() {
        const me = this;
        const previousLoadSize = new CircularBuffer(Config.LOAD_ANALYZER_FILTER_DEPTH);
        let activeLoadStep = 0;

        me.loadAnalyzerHandler = setInterval(() => {
            let averageLoadSize, currentStep;
            const currentLoadSize = me._getLoadSizeCounter() /  Config.LOAD_ANALYZER_INTERVAL_SEC;
            const loadBalanceStepB = Config.LOAD_BALANCE_STEP_KB * Utils.B_IN_KB;

            previousLoadSize.enq(currentLoadSize);
            averageLoadSize = Math.floor(previousLoadSize
                .toarray()
                .reduce((prev, value) => prev + value, 0) / Config.LOAD_ANALYZER_FILTER_DEPTH);


            me._resetLoadSizeCounter();
            me._setMemoryLoadPerSec(currentLoadSize);

            currentStep = Math.floor(averageLoadSize / loadBalanceStepB);

            if (activeLoadStep !== currentStep && (me.length === 0 || activeLoadStep < currentStep)) {
                activeLoadStep = currentStep;
                me.emit(MessageBuffer.LOAD_CHANGED_EVENT, averageLoadSize, activeLoadStep);
            }
        }, Config.LOAD_ANALYZER_INTERVAL_SEC * Utils.MS_IN_S);
    }
}


module.exports = MessageBuffer;