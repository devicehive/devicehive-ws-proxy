const FIFO = require(`fifo`);
const EventEmitter = require(`events`);
const debug = require(`debug`)(`messagebuffer`);


/**
 * Message buffer class. Implements FIFO stack functionality
 * @event notEmpty
 * @event empty
 */
class MessageBuffer extends EventEmitter {

    static get NOT_EMPTY_EVENT() { return `notEmpty`; }
    static get EMPTY_EVENT() { return `empty`; }

    /**
     * Creates new MessageBuffer
     */
    constructor() {
        super();

        const me = this;

        me.fifo = new FIFO();
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
        const wasEmpty = me.length === 0;

        me.fifo.push(message);

        debug(`Pushed new message, length: ${me.length}`);

        if (wasEmpty) {
            me.emit(MessageBuffer.NOT_EMPTY_EVENT);
        }
    }

    /**
     * Inserts message in a first position of MessageBuffer
     * Emits next events:
     *      - notEmpty
     * @param message
     */
    unshift(message) {
        const me = this;
        const wasEmpty = me.length === 0;

        me.fifo.unshift(message);

        debug(`Unshifted new message, length: ${me.length}`);

        if (wasEmpty) {
            me.emit(MessageBuffer.NOT_EMPTY_EVENT);
        }
    }

    /**
     * Returns an earliest message from MessageBuffer and removes it
     * Emits next events:
     *      - empty
     * @returns {*}
     */
    shift() {
        const me = this;
        const willBeEmpty = me.length === 1;
        const message = me.fifo.shift();

        debug(`Shifted message, length: ${me.length}`);

        if (willBeEmpty) {
            me.emit(MessageBuffer.EMPTY_EVENT);
        }

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

        debug(`Buffer cleared`);

        me.emit(MessageBuffer.EMPTY_EVENT);
    }
}


module.exports = MessageBuffer;