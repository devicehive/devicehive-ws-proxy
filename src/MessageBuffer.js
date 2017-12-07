const FIFO = require(`fifo`);
const EventEmitter = require(`events`);
const debug = require(`debug`)(`messagebuffer`);


/**
 *
 * @event notEmpty
 * @event empty
 */
class MessageBuffer extends EventEmitter {

    constructor() {
        super();

        const me = this;

        me.fifo = new FIFO();
    }

    get length() {
        const me = this;

        return me.fifo.length;
    }

    push(message) {
        const me = this;
        const wasEmpty = me.length === 0;

        me.fifo.push(message);

        debug(`Pushed new message, length: ${me.length}`);

        if (wasEmpty) {
            me.emit(`notEmpty`);
        }
    }

    unshift(message) {
        const me = this;
        const wasEmpty = me.length === 0;

        me.fifo.unshift(message);

        debug(`Unshifted new message, length: ${me.length}`);

        if (wasEmpty) {
            me.emit(`notEmpty`);
        }
    }

    shift() {
        const me = this;
        const willBeEmpty = me.length === 1;
        const message = me.fifo.shift();

        debug(`Shifted message, length: ${me.length}`);

        if (willBeEmpty) {
            me.emit(`empty`);
        }

        return message;
    }

    forEach(cb) {
        const me = this;

        me.fifo.forEach(cb);
    }

    clear() {
        const me = this;

        me.fifo.clear();

        debug(`Buffer cleared`);

        me.emit(`empty`);
    }
}

module.exports = new MessageBuffer();