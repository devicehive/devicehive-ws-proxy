const ProxyConfig = require(`./ProxyConfig`);
const FIFO = require(`fifo`);
const EventEmitter = require(`events`);
const debug = require(`debug`)(`messagebuffer`);

//TODO
function memorySizeOf(obj) {
    var bytes = 0;

    function sizeOf(obj) {
        if(obj !== null && obj !== undefined) {
            switch(typeof obj) {
                case 'number':
                    bytes += 8;
                    break;
                case 'string':
                    bytes += obj.length * 2;
                    break;
                case 'boolean':
                    bytes += 4;
                    break;
                case 'object':
                    var objClass = Object.prototype.toString.call(obj).slice(8, -1);
                    if(objClass === 'Object' || objClass === 'Array') {
                        for(var key in obj) {
                            if(!obj.hasOwnProperty(key)) continue;
                            sizeOf(obj[key]);
                        }
                    } else bytes += obj.toString().length * 2;
                    break;
            }
        }
        return bytes;
    };

    function formatByteSize(bytes) {
        if(bytes < 1024) return bytes + " bytes";
        else if(bytes < 1048576) return(bytes / 1024).toFixed(3) + " KiB";
        else if(bytes < 1073741824) return(bytes / 1048576).toFixed(3) + " MiB";
        else return(bytes / 1073741824).toFixed(3) + " GiB";
    };

    return formatByteSize(sizeOf(obj));
};

/**
 * Message buffer class. Implements FIFO stack functionality
 * @event notEmpty
 * @event empty
 * @event filled25
 * @event filled50
 * @event filled75
 * @event full
 */
class MessageBuffer extends EventEmitter {

    static get NOT_EMPTY_EVENT() { return `notEmpty`; }
    static get EMPTY_EVENT() { return `empty`; }
    static get FILLED_25_EVENT() { return `filled25`; }
    static get FILLED_50_EVENT() { return `filled50`; }
    static get FILLED_75_EVENT() { return `filled75`; }
    static get FULL_EVENT() { return `full`; }


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