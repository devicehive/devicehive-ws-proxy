const MessagePayload = require(`./MessagePayload`);


class HealthMessagePayload extends MessagePayload {

    static normalize({ prx, mb, mbfp, comm } = {}) {
        return new HealthMessagePayload({
            proxy: prx,
            messageBuffer: mb,
            messageBufferFillPercentage: mbfp,
            communicator: comm
        });
    }

    constructor({ proxy, messageBuffer, messageBufferFillPercentage, communicator } = {}) {
        super();

        const me = this;

        me.proxy = proxy;
        me.messageBuffer = messageBuffer;
        me.messageBufferFillPercentage = messageBufferFillPercentage;
        me.communicator = communicator;
    }

    get proxy() {
        return this._proxy;
    }

    set proxy(value) {
        this._proxy = value;
    }

    get messageBuffer() {
        return this._messageBuffer;
    }

    set messageBuffer(value) {
        this._messageBuffer = value;
    }

    get messageBufferFillPercentage() {
        return this._messageBufferFillPercentage;
    }

    set messageBufferFillPercentage(value) {
        this._messageBufferFillPercentage = value;
    }

    get communicator() {
        return this._communicator;
    }

    set communicator(value) {
        this._communicator = value;
    }

    toObject() {
        const me = this;

        return {
            prx: me.proxy,
            mb: me.messageBuffer,
            mbfp: me.messageBufferFillPercentage,
            comm: me.communicator
        }
    }
}


module.exports = HealthMessagePayload;