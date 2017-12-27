
class PluginError extends Error {

    constructor(message, messageObject) {
        super(message);

        const me = this;

        me.messageObject = messageObject;
    }

    get messageObject() {
        const me = this;

        return me._messageObject;
    }

    set messageObject(value) {
        const me = this;

        me._messageObject = value;
    }
}


module.exports = PluginError;