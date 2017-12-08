
class PluginError extends Error {

    constructor(message, messageId) {
        super(message);

        const me = this;

        me.messageId = messageId;
    }

    get messageId() {
        const me = this;

        return me._messageId;
    }

    set messageId(value) {
        const me = this;

        me._messageId = value;
    }
}


module.exports = PluginError;