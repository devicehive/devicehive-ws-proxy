
class PluginError extends Error {

    constructor(message, messageObject) {
        super(message);

        this.messageObject = messageObject;
    }

    get messageObject() {
        return this._messageObject;
    }

    set messageObject(value) {
        this._messageObject = value;
    }
}


module.exports = PluginError;
