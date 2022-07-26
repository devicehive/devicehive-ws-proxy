/**
 * DeviceHive Plugin Error
 */
class PluginError extends Error {
    /**
     * @param {string} message
     * @param {Object} messageObject
     * @constructor
     */
    constructor(message, messageObject) {
        super(message);

        this.messageObject = messageObject;
    }

    /**
     * Message object getter
     * @return {Object}
     */
    get messageObject() {
        return this._messageObject;
    }

    /**
     * Message object setter
     * @param {Object} value
     */
    set messageObject(value) {
        this._messageObject = value;
    }
}

module.exports = PluginError;
