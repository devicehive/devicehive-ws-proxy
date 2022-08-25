const ProxyError = require(`../ProxyError`);

/**
 * Plugin authorization error
 */
class NotAuthorizedPluginError extends ProxyError {
    /**
     * @param {Object} messageObject
     * @constructor
     */
    constructor(messageObject) {
        super(`Plugin not authorized`, messageObject);
    }
}

module.exports = NotAuthorizedPluginError;
