const ProxyError = require(`../ProxyError`);

/**
 * Plugin authentication error
 */
class AuthenticationPluginError extends ProxyError {
    /**
     * @param {string} message
     * @param {Object} messageObject
     * @constructor
     */
    constructor(message = `Plugin authentication error`, messageObject) {
        super(message, messageObject);
    }
}

module.exports = AuthenticationPluginError;
