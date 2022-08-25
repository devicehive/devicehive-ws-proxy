const ProxyError = require(`../ProxyError`);

/**
 * Plugin is not enabled error
 */
class NotEnabledPluginError extends ProxyError {
    /**
     * @param {Object} messageObject
     * @constructor
     */
    constructor(messageObject) {
        super(`Plugin API not enabled`, messageObject);
    }
}

module.exports = NotEnabledPluginError;
