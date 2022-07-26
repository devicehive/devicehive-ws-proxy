const ProxyError = require(`../ProxyError`);

/**
 * Plugin permission error
 */
class NoPermissionsPluginError extends ProxyError {
    /**
     * @param {Object} messageObject
     * @constructor
     */
    constructor(messageObject) {
        super(`Plugin has no permissions for this operation`, messageObject);
    }
}

module.exports = NoPermissionsPluginError;
