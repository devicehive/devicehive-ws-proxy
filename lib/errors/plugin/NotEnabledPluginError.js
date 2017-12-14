const ProxyError = require(`../ProxyError`);


class NotEnabledPluginError extends ProxyError {

    constructor(messageObject) {
        super(`Plugin API not enabled`, messageObject);
    }
}


module.exports = NotEnabledPluginError;
