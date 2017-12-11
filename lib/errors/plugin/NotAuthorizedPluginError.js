const ProxyError = require(`../ProxyError`);


class NotAuthorizedPluginError extends ProxyError {

    constructor(messageObject) {
        super(`Plugin not authorized`, messageObject);
    }
}


module.exports = NotAuthorizedPluginError;