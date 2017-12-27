const ProxyError = require(`../ProxyError`);


class AuthenticationPluginError extends ProxyError {

    constructor(message = `Plugin authentication error`, messageObject) {
        super(message, messageObject);
    }
}


module.exports = AuthenticationPluginError;
