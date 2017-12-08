const PluginError = require(`./PluginError`);


class AuthenticationPluginError extends PluginError {

    constructor(message = `Plugin authentication error`, messageObject) {
        super(message, messageObject);
    }
}


module.exports = AuthenticationPluginError;
