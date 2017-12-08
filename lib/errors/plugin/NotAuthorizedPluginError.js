const PluginError = require(`./PluginError`);


class NotAuthorizedPluginError extends PluginError {

    constructor(messageObject) {
        super(`Plugin not authorized`, messageObject);
    }
}


module.exports = NotAuthorizedPluginError;