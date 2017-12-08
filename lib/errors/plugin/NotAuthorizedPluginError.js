const PluginError = require(`./PluginError`);


class NotAuthorizedPluginError extends PluginError {

    constructor(messageId) {
        super(`Plugin not authorized`, messageId);
    }
}


module.exports = NotAuthorizedPluginError;