const PluginError = require(`./PluginError`);


class NoPermissionsPluginError extends PluginError {

    constructor(messageId) {
        super(`Plugin has no permissions for this operation`, messageId);
    }
}


module.exports = NoPermissionsPluginError;
