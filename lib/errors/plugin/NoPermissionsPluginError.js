const PluginError = require(`./PluginError`);


class NoPermissionsPluginError extends PluginError {

    constructor(messageObject) {
        super(`Plugin has no permissions for this operation`, messageObject);
    }
}


module.exports = NoPermissionsPluginError;
