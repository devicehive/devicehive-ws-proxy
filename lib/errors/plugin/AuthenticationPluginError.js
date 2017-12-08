const PluginError = require(`./PluginError`);


class NoPermissionsPluginError extends PluginError {

    constructor(message = `Plugin authentication error`) {
        super(message);
    }
}


module.exports = NoPermissionsPluginError;
