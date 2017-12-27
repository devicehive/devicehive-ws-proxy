const ProxyError = require(`../ProxyError`);


class NoPermissionsPluginError extends ProxyError {

    constructor(messageObject) {
        super(`Plugin has no permissions for this operation`, messageObject);
    }
}


module.exports = NoPermissionsPluginError;
