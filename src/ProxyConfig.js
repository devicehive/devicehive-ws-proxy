const Config = require(`./config.json`);
const Utils = require(`../utils`);

class ProxyConfig {
    
    constructor() {
        const me = this;

        me.WEB_SOCKET_SERVER_HOST = Utils.value(process.env[`PROXY.WEB_SOCKET_SERVER_HOST`], Config.WEB_SOCKET_SERVER_HOST);
        me.WEB_SOCKET_SERVER_PORT = Utils.value(process.env[`PROXY.WEB_SOCKET_SERVER_PORT`], Config.WEB_SOCKET_SERVER_PORT);
        me.AUTH_SERVICE_ENDPOINT = Utils.value(process.env[`PROXY.AUTH_SERVICE_ENDPOINT`], Config.AUTH_SERVICE_ENDPOINT);
        me.ENABLE_PLUGIN_MANGER = !!Utils.value(process.env[`PROXY.ENABLE_PLUGIN_MANGER`], Config.ENABLE_PLUGIN_MANGER);
        me.SPECIFIC = {
            BUFFER_POLLING_INTERVAL_MS: Utils.value(process.env[`PROXY.SPECIFIC.BUFFER_POLLING_INTERVAL_MS`], Config.SPECIFIC.BUFFER_POLLING_INTERVAL_MS),
            BUFFER_POLLING_MESSAGE_AMOUNT: Utils.value(process.env[`PROXY.SPECIFIC.BUFFER_POLLING_MESSAGE_AMOUNT`], Config.SPECIFIC.BUFFER_POLLING_MESSAGE_AMOUNT)
        }
    }
}


module.exports = new ProxyConfig();