const Config = require(`./config.json`);
const Utils = require(`../utils`);


/**
 * Proxy configuration class
 */
class ProxyConfig {

    /**
     * Creates new ProxyConfig
     * Corresponding config.json file lies in the same directory
     */
    constructor() {
        const me = this;

        me.WEB_SOCKET_SERVER_HOST = Utils.value(process.env[`PROXY.WEB_SOCKET_SERVER_HOST`], Config.WEB_SOCKET_SERVER_HOST);
        me.WEB_SOCKET_SERVER_PORT = Utils.value(process.env[`PROXY.WEB_SOCKET_SERVER_PORT`], Config.WEB_SOCKET_SERVER_PORT);
        me.WEB_SOCKET_PING_INTERVAL_S = Utils.value(process.env[`PROXY.WEB_SOCKET_PING_INTERVAL_S`], Config.WEB_SOCKET_PING_INTERVAL_S);
        me.AUTH_SERVICE_ENDPOINT = Utils.value(process.env[`PROXY.AUTH_SERVICE_ENDPOINT`], Config.AUTH_SERVICE_ENDPOINT);
        me.COMMUNICATOR_TYPE = Utils.value(process.env[`PROXY.COMMUNICATOR_TYPE`], Config.COMMUNICATOR_TYPE);
        me.ENABLE_PLUGIN_MANGER = Utils.value(Utils.isTrue(process.env[`PROXY.ENABLE_PLUGIN_MANGER`]), Config.ENABLE_PLUGIN_MANGER);
        me.APP_LOG_LEVEL = Utils.value(process.env[`PROXY.APP_LOG_LEVEL`], Config.APP_LOG_LEVEL);
        me.SPECIFIC = {
            BUFFER_POLLING_INTERVAL_MS: Utils.value(process.env[`PROXY.SPECIFIC.BUFFER_POLLING_INTERVAL_MS`], Config.SPECIFIC.BUFFER_POLLING_INTERVAL_MS),
            BUFFER_POLLING_MESSAGE_AMOUNT: Utils.value(process.env[`PROXY.SPECIFIC.BUFFER_POLLING_MESSAGE_AMOUNT`], Config.SPECIFIC.BUFFER_POLLING_MESSAGE_AMOUNT)
        }
    }
}


module.exports = new ProxyConfig();