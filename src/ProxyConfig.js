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
        me.ACK_ON_EVERY_MESSAGE_ENABLED = Utils.isTrue(Utils.value(process.env[`PROXY.ACK_ON_EVERY_MESSAGE_ENABLED`], Config.ACK_ON_EVERY_MESSAGE_ENABLED));
        me.AUTH_SERVICE_ENDPOINT = Utils.value(process.env[`PROXY.AUTH_SERVICE_ENDPOINT`], Config.AUTH_SERVICE_ENDPOINT);
        me.COMMUNICATOR_TYPE = Utils.value(process.env[`PROXY.COMMUNICATOR_TYPE`], Config.COMMUNICATOR_TYPE);
        me.ENABLE_PLUGIN_MANGER = Utils.isTrue(Utils.value(process.env[`PROXY.ENABLE_PLUGIN_MANGER`], Config.ENABLE_PLUGIN_MANGER));
        me.APP_LOG_LEVEL = Utils.value(process.env[`PROXY.APP_LOG_LEVEL`], Config.APP_LOG_LEVEL);
        me.MESSAGE_BUFFER = {
            BUFFER_POLLING_INTERVAL_MS: Utils.value(process.env[`PROXY.MESSAGE_BUFFER.BUFFER_POLLING_INTERVAL_MS`], Config.MESSAGE_BUFFER.BUFFER_POLLING_INTERVAL_MS),
            BUFFER_POLLING_MESSAGE_AMOUNT: Utils.value(process.env[`PROXY.MESSAGE_BUFFER.BUFFER_POLLING_MESSAGE_AMOUNT`], Config.MESSAGE_BUFFER.BUFFER_POLLING_MESSAGE_AMOUNT),
            MAX_SIZE_MB: Utils.value(process.env[`PROXY.MESSAGE_BUFFER.MAX_SIZE_MB`], Config.MESSAGE_BUFFER.MAX_SIZE_MB)
        }
    }
}


module.exports = new ProxyConfig();