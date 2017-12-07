const ProxyConfig = require(`./ProxyConfig`);
const Message = require(`../lib/Message`);
const NotificationMessagePayload = require(`../lib/NotificationMessagePayload`);
const TokenPayload = require(`../lib/TokenPayload`);
const request = require(`request`);
const jwt = require('jsonwebtoken');
const debug = require(`debug`)(`pluginmanager`);

/**
 *
 */
class PluginManager {

    static get PLUGIN_AUTHENTICATE_RESOURCE_PATH() { return `/token/plugin/authenticate`; }

    constructor() {
        const me = this;

        me.pluginKeyTokenPayloadMap = new Map();
    }

    authenticate(pluginKey, token) {
        const me = this;

        return new Promise((resolve, reject) => {
            request({
                method: `GET`,
                uri: `${ProxyConfig.AUTH_SERVICE_ENDPOINT}${PluginManager.PLUGIN_AUTHENTICATE_RESOURCE_PATH}?token=${token}`
            }, (err, response, body) => {
                if (!err && response.statusCode === 200) {
                    me.pluginKeyTokenPayloadMap.set(pluginKey, TokenPayload.normalize(jwt.decode(token).payload));

                    debug(`Plugin with key: ${pluginKey} has been authenticated`);

                    resolve(body);
                } else {
                    debug(`Plugin with key: ${pluginKey} has not been authenticated`);

                    reject(JSON.parse(body).message);
                }
            });
        });
    }

    checkConstraints(pluginKey, message) {
        const me = this;
        const isAuthenticated = me.isAuthenticated(pluginKey);

        if (!isAuthenticated &&
            message.type !== Message.PLUGIN_TYPE &&
            message.action !== Message.AUTHENTICATE_ACTION) {
            throw new Error(`Not authorized`);
        } else if (isAuthenticated === true) {
            const tokenPayload = me.getPluginToken(pluginKey);

            switch(message.type) {
                case Message.TOPIC_TYPE:
                    switch(message.action) {
                        case Message.CREATE_ACTION:
                            if (message.payload &&
                                (message.payload.length > 1 || message.payload[0] !== tokenPayload.topic)) {
                                throw new Error(`Plugin has no permissions for this operation`);
                            }
                            break;
                        case Message.SUBSCRIBE_ACTION:
                        case Message.UNSUBSCRIBE_ACTION:
                            if (message.payload && message.payload.t &&
                                (message.payload.t.length > 1 || message.payload.t[0] !== tokenPayload.topic)) {
                                throw new Error(`Plugin has no permissions for this operation`);
                            }
                            break;
                        case Message.LIST_ACTION:
                            throw new Error(`Plugin has no permissions for this operation`);
                    }
                    break;
                case Message.NOTIFICATION_TYPE:
                    if (NotificationMessagePayload.normalize(message.payload).topic !== tokenPayload.topic) {
                        throw new Error(`Plugin has no permissions for this operation`);
                    }
                    break;
            }
        }
    }

    isAuthenticated(pluginKey) {
        const me = this;

        return me.pluginKeyTokenPayloadMap.has(pluginKey);
    }

    removeAuthentication(pluginKey) {
        const me = this;

        me.pluginKeyTokenPayloadMap.delete(pluginKey);
    }

    getPluginToken(pluginKey) {
        const me = this;

        return me.pluginKeyTokenPayloadMap.get(pluginKey);
    }

    isEnabled() {
        return ProxyConfig.ENABLE_PLUGIN_MANGER === true;
    }
}


module.exports = new PluginManager();