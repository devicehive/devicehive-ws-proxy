const { Message, MessageUtils, payload } = require(`devicehive-proxy-message`);
const AuthenticationPluginError = require(`../lib/errors/plugin/AuthenticationPluginError`);
const NotAuthorizedPluginError = require(`../lib/errors/plugin/NotAuthorizedPluginError`);
const NoPermissionsPluginError = require(`../lib/errors/plugin/NoPermissionsPluginError`);
const NotEnabledPluginError = require(`../lib/errors/plugin/NotEnabledPluginError`);
const EventEmitter = require(`events`);
const request = require(`request`);
const jwt = require('jsonwebtoken');
const debug = require(`debug`)(`pluginmanager`);
const TokenPayload = payload.TokenPayload;


/**
 * Plugin manager class
 */
class PluginManager extends EventEmitter {

    static get PLUGIN_ACTIVE_EVENT() { return `pluginActive`; }
    static get PLUGIN_NOT_ACTIVE_EVENT() { return `pluginNotActive`; }
    static get PLUGIN_AUTHENTICATE_RESOURCE_PATH() { return `/token/plugin/authenticate`; }


    /**
     * Creates new PluginManager
     */
    constructor(authServiceEndpoint, disabled) {
        super();

        const me = this;

        me.authServiceEndpoint = authServiceEndpoint;
        me.disabled = disabled;
        me.pluginKeyTokenPayloadMap = new Map();

        if (me.isEnabled()) {
            debug(`Plugin Manager is enabled`);
        }
    }

    /**
     * Authenticates plugin with pluginKey by access token
     * @param pluginKey
     * @param token
     * @returns {Promise<any>}
     */
    authenticate(pluginKey, token) {
        const me = this;

        return new Promise((resolve, reject) => {
            if (!me.isEnabled()) {
                reject(new NotEnabledPluginError());
            } else {
                request({
                    method: `GET`,
                    uri: `${me.authServiceEndpoint}${PluginManager.PLUGIN_AUTHENTICATE_RESOURCE_PATH}?token=${token}`
                }, (err, response, body) => {
                    try {
                        const authenticationResponse = JSON.parse(body);

                        if (!err && response.statusCode === 200) {
                            const tokenPayload = TokenPayload.normalize(jwt.decode(token).payload);

                            debug(`Plugin with key: ${pluginKey} has been authenticated`);

                            me.pluginKeyTokenPayloadMap.set(pluginKey, tokenPayload);

                            resolve(tokenPayload);
                        } else {
                            debug(`Plugin with key: ${pluginKey} has not been authenticated`);

                            reject(new AuthenticationPluginError(authenticationResponse.message));
                        }
                    } catch (error) {
                        reject(error.message);
                    }
                });
            }
        });
    }

    /**
     * TODO separate each check
     * Checks that plugin by pluginKey has permissions for operation
     * Throws next errors:
     *      - NotAuthorizedPluginError
     *      - NoPermissionsPluginError
     * @param pluginKey
     * @param message
     */
    checkConstraints(pluginKey, message) {
        const me = this;

        if (me.isEnabled()) {
            const isAuthenticated = me.isAuthenticated(pluginKey);

            if (!isAuthenticated &&
                message.type !== MessageUtils.PLUGIN_TYPE &&
                message.action !== MessageUtils.AUTHENTICATE_ACTION) {
                throw new NotAuthorizedPluginError(message);
            } else if (isAuthenticated === true) {
                const tokenPayload = me.getPluginTokenPayload(pluginKey);

                switch (message.type) {
                    case MessageUtils.TOPIC_TYPE:
                        switch (message.action) {
                            case MessageUtils.CREATE_ACTION:
                                if (message.payload && message.payload.topicList &&
                                    (message.payload.topicList.length > 1 || message.payload.topicList[0] !== tokenPayload.topic)) {
                                    throw new NoPermissionsPluginError(message);
                                }
                                break;
                            case MessageUtils.SUBSCRIBE_ACTION:
                            case MessageUtils.UNSUBSCRIBE_ACTION:
                                if (message.payload && message.payload.topicList &&
                                    (message.payload.topicList.length > 1 || message.payload.topicList[0] !== tokenPayload.topic)) {
                                    throw new NoPermissionsPluginError(message);
                                }
                                break;
                            case MessageUtils.LIST_ACTION:
                                throw new NoPermissionsPluginError(message);
                        }
                        break;
                    case MessageUtils.NOTIFICATION_TYPE:
                        if (message.payload.topic !== tokenPayload.topic) {
                            throw new NoPermissionsPluginError(message);
                        }
                        break;
                }
            }
        }
    }

    /**
     * Checks that plugin with pluginKey is authenticated
     * @param pluginKey
     * @returns {boolean}
     */
    isAuthenticated(pluginKey) {
        const me = this;

        return me.pluginKeyTokenPayloadMap.has(pluginKey);
    }

    /**
     * Removes authentication for plugin with pluginKey
     * @param pluginKey
     */
    removeAuthentication(pluginKey) {
        const me = this;

        if (me.isEnabled()) {
            me.pluginKeyTokenPayloadMap.delete(pluginKey);
        }
    }

    /**
     * Returns plugin TokenPayload by pluginKey
     * @param pluginKey
     * @returns {TokenPayload}
     */
    getPluginTokenPayload(pluginKey) {
        const me = this;

        return me.pluginKeyTokenPayloadMap.get(pluginKey);
    }

    /**
     * Checking if Plugin Manager is enabled
     * @returns {boolean}
     */
    isEnabled() {
        const me = this;

        return !me.disabled;
    }
}


module.exports = PluginManager;