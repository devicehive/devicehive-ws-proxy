const Config = require(`../../config`).pluginManager;
const Utils = require(`../../utils`);
const {MessageUtils, payload} = require(`devicehive-proxy-message`);
const NotAuthorizedPluginError = require(`../../lib/errors/plugin/NotAuthorizedPluginError`);
const NoPermissionsPluginError = require(`../../lib/errors/plugin/NoPermissionsPluginError`);
const NotEnabledPluginError = require(`../../lib/errors/plugin/NotEnabledPluginError`);
const EventEmitter = require(`events`);
const axios = require(`axios`);
const jwt = require('jsonwebtoken');
const debug = require(`debug`)(`pluginmanager`);
const TokenPayload = payload.TokenPayload;


/**
 * Plugin manager class
 */
class PluginManager extends EventEmitter {

    static get PLUGIN_ACTIVE_STATUS() {
        return `ACTIVE`;
    }

    static get PLUGIN_INACTIVE_STATUS() {
        return `INACTIVE`;
    }

    static get PLUGIN_AUTHENTICATE_RESOURCE_PATH() {
        return `/token/plugin/authenticate`;
    }

    /**
     * Creates new PluginManager
     */
    constructor(disabled) {
        super();

        const me = this;

        me.disabled = disabled;
        me.pluginKeyTokenMap = new Map();
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
    async authenticate(pluginKey, token) {
        const me = this;

        if (!me.isEnabled()) {
            throw new NotEnabledPluginError();
        } else {
            try {
                await axios.get(`${Config.AUTH_SERVICE_ENDPOINT}${PluginManager.PLUGIN_AUTHENTICATE_RESOURCE_PATH}?token=${token}`)

                const tokenPayload = TokenPayload.normalize(jwt.decode(token, {}).payload);

                debug(`Plugin with key: ${pluginKey} has been authenticated`);

                me.pluginKeyTokenMap.set(pluginKey, token);
                me.pluginKeyTokenPayloadMap.set(pluginKey, tokenPayload);
                me.updatePlugin(pluginKey, PluginManager.PLUGIN_ACTIVE_STATUS);

                return tokenPayload;
            } catch (error) {
                debug(`Unexpected error: ${error.message}`);
                throw error;
            }
        }
    }

    /**
     * Update plugin
     * @param pluginKey
     * @param status
     */
    async updatePlugin(pluginKey, status) {
        const me = this;
        const tokenPayload = me.pluginKeyTokenPayloadMap.get(pluginKey);

        if (tokenPayload) {
            try {
                const queryString = Utils.queryBuilder({
                    status: status,
                    topicName: tokenPayload.topic
                });

                await axios.put(`${Config.PLUGIN_MANAGEMENT_SERVICE_ENDPOINT}/plugin${queryString}`, {
                    headers: {
                        Authorization: `Bearer ${me.pluginKeyTokenMap.get(pluginKey)}`
                    }
                })

                debug(`Plugin ${pluginKey} has changed it's state to ${status}`);
            } catch(error) {
                debug(`Error while updating plugin (${pluginKey}) status: ${error}`);
            }
        }
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
                (message.type !== MessageUtils.PLUGIN_TYPE && message.action !== MessageUtils.AUTHENTICATE_ACTION) &&
                message.type !== MessageUtils.HEALTH_CHECK_TYPE) {
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
            me.updatePlugin(pluginKey, PluginManager.PLUGIN_INACTIVE_STATUS);
            me.pluginKeyTokenPayloadMap.delete(pluginKey);
            me.pluginKeyTokenMap.delete(pluginKey);
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
