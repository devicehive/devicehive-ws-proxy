const Config = require(`../../config`).pluginManager;
const { MessageUtils, payload } = require(`devicehive-proxy-message`);
const NotAuthorizedPluginError = require(`../../lib/errors/plugin/NotAuthorizedPluginError`);
const NoPermissionsPluginError = require(`../../lib/errors/plugin/NoPermissionsPluginError`);
const NotEnabledPluginError = require(`../../lib/errors/plugin/NotEnabledPluginError`);
const EventEmitter = require(`events`);
const axios = require(`axios`);
const jwt = require("jsonwebtoken");
const debug = require(`debug`)(`pluginmanager`);
const TokenPayload = payload.TokenPayload;

/**
 * Plugin manager class
 */
class PluginManager extends EventEmitter {
    /**
     * @return {string}
     */
    static get PLUGIN_ACTIVE_STATUS() {
        return `ACTIVE`;
    }
    /**
     * @return {string}
     */
    static get PLUGIN_INACTIVE_STATUS() {
        return `INACTIVE`;
    }
    /**
     * @return {string}
     */
    static get PLUGIN_AUTHENTICATE_RESOURCE_PATH() {
        return `/token/plugin/authenticate`;
    }

    /**
     * Creates new PluginManager
     * @param {boolean} disabled
     * @constructor
     */
    constructor(disabled) {
        super();

        this.disabled = disabled;
        this.pluginKeyTokenMap = new Map();
        this.pluginKeyTokenPayloadMap = new Map();

        if (this.isEnabled()) {
            debug(`Plugin Manager is enabled`);
        }
    }

    /**
     * Authenticates plugin with pluginKey by access token
     * @param {string} pluginKey
     * @param {string} token
     * @return {Promise}
     */
    async authenticate(pluginKey, token) {
        if (!this.isEnabled()) {
            throw new NotEnabledPluginError();
        } else {
            try {
                await axios.get(
                    `${Config.AUTH_SERVICE_ENDPOINT}${PluginManager.PLUGIN_AUTHENTICATE_RESOURCE_PATH}`,
                    {
                        params: {
                            token,
                        },
                    }
                );

                const tokenPayload = TokenPayload.normalize(
                    jwt.decode(token, {}).payload
                );

                debug(`Plugin with key: ${pluginKey} has been authenticated`);

                this.pluginKeyTokenMap.set(pluginKey, token);
                this.pluginKeyTokenPayloadMap.set(pluginKey, tokenPayload);

                await this.updatePlugin(
                    pluginKey,
                    PluginManager.PLUGIN_ACTIVE_STATUS
                );

                return tokenPayload;
            } catch (error) {
                debug(`Unexpected error: ${error.message}`);
                throw error;
            }
        }
    }

    /**
     * Update plugin
     * @param {string} pluginKey
     * @param {string} status
     */
    async updatePlugin(pluginKey, status) {
        const tokenPayload = this.pluginKeyTokenPayloadMap.get(pluginKey);

        if (tokenPayload) {
            try {
                await axios.put(
                    `${Config.PLUGIN_MANAGEMENT_SERVICE_ENDPOINT}/plugin`,
                    null,
                    {
                        params: {
                            status: status,
                            topicName: tokenPayload.topic,
                        },
                        headers: {
                            Authorization: `Bearer ${this.pluginKeyTokenMap.get(
                                pluginKey
                            )}`,
                        },
                    }
                );

                debug(
                    `Plugin ${pluginKey} has changed it's state to ${status}`
                );
            } catch (error) {
                debug(
                    `Error while updating plugin (${pluginKey}) status: ${error}`
                );
            }
        }
    }

    /**
     * TODO separate each check
     * Checks that plugin by pluginKey has permissions for operation
     * Throws next errors:
     *      - NotAuthorizedPluginError
     *      - NoPermissionsPluginError
     * @param {string} pluginKey
     * @param {Object} message
     */
    checkConstraints(pluginKey, message) {
        if (this.isEnabled()) {
            const isAuthenticated = this.isAuthenticated(pluginKey);

            if (
                !isAuthenticated &&
                message.type !== MessageUtils.PLUGIN_TYPE &&
                message.action !== MessageUtils.AUTHENTICATE_ACTION &&
                message.type !== MessageUtils.HEALTH_CHECK_TYPE
            ) {
                throw new NotAuthorizedPluginError(message);
            } else if (isAuthenticated === true) {
                const tokenPayload = this.getPluginTokenPayload(pluginKey);

                switch (message.type) {
                    case MessageUtils.TOPIC_TYPE:
                        switch (message.action) {
                            case MessageUtils.CREATE_ACTION:
                                if (
                                    message.payload &&
                                    message.payload.topicList &&
                                    (message.payload.topicList.length > 1 ||
                                        message.payload.topicList[0] !==
                                            tokenPayload.topic)
                                ) {
                                    throw new NoPermissionsPluginError(message);
                                }
                                break;
                            case MessageUtils.SUBSCRIBE_ACTION:
                            case MessageUtils.UNSUBSCRIBE_ACTION:
                                if (
                                    message.payload &&
                                    message.payload.topicList &&
                                    (message.payload.topicList.length > 1 ||
                                        message.payload.topicList[0] !==
                                            tokenPayload.topic)
                                ) {
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
     * @param {string} pluginKey
     * @return {boolean}
     */
    isAuthenticated(pluginKey) {
        return this.pluginKeyTokenPayloadMap.has(pluginKey);
    }

    /**
     * Removes authentication for plugin with pluginKey
     * @param {string} pluginKey
     */
    async removeAuthentication(pluginKey) {
        if (this.isEnabled()) {
            await this.updatePlugin(
                pluginKey,
                PluginManager.PLUGIN_INACTIVE_STATUS
            );

            this.pluginKeyTokenPayloadMap.delete(pluginKey);
            this.pluginKeyTokenMap.delete(pluginKey);
        }
    }

    /**
     * Returns plugin TokenPayload by pluginKey
     * @param {string} pluginKey
     * @return {TokenPayload}
     */
    getPluginTokenPayload(pluginKey) {
        return this.pluginKeyTokenPayloadMap.get(pluginKey);
    }

    /**
     * Checking if Plugin Manager is enabled
     * @return {boolean}
     */
    isEnabled() {
        return !this.disabled;
    }
}

module.exports = PluginManager;
