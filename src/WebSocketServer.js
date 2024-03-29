const CONST = require(`./constants.json`);
const ProxyConfig = require(`../config`).proxy;
const EventEmitter = require(`events`);
const WebSocket = require(`ws`);
const Utils = require(`../utils`);
const debug = require(`debug`)(`websocketserver`);
const shortId = require("shortid");

/**
 * Web Socket server class
 * @event clientConnect
 * @event clientMessage
 * @event clientDisconnect
 */
class WebSocketServer extends EventEmitter {
    /**
     * @return {string}
     */
    static get CLIENT_CONNECT_EVENT() {
        return `clientConnect`;
    }
    /**
     * @return {string}
     */
    static get CLIENT_MESSAGE_EVENT() {
        return `clientMessage`;
    }
    /**
     * @return {string}
     */
    static get CLIENT_DISCONNECT_EVENT() {
        return `clientDisconnect`;
    }
    /**
     * @return {number}
     */
    static get WS_OPEN_STATE() {
        return 1;
    }

    /**
     * Creates new WebSocketServer
     * @constructor
     */
    constructor() {
        super();

        this.clientIdMap = new Map();
        this.wsServer = new WebSocket.Server({
            host: ProxyConfig.WEB_SOCKET_SERVER_HOST,
            port: ProxyConfig.WEB_SOCKET_SERVER_PORT,
            clientTracking: true,
        });

        this.wsServer.on(`connection`, (ws) => this._processNewConnection(ws));

        this.wsServer.on(`error`, (error) => {
            debug(`Server error ${error}`);
        });

        this.wsServer.on(`listening`, () => {
            debug(
                `Server starts listening on ${ProxyConfig.WEB_SOCKET_SERVER_HOST}:${ProxyConfig.WEB_SOCKET_SERVER_PORT}`
            );
        });

        this._setupPingInterval();
    }

    /**
     * Returns set of active WebSocket clients
     * @return {Set}
     */
    getClientsSet() {
        return this.wsServer.clients;
    }

    /**
     * Returns WebSocket client by client id
     * @param {string} id
     * @return {String}
     */
    getClientById(id) {
        return this.clientIdMap.get(id);
    }

    /**
     * Sends message to client by client id
     * @param {string} id
     * @param {string} data
     */
    send(id, data) {
        const client = this.getClientById(id);

        if (client && client.readyState === WebSocketServer.WS_OPEN_STATE) {
            client.send(data);
        }
    }

    /**
     * Sets up new WebSocket connection. Adds message, close amd pong listeners
     * @param {Object} ws - WebSocket handler
     * @private
     */
    _processNewConnection(ws) {
        const clientId = shortId.generate();

        this.clientIdMap.set(clientId, ws);
        ws.isAlive = true;

        debug(`New connection with id ${clientId} established`);
        this.emit(WebSocketServer.CLIENT_CONNECT_EVENT, clientId);

        ws.on(`message`, (data) => {
            debug(`New message from client ${clientId}`);
            this.emit(WebSocketServer.CLIENT_MESSAGE_EVENT, clientId, data);
        });

        ws.on(`close`, (code, reason) => {
            debug(
                `Client ${clientId} has closed the connection with code: ${code} and reason: ${reason}`
            );
            this.clientIdMap.delete(clientId);
            this.emit(WebSocketServer.CLIENT_DISCONNECT_EVENT, clientId);
        });

        ws.on(`pong`, () => {
            ws.isAlive = true;
        });

        ws.on(`error`, (error) => {
            debug(`WebSocket client (${clientId}) error: ${error}`);
        });
    }

    /**
     * Sets up interval to ping clients
     * Interval time is configured by "WEB_SOCKET_PING_INTERVAL_SEC" field of ProxyConfig
     * @private
     */
    _setupPingInterval() {
        setInterval(() => {
            this.getClientsSet().forEach((ws) => {
                if (
                    process.env.NODE_ENV !== CONST.DEVELOPMENT &&
                    ws.isAlive === false
                ) {
                    return ws.terminate();
                }

                ws.isAlive = false;
                ws.ping(() => {});
            });
        }, ProxyConfig.WEB_SOCKET_PING_INTERVAL_SEC * Utils.MS_IN_S);
    }
}

module.exports = WebSocketServer;
