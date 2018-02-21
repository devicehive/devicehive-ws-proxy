const CONST = require(`./constants.json`);
const ProxyConfig = require(`../config`).proxy;
const EventEmitter = require(`events`);
const WebSocket = require(`ws`);
const Utils = require(`../utils`);
const debug = require(`debug`)(`websocketserver`);
const shortId = require('shortid');
const http = require(`http`);


/**
 * Web Socket server class
 * @event clientConnect
 * @event clientMessage
 * @event clientDisconnect
 */
class WebSocketServer extends EventEmitter {

	static get CLIENT_CONNECT_EVENT() { return `clientConnect`; }
	static get CLIENT_MESSAGE_EVENT() { return `clientMessage`; }
	static get CLIENT_DISCONNECT_EVENT() { return `clientDisconnect`; }
	static get WS_OPEN_STATE() { return 1; }

    /**
	 * Creates new WebSocketServer
     */
	constructor() {
		super();

		const me = this;

        me.isReady = false;
        me.clientIdMap = new Map();

        // if (cluster.isMaster) {
        // 	console.log(`master`);
        //     me.wsServer = new WebSocket.Server({
        //         host: ProxyConfig.WEB_SOCKET_SERVER_HOST,
        //         port: ProxyConfig.WEB_SOCKET_SERVER_PORT,
        //         clientTracking: true
        //     });
        // } else {

            const server = new http.createServer().listen(0, 'localhost');

            me.wsServer = new WebSocket.Server({ server });

            process.on('message', function(message, connection) {
                console.log(process.pid, message);

                if (message !== 'sticky-session:connection') { return; }

                server.emit('connection', connection);
                connection.resume();
            });
		//}

        me.wsServer.on(`connection`, (ws, req) => me._processNewConnection(ws));

        me.wsServer.on(`error`, (error) => {
            debug(`Server error ${error}`);
            me.isReady = true
        });

        me.wsServer.on(`listening`, () => {
            debug(`Server starts listening on ${ProxyConfig.WEB_SOCKET_SERVER_HOST}:${ProxyConfig.WEB_SOCKET_SERVER_PORT}`);
            me.isReady = true
        });

        me._setupPingInterval();
	}

    /**
	 * Returns set of active WebSocket clients
     * @returns {Set}
     */
	getClientsSet() {
		const me = this;

		return me.wsServer.clients;
	}

    /**
	 * Returns WebSocket client by client id
     * @param id
     * @returns {String}
     */
	getClientById(id) {
		const me = this;

		return me.clientIdMap.get(id);
	}

    /**
	 * Sends message to client by client id
     * @param id
     * @param data
     */
	send(id, data) {
	    const me = this;
	    const client = me.getClientById(id);

	    if (client && client.readyState === WebSocketServer.WS_OPEN_STATE) {
		    client.send(data);
	    }
    }

    /**
	 * Sets up new WebSocket connection. Adds message, close amd pong listeners
     * @param ws
     * @private
     */
	_processNewConnection(ws) {
		const me = this;
		const clientId = shortId.generate();

		me.clientIdMap.set(clientId, ws);
        ws.isAlive = true;

		debug(`New connection with id ${clientId} established`);
		me.emit(WebSocketServer.CLIENT_CONNECT_EVENT, clientId);

		ws.on(`message`, (data) => {
			debug(`New message from client ${clientId}`);
			me.emit(WebSocketServer.CLIENT_MESSAGE_EVENT, clientId, data);
		});

		ws.on(`close`, (code, reason) => {
			debug(`Client ${clientId} has closed the connection with code: ${code} and reason: ${reason}`);
			me.clientIdMap.delete(clientId);
			me.emit(WebSocketServer.CLIENT_DISCONNECT_EVENT, clientId);
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
        const me = this;

        setInterval(() => {
            me.getClientsSet().forEach((ws) => {
                if (process.env.NODE_ENV !== CONST.DEVELOPMENT && ws.isAlive === false) {
                    return ws.terminate();
                }

                ws.isAlive = false;
                ws.ping(Utils.EMPTY_STRING, false, true);
            });
        }, ProxyConfig.WEB_SOCKET_PING_INTERVAL_SEC * Utils.MS_IN_S);
    }
}


module.exports = WebSocketServer;