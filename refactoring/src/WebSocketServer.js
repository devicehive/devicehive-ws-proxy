const EventEmitter = require(`events`);
const WebSocket = require(`ws`);
const debug = require(`debug`)(`websocketserver`);
const uuid = require(`uuid/v1`);


/**
 *
 * @event clientConnect
 * @event clientMessage
 * @event clientDisconnect
 */
class WebSocketServer extends EventEmitter {

	static get CLIENT_CONNECT_EVENT() { return `clientConnect`; }
	static get CLIENT_MESSAGE_EVENT() { return `clientMessage`; }
	static get CLIENT_DISCONNECT_EVENT() { return `clientDisconnect`; }

	constructor() {
		super();

		const me = this;

		me.isReady = false;
		me.clientIdMap = new Map();
		me.wsServer = new WebSocket.Server({
			host: `localhost`,
			port: `3000`,
			clientTracking: true,
			verifyClient: () => true
		});

		me.wsServer.on(`connection`, (ws, req) => me._processNewConnection(ws));

		me.wsServer.on(`error`, (error) => {
			debug(`Server error ${error}`);
			me.isReady = true
		});

		me.wsServer.on(`listening`, () => {
			debug(`Server starts listening`);
			me.isReady = true
		});
	}

	getClientsSet() {
		const me = this;

		return me.wsServer.clients;
	}

	getClientById(id) {
		const me = this;

		return me.clientIdMap.get(id);
	}

	_processNewConnection(ws) {
		const me = this;
		const clientId = uuid();

		me.clientIdMap.set(clientId, ws);

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
	}
}

module.exports = new WebSocketServer();