const webSocketServer = require(`./WebSocketServer`);


webSocketServer.on(`clientConnect`, (clientId) => {
	console.log(clientId);
});

webSocketServer.on(`clientMessage`, (clientId, data) => {
	console.log(clientId, data);
});

webSocketServer.on(`clientDisconnect`, (clientId) => {
	console.log(clientId);
});