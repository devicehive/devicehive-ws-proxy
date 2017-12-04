const Config = require(`./config.json`);
const Utils = require(`../utils`);
const Message = require(`../lib/Message`);
const request = require(`request`);
const webSocketServer = require(`./WebSocketServer`);
const messageBuffer = require(`./MessageBuffer`);
const internalCommunicatorFacade = require(`./InternalCommunicatorFacade`);


webSocketServer.on(`clientConnect`, (clientId) => {

});

webSocketServer.on(`clientMessage`, (clientId, data) => {
    try {
        const messages = JSON.parse(data);

        Utils.forEach(messages, (message) => {
            messageBuffer.push({ clientId: clientId, message: Message.normalize(message) });
        });
    } catch (error) {
        respondWithFailure(clientId, error.message);
    }
});

webSocketServer.on(`clientDisconnect`, (clientId) => {

});

messageBuffer.on(`notEmpty`, () => { // TODO
    while (messageBuffer.length) {
        processMessage(messageBuffer.shift());
    }
});


function processMessage({ clientId, message }) {
	if (webSocketServer.isClientAuthenticated(clientId) || message.action === Message.AUTHENTICATE_ACTION) {
		switch (message.type) {
			case Message.TOPIC_TYPE:
				processTopicTypeMessage(clientId, message);
				break;
			case Message.NOTIFICATION_TYPE:
				processNotificationTypeMessage(clientId, message);
				break;
			case Message.PLUGIN_TYPE:
				processPluginTypeMessage(clientId, message);
				break;
			case Message.HEALTH_CHECK_TYPE:
				processHealthTypeMessage(clientId, message);
				break;
			default:
				respondWithFailure(clientId, `Unsupported message type: ${message.type}`, message.id);
				break;
		}
	} else {
		respondWithFailure(clientId, `Unauthorized`, message.id);
	}
}


function processTopicTypeMessage(clientId, message) {
    switch (message.action) {
        case Message.CREATE_ACTION:
            processTopicCreateAction(clientId, message);
            break;
        case Message.LIST_ACTION:
            processTopicListAction(clientId, message);
            break;
        case Message.SUBSCRIBE_ACTION:
            processTopicSubscribeAction(clientId, message);
            break;
        case Message.UNSUBSCRIBE_ACTION:
            processTopicUnsubscribeAction(clientId, message);
            break;
        default:
            respondWithFailure(clientId, `Unsupported topic message action: ${message.action}`, message.id);
            break;
    }
}


function processPluginTypeMessage(clientId, message) {
    switch (message.action) {
        case Message.AUTHENTICATE_ACTION:
            processPluginAuthenticateAction(clientId, message);
            break;
        default:
            respondWithFailure(clientId, `Unsupported plugin message action: ${message.action}`, message.id);
            break;
    }
}


function processNotificationTypeMessage(clientId, message) {
    switch (message.action) {
        case Message.CREATE_ACTION:
            processNotificationCreateAction(clientId, message);
            break;
        default:
            respondWithFailure(clientId, `Unsupported notification message action: ${message.action}`, message.id);
            break;
    }
}


function processHealthTypeMessage(clientId, message) {
    webSocketServer.send(clientId, new Message({
        id: message.id,
        type: Message.HEALTH_CHECK_TYPE,
        status: Message.SUCCESS_STATUS,
        payload: { status: 'available' } //TODO
    }).toString());
}


function processTopicCreateAction(clientId, message) {
	if (Array.isArray(message.payload)) {
		internalCommunicatorFacade.createTopics(message.payload)
			.then((createdTopicList) => {
				webSocketServer.send(clientId, new Message({
					id: message.id,
					type: Message.TOPIC_TYPE,
					action: Message.CREATE_ACTION,
					status: Message.SUCCESS_STATUS,
					payload: createdTopicList
				}).toString());
			})
			.catch((error) => respondWithFailure(clientId, error.message, message.id));
	} else {
		respondWithFailure(clientId, `Payload should consist an array with topics to create`, message.id);
	}
}


function processTopicListAction(clientId, message) {
	internalCommunicatorFacade.listTopics()
		.then((topicsList) => {
			webSocketServer.send(clientId, new Message({
				id: message.id,
				type: Message.TOPIC_TYPE,
				action: Message.LIST_ACTION,
				status: Message.SUCCESS_STATUS,
				payload: topicsList
			}).toString());
		})
		.catch((error) => respondWithFailure(clientId, error.message, message.id));
}


function processTopicSubscribeAction(clientId, message) {
	if (Array.isArray(message.payload.t)) {
	    internalCommunicatorFacade.subscribe(clientId, message.payload.t)
		    .then((topicSubscriptionList) => {
			    webSocketServer.send(clientId, new Message({
				    id: message.id,
				    type: Message.TOPIC_TYPE,
				    action: Message.SUBSCRIBE_ACTION,
				    status: Message.SUCCESS_STATUS,
				    payload: topicSubscriptionList
			    }).toString());
		    })
		    .catch((error) => respondWithFailure(clientId, error.message, message.id));
    } else {
	    respondWithFailure(clientId, `Payload should consist property "t" with list of topics to subscribe`, message.id);
    }
}


function processTopicUnsubscribeAction(clientId, message) {
	if (Array.isArray(message.payload.t)) {
		internalCommunicatorFacade.unsubscribe(clientId, message.payload.t)
			.then(() => {
				webSocketServer.send(clientId, new Message({
					id: message.id,
					type: Message.TOPIC_TYPE,
					action: Message.UNSUBSCRIBE_ACTION,
					status: Message.SUCCESS_STATUS
				}).toString());
			})
			.catch((error) => respondWithFailure(clientId, error.message, message.id));
	} else {
		respondWithFailure(clientId, `Payload should consist property "t" with list of topics to unsubscribe`, message.id);
	}
}


function processNotificationCreateAction(clientId, message) {
	internalCommunicatorFacade.send({
		topic: message.payload.t,
		messages: [ message.payload.m ],
		partition: 0,
		attributes: 0
	}).then(() => {
		webSocketServer.send(clientId, new Message({
			id: message.id,
			type: Message.NOTIFICATION_TYPE,
			action: Message.CREATE_ACTION,
			status: Message.SUCCESS_STATUS
		}).toString());
	}).catch((error) => respondWithFailure(clientId, error.message, message.id));
}


function processPluginAuthenticateAction(clientId, message) {
	const token = message.payload.token;

	if (token) {
		request({
			method: `GET`,
			uri: `${Config.AUTH_SERVICE_ENDPOINT}/token/plugin/authenticate?token=${token}`
		}, (err, response, body) => {
			if (!err && response.statusCode === 200) {
				webSocketServer.authenticateClient(clientId);
				webSocketServer.send(clientId, new Message({
					id: message.id,
					type: Message.PLUGIN_TYPE,
					action: Message.AUTHENTICATE_ACTION,
					status: Message.SUCCESS_STATUS,
					payload: JSON.parse(body) // TODO
				}).toString());
			} else {
				respondWithFailure(clientId, JSON.parse(body).message, message.id); // TODO
			}
		});
	} else {
		respondWithFailure(clientId, `Payload should consist property "token" to authenticate plugin`, message.id);
	}
}


function respondWithFailure (clientId, errorMessage, messageId) {
    webSocketServer.send(clientId, new Message({
        id: messageId,
        type: Message.ACK_TYPE,
        status: Message.FAILED_STATUS,
        payload: { message: errorMessage } // TODO
    }).toString());
}