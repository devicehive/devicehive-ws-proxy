const ProxyConfig = require(`./ProxyConfig`);
const Utils = require(`../utils`);
const Message = require(`../lib/Message`);
const webSocketServer = require(`./WebSocketServer`);
const messageBuffer = require(`./MessageBuffer`);
const internalCommunicatorFacade = require(`./InternalCommunicatorFacade`);
const pluginManager = require(`./PluginManager`);


webSocketServer.on(`clientConnect`, (clientId) => {

});

webSocketServer.on(`clientMessage`, (clientId, data) => {
    try {
        const messages = JSON.parse(data);

        Utils.forEach(messages, (message) => {
        	const normalizedMessage = Message.normalize(message);

            if (pluginManager.isEnabled()) {
                pluginManager.checkConstraints(clientId, normalizedMessage);
            }

            messageBuffer.push({ clientId: clientId, message: normalizedMessage });

            webSocketServer.send(clientId, new Message({
				id: message.id,
				type: Message.ACK_TYPE,
				action: message.action,
				status: Message.SUCCESS_STATUS
			}).toString());
        });
    } catch (error) {
        respondWithFailure(clientId, error.message);
    }
});

webSocketServer.on(`clientDisconnect`, (clientId) => {
	internalCommunicatorFacade.removeSubscriber(clientId);
    pluginManager.removeAuthentication(clientId);
});

internalCommunicatorFacade.on(`message`, (clientId, topic, message, partition) => {
    webSocketServer.send(clientId, new Message({
		id: -1, //TODO
        type: Message.NOTIFICATION_TYPE,
        payload: message.value.toString(),
    }).toString());
});


setInterval(() => {
	if (internalCommunicatorFacade.isAvailable() && messageBuffer.length > 0) {
		const counter = messageBuffer.length < ProxyConfig.SPECIFIC.BUFFER_POLLING_MESSAGE_AMOUNT ?
			messageBuffer.length : ProxyConfig.SPECIFIC.BUFFER_POLLING_MESSAGE_AMOUNT;
		for (let messageCounter = 0; messageCounter < counter; messageCounter++) {
			processMessage(messageBuffer.shift());
		}
	}
}, ProxyConfig.SPECIFIC.BUFFER_POLLING_INTERVAL_MS);


/**
 *
 * @param clientId
 * @param message
 */
function processMessage({ clientId, message }) {
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
}


/**
 *
 * @param clientId
 * @param message
 */
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


/**
 *
 * @param clientId
 * @param message
 */
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


/**
 *
 * @param clientId
 * @param message
 */
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


/**
 *
 * @param clientId
 * @param message
 */
function processHealthTypeMessage(clientId, message) {
    webSocketServer.send(clientId, new Message({
        id: message.id,
        type: Message.HEALTH_CHECK_TYPE,
        status: Message.SUCCESS_STATUS,
        payload: {
        	status: internalCommunicatorFacade.isAvailable() ? `available` : `Not available` //TODO
        }
    }).toString());
}


/**
 *
 * @param clientId
 * @param message
 */
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


/**
 *
 * @param clientId
 * @param message
 */
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


/**
 *
 * @param clientId
 * @param message
 */
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


/**
 *
 * @param clientId
 * @param message
 */
function processTopicUnsubscribeAction(clientId, message) {
	if (Array.isArray(message.payload.t)) {
		internalCommunicatorFacade.unsubscribe(clientId, message.payload.t)
			.then((topicUnsubscriptionList) => {
				webSocketServer.send(clientId, new Message({
					id: message.id,
					type: Message.TOPIC_TYPE,
					action: Message.UNSUBSCRIBE_ACTION,
					status: Message.SUCCESS_STATUS,
					payload: topicUnsubscriptionList
				}).toString());
			})
			.catch((error) => respondWithFailure(clientId, error.message, message.id));
	} else {
		respondWithFailure(clientId, `Payload should consist property "t" with list of topics to unsubscribe`, message.id);
	}
}


/**
 *
 * @param clientId
 * @param message
 */
function processNotificationCreateAction(clientId, message) {
	internalCommunicatorFacade.send({
		topic: message.payload.t,
		message: { value: message.payload.m },
		partition: message.payload.part
	}).then(() => {
		webSocketServer.send(clientId, new Message({
			id: message.id,
			type: Message.NOTIFICATION_TYPE,
			action: Message.CREATE_ACTION,
			status: Message.SUCCESS_STATUS
		}).toString());
	}).catch((error) => respondWithFailure(clientId, error.message, message.id));
}


/**
 *
 * @param clientId
 * @param message
 */
function processPluginAuthenticateAction(clientId, message) {
	const token = message.payload.token;

	if (token) {
        pluginManager.authenticate(clientId, token)
			.then((info) => {
                webSocketServer.send(clientId, new Message({
                    id: message.id,
                    type: Message.PLUGIN_TYPE,
                    action: Message.AUTHENTICATE_ACTION,
                    status: Message.SUCCESS_STATUS,
                    payload: JSON.parse(info) // TODO
                }).toString());
			})
			.catch((err) => {
                respondWithFailure(clientId, err, message.id); // TODO
			});
	} else {
		respondWithFailure(clientId, `Payload should consist property "token" to authenticate plugin`, message.id);
	}
}


/**
 *
 * @param clientId
 * @param errorMessage
 * @param messageId
 */
function respondWithFailure (clientId, errorMessage, messageId) {
    webSocketServer.send(clientId, new Message({
        id: messageId,
        type: Message.ACK_TYPE,
        status: Message.FAILED_STATUS,
        payload: { message: errorMessage } // TODO
    }).toString());
}