const ProxyConfig = require(`./ProxyConfig`);
const CONST = require(`./constants.json`);
const Utils = require(`../utils`);
const Message = require(`../lib/Message`);
const WebSocketServer = require(`./WebSocketServer`);
const MessageBuffer = require(`./MessageBuffer`);
const InternalCommunicatorFacade = require(`./InternalCommunicatorFacade`);
const PluginManager = require(`./PluginManager`);
const ApplicationLogger = require(`./ApplicationLogger`);


const logger = new ApplicationLogger(CONST.APPLICATION_TAG, ProxyConfig.APP_LOG_LEVEL);
const messageBuffer = new MessageBuffer();
const internalCommunicatorFacade = new InternalCommunicatorFacade(ProxyConfig.COMMUNICATOR_TYPE);
const pluginManager = new PluginManager();
const webSocketServer = new WebSocketServer();


initProcessExitHandlers();


webSocketServer.on(WebSocketServer.CLIENT_CONNECT_EVENT, (clientId) => {
    logger.info(`New WebSocket client connected. ID: ${clientId}`);
});

webSocketServer.on(WebSocketServer.CLIENT_MESSAGE_EVENT, (clientId, data) => {
    try {
        const messages = JSON.parse(data);

        Utils.forEach(messages, (message) => {
        	const normalizedMessage = Message.normalize(message);

            if (pluginManager.isEnabled()) {
                pluginManager.checkConstraints(clientId, normalizedMessage);
            }

            messageBuffer.push({ clientId: clientId, message: normalizedMessage });
        });
    } catch (error) {
        logger.warn(`Error on incoming WebSocket message: ${error.message}`);
        respondWithFailure(clientId, error.message, error.messageId);
    }
});

webSocketServer.on(WebSocketServer.CLIENT_DISCONNECT_EVENT, (clientId) => {
	internalCommunicatorFacade.removeSubscriber(clientId);
    pluginManager.removeAuthentication(clientId);

    logger.info(`WebSocket client has been disconnected. ID: ${clientId}`);
});

internalCommunicatorFacade.on(InternalCommunicatorFacade.MESSAGE_EVENT, (clientId, topic, message, partition) => {
    webSocketServer.send(clientId, new Message({
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
 * Process incoming message
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
 * Process topic type messages
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
 * Process plugin type messages
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
 * Process notification type messages
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
 * Process Health type messages
 * @param clientId
 * @param message
 */
function processHealthTypeMessage(clientId, message) {
    webSocketServer.send(clientId, new Message({
        id: message.id,
        type: Message.HEALTH_CHECK_TYPE,
        status: Message.SUCCESS_STATUS,
        payload: {
        	status: internalCommunicatorFacade.isAvailable() ?
				CONST.PROXY_AVAILABLE_STATUS :
				CONST.PROXY_NOT_AVAILABLE_STATUS
        }
    }).toString());
}


/**
 * Process topic create messages
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

                logger.info(`Topics ${createdTopicList} has been created (request from ${clientId})`);
			})
			.catch((error) => respondWithFailure(clientId, error.message, message.id));
	} else {
		respondWithFailure(clientId, `Payload should consist an array with topics to create`, message.id);
	}
}


/**
 * Process topic list messages
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
 * Process topic subscription messages
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

                logger.info(`Client ${clientId} has been subscribed to the next topics: ${topicSubscriptionList}`);
		    })
		    .catch((error) => respondWithFailure(clientId, error.message, message.id));
    } else {
	    respondWithFailure(clientId, `Payload should consist property "t" with list of topics to subscribe`, message.id);
    }
}


/**
 * Process topic unsubscribe message
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

                logger.info(`Client ${clientId} has been unsubscribed from the next topics: ${topicUnsubscriptionList}`);
			})
			.catch((error) => respondWithFailure(clientId, error.message, message.id));
	} else {
		respondWithFailure(clientId, `Payload should consist property "t" with list of topics to unsubscribe`, message.id);
	}
}


/**
 * Process notification create message
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
 * Process plugin authenticate message
 * @param clientId
 * @param message
 */
function processPluginAuthenticateAction(clientId, message) {
	const token = message.payload.token;

	if (token) {
        pluginManager.authenticate(clientId, token)
			.then((authenticationResponse) => {
                webSocketServer.send(clientId, new Message({
                    id: message.id,
                    type: Message.PLUGIN_TYPE,
                    action: Message.AUTHENTICATE_ACTION,
                    status: Message.SUCCESS_STATUS,
                    payload: authenticationResponse
                }).toString());

                logger.info(`Client plugin ${clientId} has been authenticated`);
			})
			.catch((error) => {
                respondWithFailure(clientId, error.message, message.id);
			});
	} else {
		respondWithFailure(clientId, `Payload should consist property "token" to authenticate plugin`, message.id);
	}
}


/**
 * Responde to WebSocket client with error
 * @param clientId
 * @param errorMessage
 * @param messageId
 */
function respondWithFailure (clientId, errorMessage, messageId) {
    webSocketServer.send(clientId, new Message({
        id: messageId,
        type: Message.ACK_TYPE,
        status: Message.FAILED_STATUS,
        payload: { message: errorMessage }
    }).toString());
}

/**
 * Init process exit handlers. Log errors
 */
function initProcessExitHandlers() {
    process.stdin.resume();

    function exitHandler(error) {
        if (error) {
            logger.err(`Process exited with error: ${error.message}`);
            logger.err(error.stack);
        }

        process.exit();
    }

    process.on('exit', exitHandler);
    process.on('SIGINT', exitHandler);
    process.on('SIGUSR1', exitHandler);
    process.on('SIGUSR2', exitHandler);
    process.on('uncaughtException', exitHandler);
}