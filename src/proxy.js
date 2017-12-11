const ProxyConfig = require(`./ProxyConfig`);
const CONST = require(`./constants.json`);
const Utils = require(`../utils`);
const Message = require(`../lib/Message`);
const NotificationCreateMessagePayload = require(`../lib/messagePayload/NotificationCreateMessagePayload`);
const NotificationMessagePayload = require(`../lib/messagePayload/NotificationMessagePayload`);
const PluginAuthenticateMessagePayload = require(`../lib/messagePayload/PluginAuthenticateMessagePayload`);
const TopicUnsubscribeMessagePayload = require(`../lib/messagePayload/TopicUnsubscribeMessagePayload`);
const TopicSubscribeMessagePayload = require(`../lib/messagePayload/TopicSubscribeMessagePayload`);
const TopicCreateMessagePayload = require(`../lib/messagePayload/TopicCreateMessagePayload`);
const TopicListMessagePayload = require(`../lib/messagePayload/TopicListMessagePayload`);
const HealthMessagePayload = require(`../lib/messagePayload/HealthMessagePayload`);
const WebSocketServer = require(`./WebSocketServer`);
const MessageBuffer = require(`./MessageBuffer`);
const InternalCommunicatorFacade = require(`./InternalCommunicatorFacade`);
const PluginManager = require(`./PluginManager`);
const ApplicationLogger = require(`./ApplicationLogger`);


const logger = new ApplicationLogger(CONST.APPLICATION_TAG, ProxyConfig.APP_LOG_LEVEL);
const messageBuffer = new MessageBuffer(ProxyConfig.MESSAGE_BUFFER.MAX_SIZE_MB);
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

            messageBuffer.push({clientId: clientId, message: normalizedMessage});

            if (ProxyConfig.ACK_ON_EVERY_MESSAGE_ENABLED === true) {
                webSocketServer.send(clientId, new Message({
                    id: normalizedMessage.id,
                    type: Message.ACK_TYPE,
                    status: Message.SUCCESS_STATUS
                }).toString());
            }
        });
    } catch (error) {
        logger.warn(`Error on incoming WebSocket message: ${error.message}`);
        respondWithFailure(clientId, error.message, error.messageObject);
    }
});

webSocketServer.on(WebSocketServer.CLIENT_DISCONNECT_EVENT, (clientId) => {
    internalCommunicatorFacade.removeSubscriber(clientId);
    pluginManager.removeAuthentication(clientId);

    logger.info(`WebSocket client has been disconnected. ID: ${clientId}`);
});

internalCommunicatorFacade.on(InternalCommunicatorFacade.MESSAGE_EVENT, (clientId, topic, payload) => {
    webSocketServer.send(clientId, new Message({
        type: Message.NOTIFICATION_TYPE,
        payload: new NotificationMessagePayload({ message: payload.toString() }).toObject()
    }).toString());
});


setInterval(() => {
    if (internalCommunicatorFacade.isAvailable() && messageBuffer.length > 0) {
        const counter = messageBuffer.length < ProxyConfig.MESSAGE_BUFFER.BUFFER_POLLING_MESSAGE_AMOUNT ?
            messageBuffer.length : ProxyConfig.MESSAGE_BUFFER.BUFFER_POLLING_MESSAGE_AMOUNT;
        for (let messageCounter = 0; messageCounter < counter; messageCounter++) {
            processMessage(messageBuffer.shift());
        }
    }
}, ProxyConfig.MESSAGE_BUFFER.BUFFER_POLLING_INTERVAL_MS);


/**
 * Process incoming message
 * @param clientId
 * @param message
 */
function processMessage({clientId, message}) {
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
            respondWithFailure(clientId, `Unsupported message type: ${message.type}`, message);
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
            respondWithFailure(clientId, `Unsupported topic message action: ${message.action}`, message);
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
            respondWithFailure(clientId, `Unsupported plugin message action: ${message.action}`, message);
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
            respondWithFailure(clientId, `Unsupported notification message action: ${message.action}`, message);
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
        payload: new HealthMessagePayload({
            proxy: CONST.AVAILABLE_STATUS,
            messageBuffer: messageBuffer.getFreeMemory() ?
                CONST.AVAILABLE_STATUS :
                CONST.NOT_AVAILABLE_STATUS,
            messageBufferFillPercentage: messageBuffer.getFillPercentage(),
            communicator: internalCommunicatorFacade.isAvailable() ?
                CONST.AVAILABLE_STATUS :
                CONST.NOT_AVAILABLE_STATUS
        }).toObject()
    }).toString());
}


/**
 * Process topic create messages
 * @param clientId
 * @param message
 */
function processTopicCreateAction(clientId, message) {
    const messagePayload = TopicCreateMessagePayload.normalize(message.payload);

    if (Array.isArray(messagePayload.topicList)) {
        internalCommunicatorFacade.createTopics(messagePayload.topicList)
            .then((createdTopicList) => {
                webSocketServer.send(clientId, new Message({
                    id: message.id,
                    type: Message.TOPIC_TYPE,
                    action: Message.CREATE_ACTION,
                    status: Message.SUCCESS_STATUS,
                    payload: new TopicCreateMessagePayload({ topicList: createdTopicList }).toObject()
                }).toString());

                logger.info(`Topics ${createdTopicList} has been created (request from ${clientId})`);
            })
            .catch((error) => respondWithFailure(clientId, error.message, message));
    } else {
        respondWithFailure(clientId, `Payload should consist an array with topics to create`, message);
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
                payload: new TopicListMessagePayload({ topicList: topicsList }).toObject()
            }).toString());
        })
        .catch((error) => respondWithFailure(clientId, error.message, message));
}


/**
 * Process topic subscription messages
 * @param clientId
 * @param message
 */
function processTopicSubscribeAction(clientId, message) {
    const messagePayload = TopicSubscribeMessagePayload.normalize(message.payload);

    if (Array.isArray(messagePayload.topicList)) {
        internalCommunicatorFacade.subscribe(clientId, messagePayload.topicList)
            .then((topicSubscriptionList) => {
                webSocketServer.send(clientId, new Message({
                    id: message.id,
                    type: Message.TOPIC_TYPE,
                    action: Message.SUBSCRIBE_ACTION,
                    status: Message.SUCCESS_STATUS,
                    payload: new TopicSubscribeMessagePayload({ topicList: topicSubscriptionList}).toObject()
                }).toString());

                logger.info(`Client ${clientId} has been subscribed to the next topics: ${topicSubscriptionList}`);
            })
            .catch((error) => respondWithFailure(clientId, error.message, message));
    } else {
        respondWithFailure(clientId, `Payload should consist property "t" with list of topics to subscribe`, message);
    }
}


/**
 * Process topic unsubscribe message
 * @param clientId
 * @param message
 */
function processTopicUnsubscribeAction(clientId, message) {
    const messagePayload = TopicUnsubscribeMessagePayload.normalize(message.payload);

    if (Array.isArray(messagePayload.topicList)) {
        internalCommunicatorFacade.unsubscribe(clientId, messagePayload.topicList)
            .then((topicUnsubscriptionList) => {
                webSocketServer.send(clientId, new Message({
                    id: message.id,
                    type: Message.TOPIC_TYPE,
                    action: Message.UNSUBSCRIBE_ACTION,
                    status: Message.SUCCESS_STATUS,
                    payload: new TopicUnsubscribeMessagePayload({ topicList: topicUnsubscriptionList }).toObject()
                }).toString());

                logger.info(`Client ${clientId} has been unsubscribed from the next topics: ${topicUnsubscriptionList}`);
            })
            .catch((error) => respondWithFailure(clientId, error.message, message));
    } else {
        respondWithFailure(clientId, `Payload should consist property "t" with list of topics to unsubscribe`, message);
    }
}


/**
 * Process notification create message
 * @param clientId
 * @param message
 */
function processNotificationCreateAction(clientId, message) {
    const messagePayload = NotificationCreateMessagePayload.normalize(message.payload);

    internalCommunicatorFacade.send({
        topic: messagePayload.topic,
        message: { value: messagePayload.message },
        partition: messagePayload.partition
    }).then(() => {
        webSocketServer.send(clientId, new Message({
            id: message.id,
            type: Message.NOTIFICATION_TYPE,
            action: Message.CREATE_ACTION,
            status: Message.SUCCESS_STATUS
        }).toString());
    }).catch((error) => respondWithFailure(clientId, error.message, message));
}


/**
 * Process plugin authenticate message
 * @param clientId
 * @param message
 */
function processPluginAuthenticateAction(clientId, message) {
    const messagePayload = PluginAuthenticateMessagePayload.normalize(message.payload);

    if (messagePayload) {
        pluginManager.authenticate(clientId, messagePayload.token)
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
                respondWithFailure(clientId, error.message, message);
            });
    } else {
        respondWithFailure(clientId, `Payload should consist property "token" to authenticate plugin`, message);
    }
}


/**
 * Responde to WebSocket client with error
 * @param clientId
 * @param errorMessage
 * @param message
 */
function respondWithFailure(clientId, errorMessage, message = {}) {
    webSocketServer.send(clientId, new Message({
        id: message.id,
        type: message.type || Message.ACK_TYPE,
        action: message.action,
        status: Message.FAILED_STATUS,
        payload: { m: errorMessage }
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