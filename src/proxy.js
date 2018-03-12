const Config = require(`../config`).proxy;
const { Message, MessageUtils } = require(`devicehive-proxy-message`);
const CONST = require(`./constants.json`);
const Utils = require(`../utils`);
const WebSocketServer = require(`./WebSocketServer`);
const MessageBuffer = require(`./messageBuffer/MessageBuffer`);
const InternalCommunicatorFacade = require(`./InternalCommunicatorFacade`);
const PluginManager = require(`./pluginManager/PluginManager`);
const ApplicationLogger = require(`./ApplicationLogger`);

const logger = new ApplicationLogger(CONST.APPLICATION_TAG, Config.APP_LOG_LEVEL);
const messageBuffer = new MessageBuffer();
const pluginManager = new PluginManager(!Config.ENABLE_PLUGIN_MANAGER);
const internalCommunicatorFacade = new InternalCommunicatorFacade(Config.COMMUNICATOR_TYPE);
const webSocketServer = new WebSocketServer();


webSocketServer.on(WebSocketServer.CLIENT_CONNECT_EVENT, (clientId) => {
    logger.info(`New WebSocket client connected. ID: ${clientId}`);
});

webSocketServer.on(WebSocketServer.CLIENT_MESSAGE_EVENT, (clientId, data) => {
    try {
        const messages = JSON.parse(data);

        Utils.forEach(messages, (message) => {
            const normalizedMessage = Message.normalize(message);

            pluginManager.checkConstraints(clientId, normalizedMessage);

            if (normalizedMessage.type === MessageUtils.HEALTH_CHECK_TYPE || message.type === MessageUtils.PLUGIN_TYPE) {
                processMessage({
                    clientId: clientId,
                    message: normalizedMessage
                });
            } else {
                messageBuffer.push({clientId: clientId, message: normalizedMessage});
            }

            respondWithAcknowledgment(clientId, normalizedMessage);
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
        type: MessageUtils.NOTIFICATION_TYPE,
        status: MessageUtils.SUCCESS_STATUS,
        payload: { message: payload }
    }).toString());
});

internalCommunicatorFacade.on(InternalCommunicatorFacade.AVAILABLE_EVENT, () => messageBuffer.enablePolling());
internalCommunicatorFacade.on(InternalCommunicatorFacade.NOT_AVAILABLE_EVENT, () => messageBuffer.disablePolling());

messageBuffer.on(MessageBuffer.POLL_EVENT, (messages) => {
    for (let messageCount = 0; messageCount < messages.length; messageCount++) {
        processMessage(messages[messageCount]);
    }
});


/**
 * Process incoming message
 * @param clientId
 * @param message
 */
function processMessage({clientId, message}) {
    switch (message.type) {
        case MessageUtils.TOPIC_TYPE:
            processTopicTypeMessage(clientId, message);
            break;
        case MessageUtils.NOTIFICATION_TYPE:
            processNotificationTypeMessage(clientId, message);
            break;
        case MessageUtils.PLUGIN_TYPE:
            processPluginTypeMessage(clientId, message);
            break;
        case MessageUtils.HEALTH_CHECK_TYPE:
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
        case MessageUtils.CREATE_ACTION:
            processTopicCreateAction(clientId, message);
            break;
        case MessageUtils.LIST_ACTION:
            processTopicListAction(clientId, message);
            break;
        case MessageUtils.SUBSCRIBE_ACTION:
            processTopicSubscribeAction(clientId, message);
            break;
        case MessageUtils.UNSUBSCRIBE_ACTION:
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
        case MessageUtils.AUTHENTICATE_ACTION:
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
        case MessageUtils.CREATE_ACTION:
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
        type: MessageUtils.HEALTH_CHECK_TYPE,
        status: MessageUtils.SUCCESS_STATUS,
        payload: {
            proxy: CONST.AVAILABLE_STATUS,
            messageBuffer: messageBuffer.getFreeMemory() ?
                CONST.AVAILABLE_STATUS :
                CONST.NOT_AVAILABLE_STATUS,
            messageBufferFillPercentage: messageBuffer.getFillPercentage(),
            communicator: {
                isAvailable: internalCommunicatorFacade.isAvailable() ?
                    CONST.AVAILABLE_STATUS :
                    CONST.NOT_AVAILABLE_STATUS,
                inputLoad: internalCommunicatorFacade.getAverageInputLoad(),
                outputLoad: internalCommunicatorFacade.getAverageOutputLoad()
            }
        }
    }).toString());
}


/**
 * Process topic create messages
 * @param clientId
 * @param message
 */
function processTopicCreateAction(clientId, message) {
    if (message.payload && Array.isArray(message.payload.topicList)) {
        internalCommunicatorFacade.createTopics(message.payload.topicList)
            .then((createdTopicList) => {
                webSocketServer.send(clientId, new Message({
                    id: message.id,
                    type: MessageUtils.TOPIC_TYPE,
                    action: MessageUtils.CREATE_ACTION,
                    status: MessageUtils.SUCCESS_STATUS,
                    payload: {
                        topicList: createdTopicList
                    }
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
                type: MessageUtils.TOPIC_TYPE,
                action: MessageUtils.LIST_ACTION,
                status: MessageUtils.SUCCESS_STATUS,
                payload: {
                    topicList: topicsList
                }
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
    if (message.payload && Array.isArray(message.payload.topicList)) {
        internalCommunicatorFacade.subscribe(clientId, message.payload.subscriptionGroup, message.payload.topicList)
            .then((topicSubscriptionList) => {
                webSocketServer.send(clientId, new Message({
                    id: message.id,
                    type: MessageUtils.TOPIC_TYPE,
                    action: MessageUtils.SUBSCRIBE_ACTION,
                    status: MessageUtils.SUCCESS_STATUS,
                    payload: {
                        subscriptionGroup: message.payload.subscriptionGroup,
                        topicList: topicSubscriptionList
                    }
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
    if (message.payload && Array.isArray(message.payload.topicList)) {
        internalCommunicatorFacade.unsubscribe(clientId, message.payload.topicList)
            .then((topicUnsubscriptionList) => {
                webSocketServer.send(clientId, new Message({
                    id: message.id,
                    type: MessageUtils.TOPIC_TYPE,
                    action: MessageUtils.UNSUBSCRIBE_ACTION,
                    status: MessageUtils.SUCCESS_STATUS,
                    payload: {
                        topicList: topicUnsubscriptionList
                    }
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
    internalCommunicatorFacade.send({
        topic: message.payload.topic,
        message: { value: message.payload.message },
        partition: message.payload.partition
    }).then(() => {
        webSocketServer.send(clientId, new Message({
            id: message.id,
            type: MessageUtils.NOTIFICATION_TYPE,
            action: MessageUtils.CREATE_ACTION,
            status: MessageUtils.SUCCESS_STATUS
        }).toString());
    }).catch((error) => respondWithFailure(clientId, error.message, message));
}


/**
 * Process plugin authenticate message
 * @param clientId
 * @param message
 */
function processPluginAuthenticateAction(clientId, message) {
    if (message.payload && message.payload.token) {
        pluginManager.authenticate(clientId, message.payload.token)
            .then((tokenPayload) => {
                webSocketServer.send(clientId, new Message({
                    id: message.id,
                    type: MessageUtils.PLUGIN_TYPE,
                    action: MessageUtils.AUTHENTICATE_ACTION,
                    status: MessageUtils.SUCCESS_STATUS,
                    payload: tokenPayload
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
 * Responde to WebSocket client with acknowledgment
 * @param clientId
 * @param message
 */
function respondWithAcknowledgment(clientId, message = {}) {
    if (Config.ACK_ON_EVERY_MESSAGE_ENABLED === true) {
        webSocketServer.send(clientId, new Message({
            id: message.id,
            type: MessageUtils.ACK_TYPE,
            status: MessageUtils.SUCCESS_STATUS
        }).toString());
    }
}


/**
 * Responde to WebSocket client with error
 * @param clientId
 * @param errorMessage
 * @param message
 * @param code
 */
function respondWithFailure(clientId, errorMessage, message = {}, code) {
    webSocketServer.send(clientId, new Message({
        id: message.id,
        type: message.type || MessageUtils.ACK_TYPE,
        action: message.action,
        status: MessageUtils.FAILED_STATUS,
        payload: {
            message: errorMessage,
            code: code
        }
    }).toString());
}