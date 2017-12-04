const Utils = require(`../utils`);
const Message = require(`../lib/Message`);
const webSocketServer = require(`./WebSocketServer`);
const messageBuffer = require(`./MessageBuffer`);


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

messageBuffer.on(`notEmpty`, async () => {
    while (messageBuffer.length) {
        processMessage(messageBuffer.shift());
    }
});


function processMessage({ clientId, message }) {
    switch (message.type) {
        case Message.TOPIC_TYPE:
            processTopicTypeMessage(clientId, message);
            break;
        case Message.NOTIF_TYPE:
            processNotificationTypeMessage(clientId, message);
            break;
        case Message.PLUGIN_TYPE:
            processPluginTypeMessage(clientId, message);
            break;
        case Message.HEALTH_TYPE:
            processHealthTypeMessage(clientId, message);
            break;
        default:
            respondWithFailure(clientId, `Unsupported message type: ${message.type}`);
            break;
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
            respondWithFailure(clientId, `Unsupported topic message action: ${message.action}`);
            break;
    }
}


function processPluginTypeMessage(clientId, message) {
    switch (message.action) {
        case Message.AUTHENTICATE_ACTION:
            processPluginAuthenticateAction(clientId, message);
            break;
        default:
            respondWithFailure(clientId, `Unsupported plugin message action: ${message.action}`);
            break;
    }
}


function processNotificationTypeMessage(clientId, message) {
    switch (message.action) {
        case Message.CREATE_ACTION:
            processNotificationCreateAction(clientId, message);
            break;
        default:
            respondWithFailure(clientId, `Unsupported notification message action: ${message.action}`);
            break;
    }
}


function processHealthTypeMessage(clientId, message) {
    webSocketServer.send(clientId, new Message({
        id: message.id,
        type: Message.HEALTH_TYPE,
        status: Message.SUCCESS_STATUS,
        payload: { status: 'available' } //TODO
    }).toString());
}


function processTopicCreateAction(clientId, message) {

}


function processTopicListAction(clientId, message) {

}


function processTopicSubscribeAction(clientId, message) {

}


function processTopicUnsubscribeAction(clientId, message) {

}


function processNotificationCreateAction(clientId, message) {

}


function processPluginAuthenticateAction(clientId, message) {

}


function respondWithFailure (clientId, errorMessage) {
    webSocketServer.send(clientId, new Message({
        type: Message.ACK_TYPE,
        status: Message.FAILED_STATUS,
        payload: { message: errorMessage }
    }).toString());
}