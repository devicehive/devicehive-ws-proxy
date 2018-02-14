const WS = require(`ws`);
const EventEmitter = require(`events`);
const { Message } = require(`devicehive-proxy-message`);
const uuid = require(`uuid/v1`);


/**
 * DeviceHive WebSocket Proxy client
 */
class ProxyClient extends EventEmitter {

    /**
     * Creates new ProxyClient object
     * @param webSocketServerUrl
     */
    constructor(webSocketServerUrl) {
        super();

        const me = this;

        me.ws = new WS(webSocketServerUrl);

        me.ws.addEventListener(`open`, () => {
            process.nextTick(() => me.emit(`open`));
        });

        me.ws.addEventListener(`close`, () => {
            process.nextTick(() => me.emit(`close`));
        });

        me.ws.addEventListener(`error`, (error) => {
            me.emit(`error`, error);
        });

        me.ws.addEventListener(`ping`, (pingData) => {
            me.emit(`ping`, pingData);
        });

        me.ws.addEventListener(`message`, (event) => {
            try {
                let messages = JSON.parse(event.data);
                messages = messages.length ? messages : [messages];

                for (let messageCount = 0; messageCount < messages.length; messageCount++) {
                    me.emit(`message`, Message.normalize(messages[messageCount]));
                    if (messages[messageCount].id) {
                        me.emit(messages[messageCount].id, Message.normalize(messages[messageCount]));
                    }
                }
            } catch (error) {
                console.warn(error);
            }
        });
    }

    /**
     * Sends message to WS Proxy
     * @param message
     */
    async sendMessage(message = new Message()) {
        const me = this;

        message.id = message.id || uuid();

        return new Promise((resolve) => {
            me.ws.send(message.toString());

            me.on(message.id, resolve);
        });
    }

    /**
     * Sends message to WS Proxy
     * @param message
     */
    send(message = new Message()) {
        const me = this;

        me.ws.send(message.toString());
    }
}


module.exports = ProxyClient;

