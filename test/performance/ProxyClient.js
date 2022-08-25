const WS = require(`ws`);
const EventEmitter = require(`events`);
const { Message } = require(`devicehive-proxy-message`);
const { v4: uuid } = require(`uuid`);

/**
 * DeviceHive WebSocket Proxy client
 */
class ProxyClient extends EventEmitter {
    /**
     * Creates new ProxyClient object
     * @param {string} webSocketServerUrl
     * @constructor
     */
    constructor(webSocketServerUrl) {
        super();

        this.ws = new WS(webSocketServerUrl);

        this.ws.addEventListener(`open`, () => {
            process.nextTick(() => this.emit(`open`));
        });

        this.ws.addEventListener(`close`, () => {
            process.nextTick(() => this.emit(`close`));
        });

        this.ws.addEventListener(`error`, (error) => {
            this.emit(`error`, error);
        });

        this.ws.addEventListener(`ping`, (pingData) => {
            this.emit(`ping`, pingData);
        });

        this.ws.addEventListener(`message`, (event) => {
            let messages = JSON.parse(event.data);
            messages = messages.length ? messages : [messages];

            for (
                let messageCount = 0;
                messageCount < messages.length;
                messageCount++
            ) {
                this.emit(`message`, Message.normalize(messages[messageCount]));
                if (messages[messageCount].id) {
                    this.emit(
                        messages[messageCount].id,
                        Message.normalize(messages[messageCount])
                    );
                }
            }
        });
    }

    /**
     * Sends message to WS Proxy
     * @param {Message} message
     */
    async sendMessage(message = new Message()) {
        message.id = message.id || uuid();

        return new Promise((resolve) => {
            this.ws.send(message.toString());

            this.on(message.id, resolve);
        });
    }

    /**
     * Sends message to WS Proxy
     * @param {Message} message
     */
    send(message = new Message()) {
        this.ws.send(message.toString());
    }
}

module.exports = ProxyClient;
