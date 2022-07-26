const Config = require(`./config`);
const ProxyClient = require("./ProxyClient");
const {
    Message,
    MessageUtils,
    MessageBuilder,
} = require(`devicehive-proxy-message`);
const { NotificationCreatePayload } =
    require(`devicehive-proxy-message`).payload;

const TEST_MESSAGE = MessageBuilder.createNotification(
    new NotificationCreatePayload({
        topic: Config.TEST_TOPIC,
        partition: Config.TEST_PARTITION,
        message: JSON.stringify(Config.TEST_MESSAGE),
    })
);
const TEST_MESSAGE_SIZE = Buffer.from(TEST_MESSAGE.toString()).length;
const TOTAL_MESSAGES = Config.TOTAL_MESSAGES_AMOUNT;

const proxyClient = new ProxyClient(Config.PROXY_SERVER_URL);

proxyClient.on(`open`, async () => {
    await proxyClient.sendMessage(
        new Message({
            type: MessageUtils.TOPIC_TYPE,
            action: MessageUtils.CREATE_ACTION,
            payload: { topicList: [Config.TEST_TOPIC] },
        })
    );

    await proxyClient.sendMessage(
        new Message({
            type: MessageUtils.TOPIC_TYPE,
            action: MessageUtils.SUBSCRIBE_ACTION,
            payload: { topicList: [Config.TEST_TOPIC] },
        })
    );

    process.send({ action: `ready` });
});

let rCount = 0;
let rCountPrv = 0;
const throughputList = [];
let intervalHandler;

proxyClient.on(`message`, (message) => {
    if (
        message.type === MessageUtils.NOTIFICATION_TYPE &&
        message.action !== MessageUtils.CREATE_ACTION
    ) {
        if (!intervalHandler) {
            intervalHandler = setInterval(() => {
                throughputList.push((rCount - rCountPrv) * TEST_MESSAGE_SIZE);
                rCountPrv = rCount;
                process.send({
                    action: `received`,
                    amount: rCount,
                });
            }, 1000);
        }

        rCount++;

        if (rCount === TOTAL_MESSAGES) {
            clearInterval(intervalHandler);
            process.send({
                action: `received`,
                amount: rCount,
            });
            process.send({
                action: `finished`,
                averageThroughput: throughputList.reduce(
                    (p, t) => p + t / throughputList.length,
                    0
                ),
            });
        }
    }
});
