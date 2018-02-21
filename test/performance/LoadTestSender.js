const Config = require(`./config`);
const ProxyClient = require('./ProxyClient');
const { MessageBuilder } = require(`devicehive-proxy-message`);
const { NotificationCreatePayload } = require(`devicehive-proxy-message`).payload;

let TEST_MESSAGE = MessageBuilder.createNotification(new NotificationCreatePayload({
    topic: Config.TEST_TOPIC,
    partition: Config.TEST_PARTITION,
    message: JSON.stringify(Config.TEST_MESSAGE)
}));
const TOTAL_MESSAGES = Config.TOTAL_MESSAGES_AMOUNT;


const proxyClient = new ProxyClient(Config.PROXY_SERVER_URL);

function startSendingRoutine(uuid) {
    let messageCount = 0;
    let messagesPes100ms = Config.MESSAGE_PER_SECOND / 10;
    let monitorIntervalHandler = setInterval(() => {
        process.send({ action: `sent`, amount: messageCount });
    }, 1000);

    TEST_MESSAGE = MessageBuilder.createNotification(new NotificationCreatePayload({
        topic: Config.TEST_TOPIC + uuid,
        partition: Config.TEST_PARTITION,
        message: JSON.stringify(Config.TEST_MESSAGE)
    }));

    const intervalHandler = setInterval(() => {
        for (let c = 0; c < messagesPes100ms; c++) {
            messageCount++;
            proxyClient.send(TEST_MESSAGE);

            if (messageCount === TOTAL_MESSAGES) {
                clearInterval(intervalHandler);
                clearInterval(monitorIntervalHandler);
                process.send({ action: `sent`, amount: messageCount });
                process.send({ action: `finished` });
            }
        }
    }, 100);
}

proxyClient.on(`open`, async () => {
    process.send({ action: `ready` });
});

process.on(`message`, (message) => {
    switch (message.action) {
        case "start":
            startSendingRoutine(message.uuid);
            break;
    }
});