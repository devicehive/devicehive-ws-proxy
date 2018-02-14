const Config = require(`./config`);
const status = require('node-status');
const { fork } = require('child_process');
const { MessageBuilder } = require(`devicehive-proxy-message`);
const { NotificationCreatePayload } = require(`devicehive-proxy-message`).payload;

const receiver = fork('LoadTestReceiver.js');
const sender = fork('LoadTestSender.js');

function multipleTrigger (count, action) {
    let doubleCounter = 0;

    return function twitch () {
        doubleCounter++;

        if (doubleCounter === count) {
            action();
        }
    }
}

const TEST_MESSAGE = MessageBuilder.createNotification(new NotificationCreatePayload({
    topic: Config.TEST_TOPIC,
    partition: Config.TEST_PARTITION,
    message: JSON.stringify(Config.TEST_MESSAGE)
}));
const TEST_MESSAGE_SIZE = new Buffer(TEST_MESSAGE.toString()).length;
const TOTAL_MESSAGES = Config.TOTAL_MESSAGES_AMOUNT;
const EXPECTED_THROUGHPUT = TEST_MESSAGE_SIZE * Config.MESSAGE_PER_SECOND;


console.log(`Test message size: ${TEST_MESSAGE_SIZE} byte`);
console.log(`Total messages amount: ${TOTAL_MESSAGES}`);
console.log(`Total messages size: ${TOTAL_MESSAGES * TEST_MESSAGE_SIZE} bytes`);
console.log(`Given messages per second speed: ${Config.MESSAGE_PER_SECOND}`);
console.log(`Expected throughput: ${EXPECTED_THROUGHPUT} B/s`);
console.log(`Expected uptime: ${TOTAL_MESSAGES / Config.MESSAGE_PER_SECOND} s`);
console.log(``);


let sentFinishDate;
let receiveFinishDate;
let averageThroughput = -1;
let latency = -1;
const sendMonitor = status.addItem('sendMonitor', { max: TOTAL_MESSAGES });
const receiveMonitor = status.addItem('receiveMonitor', { max: TOTAL_MESSAGES });
const latencyMonitor = status.addItem('latencyMonitor', { custom: () => {
        return latency === -1 ? `-` : latency.toFixed();
    }
});
const throughputMonitor = status.addItem('throughputMonitor', { custom: () => {
        return averageThroughput === -1 ? `-` : averageThroughput.toFixed();
    }
});

const readyTrigger = multipleTrigger(2, () => {
    status.start({
        interval: 200,
        pattern: ' Uptime: {uptime} | {spinner.cyan} | Sent out: {sendMonitor.bar} | Received: {receiveMonitor.bar} | Actual throughput: {throughputMonitor.green.custom} B/s | Largest latency: {latencyMonitor.red.custom} s '
    });

    sender.send({ action: "start" });
});

const finishTrigger = multipleTrigger(2, () => {
    latency = (receiveFinishDate - sentFinishDate) / 1000;

    receiver.kill();
    sender.kill();

    process.exit();
});

receiver.on(`message`, (message) => {
    switch (message.action) {
        case "ready":
            readyTrigger();
            break;
        case "received":
            receiveMonitor.inc(message.amount - receiveMonitor.count);
            break;
        case "finished":
            receiveFinishDate = new Date().getTime();

            averageThroughput = message.averageThroughput;

            finishTrigger();
            break;
    }
});

sender.on(`message`, (message) => {
    switch (message.action) {
        case "ready":
            readyTrigger();
            break;
        case "sent":
            sendMonitor.inc(message.amount - sendMonitor.count);
            break;
        case "finished":
            sentFinishDate = new Date().getTime();

            finishTrigger();
            break;
    }
});
