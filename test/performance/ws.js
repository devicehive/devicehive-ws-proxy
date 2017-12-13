const WebSocket = require('ws');
const { Message, MessageUtils } = require(`devicehive-proxy-message`);
const status = require('node-status');

const TOTAL_MESSAGES = 600000;

const sendedStatus = status.addItem('sendedStatus', { max: TOTAL_MESSAGES });
const recievedStatus = status.addItem('recievedStatus', { max: TOTAL_MESSAGES });


const ws = new WebSocket('ws://localhost:3000');
let sendCounter = 0;
let counter = 0;
let interval;


const counterSet = new Set();

status.start({
	invert: true,
	interval: 200,
	pattern: 'Doing work: {uptime}  |  {spinner.cyan}  |  {sendedStatus.bar} | {recievedStatus.bar} | {recievedStatus}'
});

console = status.console();

ws.on('open', function open() {
	ws.send(new Message({
		type: MessageUtils.TOPIC_TYPE,
		action: MessageUtils.SUBSCRIBE_ACTION,
		payload: {
			t: ["topicN"]
		}
	}).toString());
});

ws.on('message', function incoming(data) {
	const message = MessageUtils.normalize(JSON.parse(data));

	if (message.type === MessageUtils.NOTIFICATION_TYPE && message.action !== MessageUtils.CREATE_ACTION) {
        const checkCount = parseInt(message.payload.m);

        if (message.payload.m.startsWith(`${process.pid}`)) {
            if (!counterSet.has(checkCount)) {
                counterSet.add(checkCount);
                counter++;
                recievedStatus.inc();
                if (counter === TOTAL_MESSAGES) {
                    setTimeout(() => {
                        console.log(counter);
                        ws.close();
                        process.exit();
                    }, 1000)
                }
            } else {
                console.log(`Offset ${checkCount} already exists`);
                process.exit();
            }
        }
	} else if (message.type === MessageUtils.TOPIC_TYPE && message.action === MessageUtils.SUBSCRIBE_ACTION) {
		interval = setInterval(() => {
			if (sendCounter < TOTAL_MESSAGES) {
				for(let i = 0; i < 30; i++) {
					ws.send(new Message({
						type: MessageUtils.NOTIFICATION_TYPE,
						action: MessageUtils.CREATE_ACTION,
						payload: {t: "topicN", m: `${process.pid}${sendCounter}`}
					}).toString());
					sendCounter++;
					sendedStatus.inc();
				}
			} else {
				clearInterval(interval);
			}
		}, 1);
	}
});