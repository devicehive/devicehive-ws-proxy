const WebSocket = require('ws');
const Message = require(`../../lib/Message`);
const status = require('node-status');

const TOTAL_MESSAGES = 300000;

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
		type: Message.TOPIC_TYPE,
		action: Message.SUBSCRIBE_ACTION,
		payload: {
			t: ["topicN"]
		}
	}).toString());
});

ws.on('message', function incoming(data) {
	const message = Message.normalize(JSON.parse(data));

	if (message.type === Message.NOTIFICATION_TYPE && message.action !== Message.CREATE_ACTION) {
        const checkCount = parseInt(message.payload);

        if (message.payload.startsWith(`${process.pid}`)) {
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
	} else if (message.type === Message.TOPIC_TYPE && message.action === Message.SUBSCRIBE_ACTION) {
		interval = setInterval(() => {
			if (sendCounter < TOTAL_MESSAGES) {
				for(let i = 0; i < 30; i++) {
					ws.send(new Message({
						type: Message.NOTIFICATION_TYPE,
						action: Message.CREATE_ACTION,
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