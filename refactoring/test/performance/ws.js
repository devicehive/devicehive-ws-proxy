const WebSocket = require('ws');
const Message = require(`../../lib/Message`);
const status = require('node-status');

const TOTAL_MESSAGES = 240;

const sendedStatus = status.addItem('sendedStatus', { max: TOTAL_MESSAGES });
const recievedStatus = status.addItem('recievedStatus', { max: TOTAL_MESSAGES });


const ws = new WebSocket('ws://localhost:3000');
let sendCounter = 0;
let counter = 0;
let interval;

status.start({
	invert: true,
	interval: 200,
	pattern: 'Doing work: {uptime}  |  {spinner.cyan}  |  {sendedStatus.bar} | {recievedStatus.bar}'
});


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
		counter++;
		recievedStatus.inc();

		if (counter === TOTAL_MESSAGES) {
			ws.close();
			process.exit();
		}
	} else if (message.type === Message.TOPIC_TYPE && message.action === Message.SUBSCRIBE_ACTION) {
		interval = setInterval(() => {
			if (sendCounter < TOTAL_MESSAGES) {
				for(let i = 0; i < 1; i++) {
					ws.send(new Message({
						type: Message.NOTIFICATION_TYPE,
						action: Message.CREATE_ACTION,
						payload: {t: "topicN", m: `counter:${sendCounter}HelloWorld!HelloWorld!HelloWorld!HelloWorld!`}
					}).toString());
					sendCounter++;
					sendedStatus.inc();
				}
			} else {
				clearInterval(interval);
			}
		}, 500);
	}
});