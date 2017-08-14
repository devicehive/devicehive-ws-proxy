const WebSocket = require('ws'),
    cfg = require('./config-test'),
    debug = require('debug')('ws-producer');
    const pino = require(`pino`)();

const MPS = process.env.MSG_RATE || (cfg.MESSAGE_RATE || 10000 );
const TOTAL_MSGS = process.env.TOTAL_MSGS || (cfg.TOTAL_MESSAGES || 1000000);
const TOPIC_COUNT = process.env.TOPICS || (cfg.TOPICS_COUNT || 1);

process.on('uncaughtException', e => console.error(e));

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

const ws = new WebSocket(process.env.WSS_URL || cfg.WSS_URL);

ws.on('open', async function open() {
    createTopics(ws);
})
    .on('error', e => console.error(e))
    .on('message', async function incoming(data) {
        let msg = JSON.parse(data);
        pino.info(msg);
        if(msg.refid === "0000" && msg.s === 0){
            pino.info(`topics created: ${msg.p}`);
            sendPayload(ws);
        }

    });

function createTopics(ws) {
    msg = {
        id: "0000",
        t: "topic",
        a: "create",
        p:[]
    };

    for(let i = 0; i < TOPIC_COUNT; i++){
        msg.p.push(`topic_${i}`);
    }
    pino.info(msg);
    ws.send(JSON.stringify(msg));
}

async function sendPayload(ws){
            let now = new Date().getTime();
            let counter = 0;
            pino.info(TOTAL_MSGS);
            while (counter < TOTAL_MSGS) {

                if (ws.readyState > 1) {
                    // debug(`WebSocket State ${ws.readyState}`);
                }
                let msgs = [];

                for (let i = 0; i < MPS; i++) {
                    msg = {
                        id: counter++,
                        t: "notif",
                        a: "create",
                        p: {t: `topic_0`, m: new Date().getTime()}
                    };

                    msgs.push(msg)
                }
                if(msgs.length > 0){
                    ws.send(JSON.stringify(msgs));
                    msgs.length = 0;
                }

                pino.info(`${counter} | ${new Date().getTime() - now} ms`);
                let wait_time = (now + 1000) - new Date().getTime();
                if (wait_time > 0)
                    await sleep(wait_time);

                now = new Date().getTime();
            }
            ws.close(1000);

}

function rand(min, max) {
    return Math.floor(Math.random() * (max - min + 1) + min);
}
