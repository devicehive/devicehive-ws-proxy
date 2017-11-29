const WebSocket = require('ws'),
    cfg = require('./config-test'),
    debug = require('debug')('ws-producer');
    const pino = require(`pino`)({level: process.env.LEVEL || 'info'});

const MPS = process.env.MSG_RATE || (cfg.MESSAGE_RATE || 10000 );
const TOTAL_MSGS = process.env.TOTAL_MSGS || (cfg.TOTAL_MESSAGES || 1000000);
const TOPIC_COUNT = process.env.TOPICS || (cfg.TOPICS_COUNT || 1);

process
    .on('uncaughtException', e => console.error(e))
    .on('SIGINT', () => { process.exit(0) });

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

const ws = new WebSocket(process.env.WSS_URL || cfg.WSS_URL, {perMessageDeflate: false});


ws.on('open', async function open() {
    createTopics(ws);
})
    .on('error', e => console.error(e))
    .on('message', async function incoming(data) {
        let localCounter = counter;
        let msg = JSON.parse(data);
        debug(msg);
        if(msg.t === 'topic' && msg.id === "0000" && msg.s === 0){
            // ws.removeEventListener('message', incoming);
            pino.info(msg);
            pino.info(`topics created: ${msg.p}`);
            sendPayload(ws);
        }else if(msg.s != 0){
            console.error(msg, localCounter);
            process.exit(1);
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

let counter = 0;

async function sendPayload(ws){
            let now = new Date().getTime();
            pino.info(`Total: ${TOTAL_MSGS}`);
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
                        p: {t: `topic_10`, m: JSON.stringify({date : new Date().getTime(), counter})}
                    };

                    msgs.push(msg)
                }
                if(msgs.length > 0){
                    console.log(msgs.length);
                    ws.send(JSON.stringify(msgs), function sent(){
                        pino.info(`${counter} | ${new Date().getTime() - now} ms`);
                    });
                    msgs.length = 0;
                }

                // pino.info(`${counter} | ${new Date().getTime() - now} ms`);
                let wait_time = (now + 1000) - new Date().getTime();
                if (wait_time > 0)
                    await sleep(wait_time);

                now = new Date().getTime();
            }
            console.log(counter);
            process.exit(0);
            // ws.close(1000);

}

function rand(min, max) {
    return Math.floor(Math.random() * (max - min + 1) + min);
}
