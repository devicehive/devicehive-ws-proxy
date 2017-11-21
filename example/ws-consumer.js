const WebSocket = require('ws'),
    cfg = require('./config-test'),
    debug = require('debug')('ws-consumer');
    const pino = require(`pino`)({level: process.env.LEVEL || 'info'});
    const fs = require(`fs`);


const TOPIC_COUNT = process.env.TOPICS || (cfg.TOPICS_COUNT || 1);

process.on('uncaughtException', e => console.error(e))
    .on('SIGINT', ()=>{
        clearInterval(mrate);
        ws.close()});

const ws = new WebSocket(process.env.WSS_URL || cfg.WSS_URL, {perMessageDeflate: false});

let awg_counter = 0;
let awg = 0;
let counter = 0;
let last_counter = 0;

const mrate = setInterval(function () {
    let rate = counter - last_counter;
    if(rate > 0) {
        awg = Math.ceil((awg * awg_counter + rate) / ++awg_counter);
        pino.info(`processed ${counter} @${rate} msgs/s with average rate ${awg} msgs/s`);
        last_counter = counter;
    }
}, 1000);

// const wrStream = fs.createWriteStream(`./messages`);

ws.on('open', () => {
    subscribeTopics(ws);
}).on('message', (data) => {
    let msg = JSON.parse(data);

    if(msg.id === "00"){
       if(msg.s === 1){
           ws.close();
           process.exit(1);
       }
    }else{
        if(Array.isArray(msg)){
            //counter += msg.length;
            // handleMsg(msg[0], counter);
            msg.forEach(m =>{
                try {
                    let json = JSON.parse(m.p);
                    if (json.counter) {
                        counter++;
                    }
                } catch (err) {}
                handleMsg(m, counter);
            });

        }else {
            try {
                let json = JSON.parse(msg.p);
                if (json.counter) {
                    counter++;
                }
            } catch (err) {}
            handleMsg(msg, counter);
        }

        // debug(msg);
    }
});
let cnt = 0;
function handleMsg(msg, counter, skipCounter = false){
    // wrStream.write(`${JSON.stringify(msg)}\n`);
    if(Math.floor(counter / 100000) > cnt || skipCounter){
        cnt++;
    // if(counter % 100000 === 0 || skipCounter){
        let lag = -1;
        if(msg.p){
            lag = new Date().getTime() - msg.p;
        }

        pino.info(`${counter} messages received with interval ${lag}`);
    }
}

function subscribeTopics(ws) {
    let msg = {
        id:"0000",
        t:"topic",
        a:"subscribe",
        p:{"t":[],consumer_group:"ingestion_1"}
    };

    for(let i = 0; i < TOPIC_COUNT; i++){
        msg.p.t.push(`topic_1${i}`);
    }

    ws.send(JSON.stringify(msg));
}