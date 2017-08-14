const WebSocket = require('ws'),
    cfg = require('./config-test'),
    debug = require('debug')('ws-consumer');


const TOPIC_COUNT = process.env.TOPICS || (cfg.TOPICS_COUNT || 1);

process.on('uncaughtException', e => console.error(e))
    .on('SIGINT', ()=>{
        clearInterval(mrate);
        ws.close()});

const ws = new WebSocket(process.env.WSS_URL || cfg.WSS_URL);

let awg_counter = 0;
let awg = 0;
let counter = 0;
let last_counter = 0;

const mrate = setInterval(function () {
    let rate = counter - last_counter;
    if(rate > 0) {
        awg = Math.ceil((awg * awg_counter + rate) / ++awg_counter);
        console.log(`processed ${counter} @${rate} msgs/s with average rate ${awg} msgs/s`);
        last_counter = counter;
    }
}, 1000);

ws.on('open', () => {
    subscribeTopics(ws);
}).on('message', (data) => {
    let msg = JSON.parse(data);
    if(msg.refid === "0000"){
       if(msg.s === 1){
           ws.close();
       }
    }else{
        if(Array.isArray(msg)){
            counter += msg.length;
            handleMsg(msg[0], counter, true);
            // msg.forEach(m =>{
            //     counter++;
            //     handleMsg(m, counter);
            // });

        }else {
            counter++;
            handleMsg(msg, counter);
        }

        debug(msg);
    }
});

function handleMsg(msg, counter, skipCounter = false){
    if(counter % 100000 === 0 || skipCounter){
        let lag = -1;
        if(msg.p){
            lag = new Date().getTime() - msg.p;
        }

        console.log(`${counter} messages received with interval ${lag}`);
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
        msg.p.t.push(`topic_${i}`);
    }

    ws.send(JSON.stringify(msg));
}