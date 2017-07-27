'use strict';

const WSKafka = require('../ws-kafka').WSKafkaConnector,
    debug = require('debug')('ws-kafka:test'),
    cfg = require('./config'),
    path = require('path');

let conf_module = {};

function getBrokerList(){
    return process.env.KAFKA_MBR || cfg.MBR_LIST;
}

function getWebSocketPort(){
    return process.env.WSS_PORT || cfg.WSS_PORT;
}

try {

    if (process.env.CONF_DIR) {
        const p = path.format({dir: process.env.CONF_DIR,  base: 'ws_kafka_config' });
        conf_module = require(p);
        debug(`config loaded form ${p}`);
    }
}catch(e){
    debug(`default config loaded`);

    conf_module.kafka_config = {
        //node-kafka options
        kafkaHost: getBrokerList(),
        clientId: 'test-kafka-client-2',
        connectTimeout: 1000,
        requestTimeout: 60000,
        autoConnect: true,
        //custom options
        no_zookeeper_client: true
    };

    conf_module.websocket_config ={
        port: getWebSocketPort()
    };

    conf_module.producer_config = {
        requireAcks: 1,
        ackTimeoutMs: 100,
        partitionerType: 2,
        // custom options
        mq_limit: 20000,
        mq_interval: 200 //if null, then messages published immediately
    };

    conf_module.consumer_config ={
        // host: 'zookeeper:2181',  // zookeeper host omit if connecting directly to broker (see kafkaHost below)
        kafkaHost: getBrokerList(),
        ssl: true, // optional (defaults to false) or tls options hash
        groupId: 'kafka-node-group', //should be set by message to ws
        autoCommit: true,
        autoCommitIntervalMs: 500,
        // Fetch message config
        fetchMaxWaitMs: 100,
        paused: false,
        maxNumSegments: 1000,
        fetchMinBytes: 1,
        fetchMaxBytes: 1024 * 1024,
        maxTickMessages: 1000,
        fromOffset: 'latest',
        outOfRangeOffset: 'earliest',
        sessionTimeout: 30000,
        retries: 10,
        retryFactor: 1.8,
        retryMinTimeout: 1000,
        connectOnReady: true,
        migrateHLC: false,
        migrateRolling: true,
        protocol: ['roundrobin'],
        // custom options
        mq_limit: 5000,
        mq_interval: 50 //if null, then messages published immediately
    };
}

const wsk = new WSKafka(conf_module);

wsk.on('ws-connection', (ws, req) => debug('connection'))
    .on('ws-close', () => debug('ws-close'))
    .on('wss-ready', () => debug('wss-ready'))
    .on('producer-ready', () => debug('producer-ready'))
    .on('producer-error', (e) => console.log(`producer-error ${e}`))
    .on('consumer-ready', () => debug('consumer-ready'))
    .on('consumer-error', (e) => console.log(`consumer-error ${e}`))
    .on('consumer-message', () => {})
    .on('error', (e) => console.log(`error ${e}`));


process.on('uncaughtException', e => {
    debug(e);
    wsk.stop.bind(wsk);
    wsk.stop();
}).on('SIGINT', function exit(){
        debug("EXIT");
        wsk.stop();
    }
);

try {
    wsk.start();
}catch(e){
    debug(e);
    wsk.stop();
}

