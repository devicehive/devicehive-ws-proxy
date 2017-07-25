function getBrokerList(){
    return process.env.KAFKA_MBR || 'kafka:9094';
}

function getWebSocketPort(){
    return process.env.WSS_PORT || '8085';
}

module.exports.kafka_config = {
    //node-kafka options
    kafkaHost: getBrokerList(),
    clientId: 'test-kafka-client-2',
    connectTimeout: 1000,
    requestTimeout: 60000,
    autoConnect: true,
    //custom options
    no_zookeeper_client: true
};

module.exports.websocket_config ={
    port: getWebSocketPort()
};

module.exports.producer_config = {
    requireAcks: 1,
    ackTimeoutMs: 100,
    partitionerType: 2,
    // custom options
    mq_limit: 20000,
    mq_interval: 50 //if null, then messages published immediately
};

// module.exports.consumer_config ={
//     groupId: 'kafka-node-group', //should be set by message to ws
//     // Auto commit config
//     autoCommit: true,
//     autoCommitMsgCount: 100,
//     autoCommitIntervalMs: 100,
//     // Fetch message config
//     fetchMaxWaitMs: 100,
//     fetchMinBytes: 1,
//     fetchMaxBytes: 1024 * 1024,
//     // Offset
//     fromOffset: false
// };

module.exports.consumer_config ={
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