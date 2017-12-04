const { KafkaClient } = require(`kafka-node`);
const KafkaConsumerGroup = require(`./KafkaConsumerGroup`);
const KafkaProducer = require(`./KafkaProducer`);
const debug = require(`debug`)(`kafka`);


class Kafka extends KafkaClient {

    constructor(onMassage) {
        super({
            kafkaHost: getBrokerList(),
            clientId: 'test-kafka-client-2',
            connectTimeout: 1000,
            requestTimeout: 60000,
            autoConnect: true,
        });

        const me = this;

        me.consumer = new KafkaConsumerGroup({
            kafkaHost: ``,
            ssl: true,
            groupId: 'kafka-node-group',
            autoCommit: true,
            autoCommitIntervalMs: 5,
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
            protocol: ['roundrobin']
        });

        me.producer = new KafkaProducer(me, {
            requireAcks: 1,
            ackTimeoutMs: 100,
            partitionerType: 2
        });
    }

}


module.exports = Kafka;