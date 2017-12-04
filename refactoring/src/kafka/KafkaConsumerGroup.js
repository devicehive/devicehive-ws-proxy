const Config = require(`./config.json`);
const { ConsumerGroup } = require(`kafka-node`);
const debug = require(`debug`)(`kafkaconsumergroup`);


class KafkaConsumerGroup extends ConsumerGroup {

    constructor(topics) {
	    super({
		    kafkaHost: Config.KAFKA_HOSTS,
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
	    }, topics);

        const me = this;

        me.isReady = false;

	    me.on(`message`, (data) => {
	        me.emit('message')
	    });

	    me.client.on(`ready`, () => {
		    me.isReady = true;
		    me.emit(`ready`);
	    });
    }
}


module.exports = KafkaConsumerGroup;