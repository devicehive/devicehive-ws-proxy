const EventEmitter = require(`events`);
const Utils = require(`../../utils`);
const Config = require(`./config.json`);
const NoKafka = require(`no-kafka`);
const uuid = require(`uuid/v1`);
const debug = require(`debug`)(`kafka`);

//require(`./patch.js`);

/**
 *
 */
class Kafka extends EventEmitter {

	static get INTERNAL_TOPIC_PREFIX() { return `__` };

	constructor() {
		super();

        const me = this;

        me.clientUUID = uuid();
        me.isProducerReady = false;
        me.subscriptionMap = new Map();
        me.producer = new NoKafka.Producer({
	        clientId: `${Config.KAFKA_CLIENT_ID}-${me.clientUUID}`,
	        connectionString: Config.KAFKA_HOSTS,
	        logger: {
		        logLevel: Config.LOGGER_LEVEL
	        }
        });

        me.producer
			.init()
			.then(() => {
                me.isProducerReady = true;
				debug(`Producer is ready`);
                me.emit(`producerReady`);
			})
			.catch((error) => {
				debug(`Producer error: ${error}`);
                me.isProducerReady = false;
			});
    }

    getProducer() {
        const me = this;

        return me.isProducerReady ?
            Promise.resolve(me.producer) :
            new Promise((resolve) => me.on(`producerReady`, () => resolve(me.producer)));
    }

	createTopics(topicsList) {
		const me = this;

		return me.getProducer()
			.then((producer) => producer.client.metadataRequest(topicsList))
			.then(() => {
				debug(`Next topics has been created: ${topicsList}`);
				return topicsList;
			});
	}

	listTopics() {
		const me = this;

		return me.getProducer()
			.then((producer) => producer.client.metadataRequest())
			.then((metadata) => {
				const result = [];

				metadata.topicMetadata
					.filter((topicObject) => !topicObject.topicName.startsWith(Kafka.INTERNAL_TOPIC_PREFIX))
					.forEach((topicObject) => {
						topicObject.partitionMetadata.forEach((topicPartitionData) => {
							result.push({ topic: topicObject.topicName, partition: topicPartitionData.partitionId })
						});
					});

				return result;
			});
	}

	subscribe(subscriberId, topicsList) {
		const me = this;
		const promiseArray =  topicsList.map((topic) => {
        	const groupUUID = uuid();
            const consumer = new NoKafka.GroupConsumer({
                clientId: `${Config.KAFKA_CLIENT_ID}-${me.clientUUID}`,
                connectionString: Config.KAFKA_HOSTS,
                groupId: `${Config.CONSUMER_GROUP_ID}-${groupUUID}`,
                logger: {
                    logLevel: Config.LOGGER_LEVEL
                }
            });

            me.subscriptionMap.set(`${subscriberId}:${topic}`, consumer);

            return consumer.init({
                strategy: NoKafka.RoundRobinAssignment,
                subscriptions: [topic],
                handler: (messageSet, topic, partition) => {
                	me._onMessage(subscriberId, messageSet, topic, partition);
                }
            });
		});

        return Promise.all(promiseArray)
			.then(() => topicsList);
	}

	unsubscribe(subscriberId, topicsList) {
		const me = this;
        const promiseArray = [];

		topicsList.forEach((topic) => {
            const consumer = me.subscriptionMap.get(`${subscriberId}:${topic}`);

            if (consumer) {
                promiseArray.push(consumer.end());
			}
        });

        return Promise.all(promiseArray)
            .then(() => {
                topicsList.forEach((topic) => {
                    me.subscriptionMap.delete(`${subscriberId}:${topic}`);
                });

            	return topicsList;
            })
        	.catch((error) => console.log(error));
	}

	send(payload) {
		const me = this;

		return me.getProducer()
			.then((producer) => producer.send(payload));
	}

	removeSubscriber(subscriberId) {
		const me = this;
		const topicsToUnsubscribe = [];

		me.subscriptionMap.forEach((consumer, subscriberTopic) => {
			const [ subscriber, topic ] = subscriberTopic.split(`:`);

			if (subscriber === subscriberId) {
				topicsToUnsubscribe.push(topic);
			}
		});

		if (topicsToUnsubscribe.length > 0) {
			me.unsubscribe(subscriberId, topicsToUnsubscribe);
		}
	}

    isAvailable() {
        const me = this;

        return me.subscriptionMap.size === 0 ? true : Array.from(me.subscriptionMap.values()).reduce((prev, consumer) => {
            const initialConsumerBrokers = consumer.client.initialBrokers;
            const consumerBrokerConnections = consumer.client.brokerConnections;

            return prev ? true : Object.keys(initialConsumerBrokers).reduce((isPreviousAvailable, brokerIndex) => {
                return isPreviousAvailable ? true :
                    (initialConsumerBrokers[brokerIndex].connected || consumerBrokerConnections[brokerIndex].connected);
            }, false);
        }, false);
    }

	_onMessage(subscriberId, messageSet, topic, partition) {
		const me = this;

        messageSet.forEach((message) => {
        	me.emit(`message`, subscriberId, topic, message.message, partition);
        });
	}
}


module.exports = Kafka;