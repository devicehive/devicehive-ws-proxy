const EventEmitter = require(`events`);
const Utils = require(`../../utils`);
const Config = require(`./config.json`);
const NoKafka = require(`no-kafka`);
const uuid = require(`uuid/v1`);
const debug = require(`debug`)(`kafka`);


/**
 *
 */
class Kafka extends EventEmitter {

	static get INTERNAL_TOPIC_PREFIX() { return `__` };
	static get HEALTH_TOPIC() { return `__health__` };

	constructor() {
		super();

        const me = this;
		const clientUUID = uuid();

        me.isProducerReady = false;
        me.isConsumerReady = false;
        me.available = true;
        me.subscriptionMap = new Map();
        me.producer = new NoKafka.Producer({
	        clientId: `${Config.KAFKA_CLIENT_ID}-${clientUUID}`,
	        connectionString: Config.KAFKA_HOSTS,
	        logger: {
		        logLevel: Config.LOGGER_LEVEL
	        }
        });
		me.consumer = new NoKafka.GroupConsumer({
			clientId: `${Config.KAFKA_CLIENT_ID}-${clientUUID}`,
			connectionString: Config.KAFKA_HOSTS,
			groupId: Config.CONSUMER_GROUP_ID,
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

        me.on(`producerReady`, () => {
	        me.createTopics([ Kafka.HEALTH_TOPIC ])
		        .then((topics) => {
			        me.consumer
				        .init({
					        strategy: NoKafka.RoundRobinAssignment,
					        subscriptions: topics,
					        handler: (messageSet, topic, partition) => me._onMessage(messageSet, topic, partition)
				        })
				        .then(() => {
					        me.isConsumerReady = true;
					        debug(`Consumer is ready`);
					        me.emit(`consumerReady`);
				        })
				        .catch((error) => {
					        debug(`Consumer error: ${error}`);
					        me.isConsumerReady = false;
				        });
		        });
        });
    }

    getProducer() {
        const me = this;

        return me.isProducerReady ?
            Promise.resolve(me.producer) :
            new Promise((resolve) => me.on(`producerReady`, () => resolve(me.producer)));
    }

	getConsumer() {
		const me = this;

		return me.isConsumerReady ?
			Promise.resolve(me.consumer) :
			new Promise((resolve) => me.on(`consumerReady`, () => resolve(me.consumer)));
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
		const topicsToSubscribe = [];

		return me.getConsumer()
			.then((consumer) => {
				topicsList.forEach((topicName) => {
					let subscriptionSet = me.subscriptionMap.get(topicName);

					subscriptionSet ? subscriptionSet.add(subscriberId) : topicsToSubscribe.push(topicName);
				});

				return Promise.all(topicsToSubscribe.map(topicName => consumer.subscribe(topicName,
					(messageSet, topics, partition) => me._onMessage(messageSet, topics, partition))))
			})
			.then(() => {
				topicsToSubscribe.forEach((topicName) => {
					me.subscriptionMap.set(topicName, new Set().add(subscriberId));
				});

				debug(`Subscriber with id: ${subscriberId} has subscribed to the next topics: ${topicsList}`);

				return topicsList;
			});
	}

	unsubscribe(subscriberId, topicsList) {
		const me = this;
		const topicsToUnsubscribe = [];

		return me.getConsumer()
			.then((consumer) => {
				topicsList.forEach((topicName) => {
					let subscriptionSet = me.subscriptionMap.get(topicName);

					if (subscriptionSet) {
						subscriptionSet.delete(subscriberId);
						if (subscriptionSet.size === 0) {
							topicsToUnsubscribe.push(topicName);
						}
					}
				});

				return Promise.all(topicsToUnsubscribe.map(topicName => consumer.unsubscribe(topicName)))
			})
			.then(() => {
				topicsToUnsubscribe.forEach((topicName) => {
					me.subscriptionMap.delete(topicName);
				});

				debug(`Subscriber with id: ${subscriberId} has unsubscribed from the next topics: ${topicsList}`);

				return topicsList
			});
	}

	send(payload) {
		const me = this;

		return me.getProducer()
			.then((producer) => producer.send(payload))
			.catch((error) => {
				console.log(`============== ERROR =============`);
				console.log(error);
			});
	}

	removeSubscriber(subscriberId) {
		const me = this;
		const topicsToUnsubscribe = [];

		me.subscriptionMap.forEach((subscribersSet, topic) => {
			if (subscribersSet.has(subscriberId)) {
				topicsToUnsubscribe.push(topic);
			}
		});

		if (topicsToUnsubscribe.length > 0) {
			me.unsubscribe(subscriberId, topicsToUnsubscribe);r
		}
	}

	isAvailable() {
		const me = this;
		const initialConsumerBrokers = me.consumer.client.initialBrokers;
		const consumerBrokerConnections = me.consumer.client.brokerConnections;

		const isConsumerAvailable = Object.keys(initialConsumerBrokers).reduce((isPreviousAvailable, brokerIndex) => {
			return isPreviousAvailable ? true :
				(initialConsumerBrokers[brokerIndex].connected || consumerBrokerConnections[brokerIndex].connected);
		}, false);

		if (me.available === false && isConsumerAvailable === true) {
			return me.subscribe(Array.from(me.subscriptionMap.keys()))
				.then(() => {
					me.available = isConsumerAvailable;
					return me.available;
				});
		} else {
			me.available = isConsumerAvailable;
			return me.available ? Promise.resolve(me.available) : Promise.reject(me.available);
		}
	}

	_onMessage(messageSet, topics, partition) {
		const me = this;

		Utils.forEach(topics, (topic) => {
			const subscriptionSet = me.subscriptionMap.get(topic);

			if (subscriptionSet) {
				subscriptionSet.forEach((subscriberId) => {
					messageSet.forEach((message) => {
						me.emit(`message`, subscriberId, topic, message.message, partition);
					});
				});
			}
		});
	}
}


module.exports = Kafka;