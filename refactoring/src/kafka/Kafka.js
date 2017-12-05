const Utils = require(`../../utils`);
const Config = require(`./config.json`);
const { KafkaClient } = require(`kafka-node`);
const KafkaConsumerGroup = require(`./KafkaConsumerGroup`);
const KafkaProducer = require(`./KafkaProducer`);
const uuid = require(`uuid/v1`);
const stringHash = require("string-hash");
const debug = require(`debug`)(`kafka`);


/**
 *
 */
class Kafka extends KafkaClient {

    static get INTERNAL_TOPIC_PREFIX() { return `__` };

    static generateKafkaClientId() {
	    return `${Config.KAFKA_CLIENT_PREFIX}-${uuid()}`;
    }

	constructor() {
        super({
            kafkaHost: Config.KAFKA_HOSTS,
            clientId: Kafka.generateKafkaClientId(),
            connectTimeout: 1000,
            requestTimeout: 60000,
            autoConnect: true,
        });

        const me = this;

        me.isProducerReady = false;
        me.isMetadataNeedUpdate = true;
        me.subscriptionMap = new Map();

        me.producer = new KafkaProducer(me);
        me.consumer = new KafkaConsumerGroup([`__health__`]);
    }

    getProducer() {
        const me = this;

        return me.producer.isReady ?
            Promise.resolve(me.producer) :
            new Promise((resolve) => me.producer.on(`ready`, () => resolve(me.producer)));
    }

	getConsumer() {
		const me = this;

		return me.consumer.isReady ?
			Promise.resolve(me.consumer) :
			new Promise((resolve) => me.consumer.on(`ready`, () => resolve(me.consumer)));
	}

	//TODO
    addTopicsToConsumer(topicsList) {
        const me = this;

        return new Promise ((resolve, reject) => {
            me.getConsumer()
                .then((consumer) => {
	                consumer.addTopics(topicsList, (error, added) => {
                        error ? reject(error) : resolve(added);
                    });
                });
	    });
    }

    //TODO
    removeTopicsFromConsumer(topicsList) {
	    const me = this;

	    return new Promise ((resolve, reject) => {
		    me.getConsumer()
			    .then((consumer) => {
				    consumer.removeTopics(topicsList, (error, removed) => {
					    error ? reject(error) : resolve(removed);
				    });
			    });
	    });
    }

    createTopicsConsumer() {

	}


	createClientTopics(topicsList) {
		const me = this;

		return new Promise((resolve, reject) => {
			me.getProducer()
				.then((producer) => {
					producer.createTopics(topicsList, true, (error, data) => {
						me.isMetadataNeedUpdate = true;
						error ? reject(error) : resolve(data);
					});
				});
		});
	}

	listTopics() {
		const me = this;

		return new Promise((resolve, reject) => {
			me.loadMetadataForTopics([], (error, metadata) => {
				if (error) {
					reject(error);
				} else {
					const result = [];
					const topicsObject = metadata[1].metadata;
					const topics = Object.keys(topicsObject);

					me.updateMetadatas(metadata);
					me.isMetadataNeedUpdate = false;

					topics.forEach(topic => {
						if (topicsObject[topic]) {
							const partitions = Object.keys(topicsObject[topic]).filter(
							    topic => !topic.startsWith(Kafka.INTERNAL_TOPIC_PREFIX));

							partitions.forEach(partition => result.push({
								topic: topic,
								partition: parseInt(partition, 10)
							}));
						}
					});

					return resolve(result);
				}
			});
		});
	}

	subscribe(subscriberId, topicsList) {
		const me = this;

		return me.addTopicsToConsumer(topicsList)
			.then((added) => {
				topicsList.forEach((topic) => {
					let subscribersIdSet = me.subscriptionMap.get(topic);

					if (subscribersIdSet) {
						subscribersIdSet.add(subscriberId);
					} else {
						subscribersIdSet = new Set().add(subscriberId);
						me.subscriptionMap.set(topic, subscribersIdSet);
					}
				});

				return added;
			});
	}

	unsubscribe(subscriberId, topicsList) {
		const me = this;

		return new Promise(() => {
			topicsList.forEach((topic) => {
				let subscribersIdSet = me.subscriptionMap.get(topic);

				if (subscribersIdSet) {
					subscribersIdSet.delete(subscriberId);
				}

				return subscribersIdSet.size === 0 ? me.removeTopicsFromConsumer(topicsList) : undefined;
			});
        });
	}

	sendData(payload) {
		const me = this;

		return new Promise((resolve, reject) => {
			me.getProducer()
				.then((producer) => {
			        if (me.isMetadataNeedUpdate === true) {
				        me.loadMetadataForTopics([], (error, metadata) => {
					        if (error) {
						        reject(error);
					        } else {
						        me.kafka.updateMetadatas(metadata);
						        me.isMetadataNeedUpdate = false;

						        producer.send(Utils.toArray(payload), (error, data) => {
							        error ? reject(error) : resolve(data);
						        });
					        }
				        });
			        } else {
				        producer.send(Utils.toArray(payload), (error, data) => {
					        error ? reject(error) : resolve(data);
				        });
                    }
				});
		});
	}
}


module.exports = Kafka;