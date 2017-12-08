const EventEmitter = require(`events`);
const Utils = require(`../../utils`);
const KafkaConfig = require(`./KafkaConfig`);
const NoKafka = require(`no-kafka`);
const Consumer = require(`./Consumer`);
const uuid = require(`uuid/v1`);
const debug = require(`debug`)(`kafka`);


/**
 * Kafka communicator implementation
 * Implements next interface:
 *      - createTopics
 *      - listTopics
 *      - subscribe
 *      - unsubscribe
 *      - send
 *      - removeSubscriber
 *      - isAvailable
 * @event "message"
 */
class Kafka extends EventEmitter {

    static get INTERNAL_TOPIC_PREFIX() { return `__` };
    static get HEALTH_TOPIC() { return `__health__` };

    /**
     * Creates new Kafka
     */
    constructor() {
        super();

        const me = this;
        const clientUUID = uuid();

        me.isProducerReady = false;
        me.isConsumerReady = false;
        me.available = true;
        me.subscriptionMap = new Map();
        me.producer = new NoKafka.Producer({
            clientId: `${KafkaConfig.KAFKA_CLIENT_ID}-${clientUUID}`,
            connectionString: KafkaConfig.KAFKA_HOSTS,
            logger: {
                logLevel: KafkaConfig.LOGGER_LEVEL
            }
        });
        me.consumer = new Consumer({
            clientId: `${KafkaConfig.KAFKA_CLIENT_ID}-${clientUUID}`,
            connectionString: KafkaConfig.KAFKA_HOSTS,
            groupId: `${KafkaConfig.CONSUMER_GROUP_ID}-${clientUUID}`,
            logger: {
                logLevel: KafkaConfig.LOGGER_LEVEL
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

    /**
     * Returns ready producer
     * @returns {Producer}
     */
    getProducer() {
        const me = this;

        return me.isProducerReady ?
            Promise.resolve(me.producer) :
            new Promise((resolve) => me.on(`producerReady`, () => resolve(me.producer)));
    }

    /**
     * Returns ready consumer
     * @returns {Consumer}
     */
    getConsumer() {
        const me = this;

        return me.isConsumerReady ?
            Promise.resolve(me.consumer) :
            new Promise((resolve) => me.on(`consumerReady`, () => resolve(me.consumer)));
    }

    /**
     * Creates Kafka topics by topicsList
     * @param topicsList
     * @returns {Promise<Array>}
     */
    createTopics(topicsList) {
        const me = this;

        return me.getProducer()
            .then((producer) => producer.client.metadataRequest(topicsList))
            .then(() => {
                debug(`Next topics has been created: ${topicsList}`);
                return topicsList;
            });
    }

    /**
     * Returns list of all existing topics
     * @returns {Promise<Array>}
     */
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

    /**
     * Subscribes consumer to each topic of topicsList and adds subscriberId to each subscription
     * @param subscriberId
     * @param topicsList
     * @returns {Promise<Array>}
     */
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
                    (messageSet, topics, partition) => me._onMessage(messageSet, topics, partition))));
            })
            .then(() => {
                topicsToSubscribe.forEach((topicName) => {
                    me.subscriptionMap.set(topicName, new Set().add(subscriberId));
                });

                debug(`Subscriber with id: ${subscriberId} has subscribed to the next topics: ${topicsList}`);

                return topicsList;
            });
    }

    /**
     * Unsubscribes consumer from each topic of topicsList and removes subscriberId from each subscription
     * @param subscriberId
     * @param topicsList
     * @returns {Bluebird<any>}
     */
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

    /**
     * Sends payload to Kafka over Kafka Producer
     * @param payload
     * @returns {Promise<>}
     */
    send(payload) {
        const me = this;

        return me.getProducer()
            .then((producer) => producer.send(payload));
    }

    /**
     * Removes subscriberId from each Consumer subscription
     * @param subscriberId
     */
    removeSubscriber(subscriberId) {
        const me = this;
        const topicsToUnsubscribe = [];

        me.subscriptionMap.forEach((subscribersSet, topic) => {
            if (subscribersSet.has(subscriberId)) {
                topicsToUnsubscribe.push(topic);
            }
        });

        if (topicsToUnsubscribe.length > 0) {
            me.unsubscribe(subscriberId, topicsToUnsubscribe);
        }
    }

    /**
     * Checks if Kafka is available
     * @returns {boolean}
     */
    isAvailable() {
        const me = this;

        return me.consumer.isAvailable !== false;
    }

    /**
     * Message handler
     * Emits next events:
     *      - message
     * @param messageSet
     * @param topics
     * @param partition
     * @private
     */
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