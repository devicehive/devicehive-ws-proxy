const KafkaConfig = require(`../../config`).kafka;
const EventEmitter = require(`events`);
const Utils = require(`../../utils`);
const NoKafka = require(`no-kafka`);
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

    static get MESSAGE_EVENT() { return `message`; }
    static get AVAILABLE_EVENT() { return `available`; }
    static get NOT_AVAILABLE_EVENT() { return `notAvailable`; }

    static get INTERNAL_TOPIC_PREFIX() { return `__` };

    /**
     * Generate subscription group identification key
     * @param subscriber
     * @param group
     * @param topic
     * @returns {string}
     * @private
     */
    static _generateSubscriptionGroupKey(subscriber, group,  topic) {
        return `${subscriber}:${group}:${topic}`;
    }

    /**
     * Returns Kafka producer config
     * @param clientId
     * @returns {{clientId: string, connectionString: *, logger: {logLevel: *}, batch: {maxWait: *, size: *}}}
     * @private
     */
    _getProducerConfig(clientId) {
        const me = this;
        const result = Object.assign({}, me.defaultProducerConfig);

        result.clientId = `${KafkaConfig.KAFKA_CLIENT_ID}-${clientId}`;

        return result;
    }

    /**
     * Returns Kafka consumer config
     * @param clientId
     * @param groupId
     * @returns {{clientId: string, connectionString: *, groupId: string, logger: {logLevel: *}, idleTimeout: *, maxWaitTime: *, maxBytes: *}}
     * @private
     */
    _getConsumerConfig(clientId, groupId) {
        const me = this;
        const result = Object.assign({}, me.defaultConsumerConfig);

        result.clientId = `${KafkaConfig.KAFKA_CLIENT_ID}-${clientId}`;
        result.groupId = `${KafkaConfig.CONSUMER_GROUP_ID_PREFIX}-${groupId || clientId}`;

        return result;
    }

    /**
     * Creates new Kafka
     */
    constructor() {
        super();

        const me = this;

        me.clientUUID = uuid();
        me.defaultProducerConfig = {
            clientId: `${KafkaConfig.KAFKA_CLIENT_ID}-${me.clientUUID}`,
            connectionString: KafkaConfig.KAFKA_HOSTS,
            logger: {
                logLevel: KafkaConfig.LOGGER_LEVEL
            }
        };
        me.defaultConsumerConfig = {
            clientId: `${KafkaConfig.KAFKA_CLIENT_ID}-${me.clientUUID}`,
            connectionString: KafkaConfig.KAFKA_HOSTS,
            groupId: `${KafkaConfig.CONSUMER_GROUP_ID_PREFIX}-${me.clientUUID}`,
            logger: {
                logLevel: KafkaConfig.LOGGER_LEVEL
            },
            idleTimeout: KafkaConfig.CONSUMER_IDLE_TIMEOUT,
            maxWaitTime: KafkaConfig.CONSUMER_MAX_WAIT_TIME,
            maxBytes: KafkaConfig.CONSUMER_MAX_BYTES
        };
        me.isProducerReady = false;
        me.isConsumerReady = false;
        me.available = true;
        me.subscriptionMap = new Map();
        me.subscriptionGroupMap = new Map();
        me.groupConsumersMap = new Map();
        me.producer = new NoKafka.Producer(me._getProducerConfig(me.clientUUID));
        me.consumer = new NoKafka.SimpleConsumer(me._getConsumerConfig(me.clientUUID));

        debug(`Started trying connect to server`);

        me.producer.init()
            .then(() => {
                me.isProducerReady = true;
                debug(`Producer is ready`);
                me.emit(`producerReady`);
            })
            .catch((error) => {
                debug(`Producer error: ${error}`);
                me.isProducerReady = false;
            });

        me.consumer.init()
            .then(() => {
                me.isConsumerReady = true;
                debug(`Consumer is ready`);
                me.emit(`consumerReady`);
                me.emit(Kafka.AVAILABLE_EVENT);
            })
            .catch((error) => {
                debug(`Consumer error: ${error}`);
                me.isConsumerReady = false;
            });

        me._metadata = null;
        me._topicArray = [];
        me._topicRequestSet = new Set();
        me._topicRequestEmitter = new EventEmitter();

        me._initMetadataPoller();

        me.inputThroughputWatcher = me.createThroughputWatcher(1000);
        me.outputThroughputWatcher = me.createThroughputWatcher(1000);
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
     * @returns {NoKafka.SimpleConsumer}
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
            .then(() => Promise.all(topicsList.map((topicName) => me._waitForTopic(topicName))))
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
            .then((metadata) => metadata.topicMetadata
                .filter((topicObject) => !topicObject.topicName.startsWith(Kafka.INTERNAL_TOPIC_PREFIX))
                .map((topicObject) => topicObject.topicName));
    }

    /**
     * Subscribes consumer to each topic of topicsList and adds subscriberId to each subscription
     * @param subscriberId
     * @param subscriptionGroup
     * @param topicsList
     * @returns {Promise<Array>}
     */
    subscribe(subscriberId, subscriptionGroup, topicsList) {
        const me = this;
        const topicsToSubscribe = new Set();
        const topicsToUnsubscribe = new Set();

        return topicsList && topicsList.length === 0 ? Promise.resolve(topicsList) : me.getProducer()
            .then((producer) => producer.client.metadataRequest(topicsList))
            .then(() => Promise.all(topicsList.map((topicName) => me._waitForTopic(topicName))))
            .then(() => {
                topicsList.forEach((topic) => {
                    const subscribersSet = me.subscriptionMap.get(topic) || new Set;
                    const groupSubscribersMap = me.subscriptionGroupMap.get(topic) || new Map;
                    const groupSubscribersSet = groupSubscribersMap.get(subscriptionGroup) || new Set;

                    if (Utils.isDefined(subscriptionGroup) &&
                        !groupSubscribersSet.has(subscriberId)) {
                        topicsToSubscribe.add(topic);

                        if (subscribersSet.has(subscriberId)) {
                            topicsToUnsubscribe.add(topic);
                        }

                        groupSubscribersMap.forEach((groupSubscribersSet) => {
                            if (groupSubscribersSet.has(subscriberId)) {
                                topicsToUnsubscribe.add(topic);
                            }
                        });
                    } else if (!Utils.isDefined(subscriptionGroup) &&
                        !subscribersSet.has(topic)) {
                        topicsToSubscribe.add(topic);

                        groupSubscribersMap.forEach((groupSubscribersSet) => {
                            if (groupSubscribersSet.has(subscriberId)) {
                                topicsToUnsubscribe.add(topic);
                            }
                        });
                    }
                });
            })
            .then(() => me.unsubscribe(subscriberId, Array.from(topicsToUnsubscribe)))
            .then(() => me.getConsumer())
            .then((consumer) => Utils.isDefined(subscriptionGroup) ?
                Promise.all(Array.from(topicsToSubscribe).map(topic => {
                    const groupConsumer = new NoKafka.GroupConsumer(me._getConsumerConfig(me.clientUUID, subscriptionGroup));

                    me.groupConsumersMap.set(
                        Kafka._generateSubscriptionGroupKey(subscriberId, subscriptionGroup, topic), groupConsumer);

                    return groupConsumer.init({
                        subscriptions: [ topic ],
                        handler: (messageSet, topic, partition) =>
                            me._onMessage(messageSet, topic, partition, subscriptionGroup, subscriberId)
                    });
                })) :
                Promise.all(Array.from(topicsToSubscribe).map(topic => consumer.subscribe(topic,
                    (messageSet, topic, partition) =>
                        me._onMessage(messageSet, topic, partition))))
            )
            .then(() => {
                Array.from(topicsToSubscribe).forEach((topic) => {
                    if (Utils.isDefined(subscriptionGroup)) {
                        const groupSubscribersMap = me.subscriptionGroupMap.get(topic) || new Map;
                        const groupSubscribersSet = groupSubscribersMap.get(subscriptionGroup) || new Set;

                        groupSubscribersSet.add(subscriberId);
                        groupSubscribersMap.set(subscriptionGroup, groupSubscribersSet);
                        me.subscriptionGroupMap.set(topic, groupSubscribersMap);
                    } else {
                        const subscribersSet = me.subscriptionMap.get(topic) || new Set;

                        subscribersSet.add(subscriberId);
                        me.subscriptionMap.set(topic, subscribersSet);
                    }
                });

                debug(`Subscriber with id: ${subscriberId} has subscribed to the next topics: ${topicsList}`);

                return topicsList;
            })
            .catch((error) => {
                debug(`Error while subscribing subscriber with id: ${subscriberId} to the next topics: ${topicsList}. Error: ${error}`);
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

        return topicsList && topicsList.length === 0 ? Promise.resolve(topicsList) : me.getProducer()
            .then((producer) => producer.client.metadataRequest(topicsList))
            .then(() => Promise.all(topicsList.map((topicName) => me._waitForTopic(topicName))))
            .then(() => me.getConsumer())
            .then((consumer) => {
                topicsList.forEach((topic) => {
                    const subscribersSet = me.subscriptionMap.get(topic) || new Set;
                    const groupSubscribersMap = me.subscriptionGroupMap.get(topic) || new Map;

                    if (subscribersSet.has(subscriberId)) {
                        subscribersSet.delete(subscriberId);

                        if (subscribersSet.size === 0) {
                            topicsToUnsubscribe.push(topic);
                        }
                    }

                    groupSubscribersMap.forEach((groupSubscribersSet, group) => {
                        if (groupSubscribersSet.has(subscriberId)) {
                            const groupConsumerKey = Kafka._generateSubscriptionGroupKey(subscriberId, group, topic);
                            const groupConsumer = me.groupConsumersMap.get(groupConsumerKey);

                            groupConsumer.end();
                            me.groupConsumersMap.delete(groupConsumerKey);
                            groupSubscribersSet.delete(subscriberId);
                        }
                    });
                });

                return Promise.all(topicsToUnsubscribe.map(topicName => consumer.unsubscribe(topicName)))
            })
            .then(() => {
                topicsToUnsubscribe.forEach((topic) => {
                    me.subscriptionMap.delete(topic);
                });

                debug(`Subscriber with id: ${subscriberId} has unsubscribed from the next topics: ${topicsList}`);

                return topicsList
            })
            .catch((error) => {
                debug(`Error while unsubscribing subscriber with id: ${subscriberId} from the next topics: ${topicsList}. Error: ${error}`);
            });
    }

    /**
     * Sends payload to Kafka over Kafka Producer
     * @param payload
     * @returns {Promise<>}
     */
    send(payload) {
        const me = this;
        let throughput, isThroughputSmall;

        me.inputThroughputWatcher.calculate(JSON.stringify(payload));
        throughput = me.inputThroughputWatcher.getThroughput();
        isThroughputSmall = throughput < KafkaConfig.PRODUCER_MINIMAL_BATCHING_THROUGHPUT_PER_SEC_B;

        return me.getProducer()
            .then((producer) => producer.send(payload, {
                batch: {
                    size: isThroughputSmall ? 0 : throughput / 50,
                    maxWait: isThroughputSmall ? 0 : 15
                }
            }));
    }

    /**
     * Removes subscriberId from each Consumer subscription
     * @param subscriberId
     */
    removeSubscriber(subscriberId) {
        const me = this;
        const topicsToUnsubscribeSet = new Set();

        me.subscriptionMap.forEach((subscribersSet, topic) => {
            if (subscribersSet.has(subscriberId)) {
                topicsToUnsubscribeSet.add(topic);
            }
        });

        me.subscriptionGroupMap.forEach((groupSubscribersMap, topic) => {
            groupSubscribersMap.forEach((subscribersSet) => {
                if (subscribersSet.has(subscriberId)) {
                    topicsToUnsubscribeSet.add(topic);
                }
            });
        });

        if (topicsToUnsubscribeSet.size > 0) {
            me.unsubscribe(subscriberId, Array.from(topicsToUnsubscribeSet));
        }
    }

    /**
     * Returns average input load
     * @returns {Number}
     */
    getAverageInputLoad() {
        const me = this;

        return me.inputThroughputWatcher.getThroughput();
    }

    /**
     * Returns average output load
     * @returns {Number}
     */
    getAverageOutputLoad() {
        const me = this;

        return me.outputThroughputWatcher.getThroughput();
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
     * @param subscriptionGroup
     * @param subscriber
     * @private
     */
    _onMessage(messageSet, topics, partition, subscriptionGroup, subscriber) {
        const me = this;

        Utils.forEach(topics, (topic) => {
            if (Utils.isDefined(subscriptionGroup)) {
                messageSet.forEach((message) => {
                    const payload = message.message.value.toString();

                    me.outputThroughputWatcher.calculate(payload);
                    me.emit(Kafka.MESSAGE_EVENT, subscriber, topic, payload, partition);
                });
            } else {
                const subscriptionSet = me.subscriptionMap.get(topic);

                if (subscriptionSet) {
                    subscriptionSet.forEach((subscriberId) => {
                        messageSet.forEach((message) => {
                            const payload = message.message.value.toString();

                            me.outputThroughputWatcher.calculate(payload);
                            me.emit(Kafka.MESSAGE_EVENT, subscriberId, topic, payload, partition);
                        });
                    });
                }
            }
        });
    }

    /**
     *
     * @private
     */
    _initMetadataPoller() {
        const me = this;

        debug(`Metadata polling started with interval: ${KafkaConfig.METADATA_POLLING_INTERVAL_MS} ms`);

        setInterval(() => {
            me.getProducer()
                .then((producer) => producer.client.metadataRequest())
                .then((metadata) => {
                    me._metadata = metadata;
                    me._topicArray = me._metadata.topicMetadata.map((topicMetadata) => topicMetadata.topicName);

                    me._topicArray.forEach((topicName) => {
                        if (me._topicRequestSet.has(topicName)) {
                            me._topicRequestEmitter.emit(topicName);
                            me._topicRequestSet.delete(topicName);
                        }
                    });
                });
        }, KafkaConfig.METADATA_POLLING_INTERVAL_MS);
    }

    /**
     *
     * @param topicName
     * @returns {Promise<any>}
     * @private
     */
    _waitForTopic(topicName) {
        const me = this;

        return new Promise((resolve) => {
            if (me._topicArray.includes(topicName)) {
                resolve();
            } else {
                me._topicRequestSet.add(topicName);
                me._topicRequestEmitter.once(topicName, () => {
                    resolve();
                });
            }
        });
    }

    createThroughputWatcher(intervalMs) {
        let throughput = 0;
        let totalSize = 0;
        let prevTimeStamp = 0;
        let resetTimerHandler = null;

        return {
            calculate (payload) {
                clearTimeout(resetTimerHandler);

                totalSize += payload.length;

                const timeStamp = new Date().getTime();

                if ((timeStamp - prevTimeStamp) > intervalMs) {
                    if (prevTimeStamp !== 0) {
                        throughput = Math.floor(totalSize / ((timeStamp - prevTimeStamp) / intervalMs));
                    }
                    totalSize = 0;
                    prevTimeStamp = timeStamp;
                }

                resetTimerHandler = setTimeout(() => throughput = 0, intervalMs);
            },

            getThroughput () {
                return throughput;
            }
        }
    }
}


module.exports = Kafka;