const KafkaConfig = require(`../../config`).kafka;
const EventEmitter = require(`events`);
const Utils = require(`../../utils`);
const { Kafka } = require(`kafkajs`);
const { v4: uuid } = require(`uuid`);
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
class InternalKafka extends EventEmitter {
    /**
     * @return {string}
     */
    static get MESSAGE_EVENT() {
        return `message`;
    }
    /**
     * @return {string}
     */
    static get AVAILABLE_EVENT() {
        return `available`;
    }
    /**
     * @return {string}
     */
    static get NOT_AVAILABLE_EVENT() {
        return `notAvailable`;
    }
    /**
     * @return {string}
     */
    static get INTERNAL_TOPIC_PREFIX() {
        return `__`;
    }

    /**
     * Generate subscription group identification key
     * @param {string} subscriber
     * @param {string} group
     * @param {string} topic
     * @return {string}
     * @private
     */
    static _generateSubscriptionGroupKey(subscriber, group, topic) {
        return `${subscriber}:${group}:${topic}`;
    }

    /**
     * Creates new Kafka
     * @constructor
     */
    constructor() {
        super();

        this.clientUUID = uuid();
        this.isProducerReady = false;
        this.isConsumerReady = false;
        this.isAdminReady = false;
        this.available = true;
        this.subscriptionMap = new Map();
        this.subscriptionGroupMap = new Map();
        this.groupConsumersMap = new Map();

        this.kafka = new Kafka({
            clientId: this.clientUUID,
            brokers: KafkaConfig.KAFKA_HOSTS.split(","),
        });

        this.admin = this.kafka.admin();
        this.producer = this.kafka.producer({});
        this.consumer = this.kafka.consumer({
            groupId: `${KafkaConfig.CONSUMER_GROUP_ID_PREFIX}-${this.clientUUID}`,
            maxWaitTimeInMs: KafkaConfig.CONSUMER_MAX_WAIT_TIME,
            maxBytes: KafkaConfig.CONSUMER_MAX_BYTES,
        });

        debug(`Started trying connect to server`);

        this.admin
            .connect()
            .then(() => {
                this.isAdminReady = true;
                debug(`Admin is ready`);
                this.emit(`adminReady`);
            })
            .catch((error) => {
                debug(`Admin error: ${error}`);
                this.isAdminReady = false;
            });

        this.producer
            .connect()
            .then(() => {
                this.isProducerReady = true;
                debug(`Producer is ready`);
                this.emit(`producerReady`);
            })
            .catch((error) => {
                debug(`Producer error: ${error}`);
                this.isProducerReady = false;
            });

        this.consumer
            .connect()
            .then(async () => {
                this.isConsumerReady = true;
                debug(`Consumer is ready`);
                this.emit(`consumerReady`);
                this.emit(InternalKafka.AVAILABLE_EVENT);

                await this.consumer.run({
                    eachMessage: async ({ topic, partition, message }) =>
                        this._onMessage(topic, partition, message),
                });
            })
            .catch((error) => {
                debug(`Consumer error: ${error}`);
                this.isConsumerReady = false;
            });
    }

    /**
     * Returns ready producer
     * @return {Object}
     */
    getAdmin() {
        return this.isAdminReady
            ? Promise.resolve(this.admin)
            : new Promise((resolve) =>
                  this.on(`adminReady`, () => resolve(this.admin))
              );
    }

    /**
     * Returns ready producer
     * @return {Object}
     */
    getProducer() {
        return this.isProducerReady
            ? Promise.resolve(this.producer)
            : new Promise((resolve) =>
                  this.on(`producerReady`, () => resolve(this.producer))
              );
    }

    /**
     * Returns ready consumer
     * @return {Object}
     */
    getConsumer() {
        return this.isConsumerReady
            ? Promise.resolve(this.consumer)
            : new Promise((resolve) =>
                  this.on(`consumerReady`, () => resolve(this.consumer))
              );
    }

    /**
     * Creates Kafka topics by topicsList
     * @param {Array<string>} topicsList
     * @return {Promise<Array<string>>}
     */
    async createTopics(topicsList) {
        const admin = await this.getAdmin();

        try {
            await admin.createTopics({
                topics: topicsList.map((t) => ({ topic: t })),
            });
        } catch (error) {
            throw error;
        }

        return topicsList;
    }

    /**
     * Returns list of all existing topics
     * @return {Promise<Array<string>>}
     */
    async listTopics() {
        const admin = await this.getAdmin();

        return admin.listTopics();
    }

    /**
     * Subscribes consumer to each topic of topicsList and adds subscriberId to each subscription
     * @param {string} subscriberId
     * @param {string} subscriptionGroup
     * @param {Array<string>} topicsList
     * @return {Promise<Array<string>>}
     */
    async subscribe(subscriberId, subscriptionGroup, topicsList) {
        const topicsToSubscribe = new Set();
        const topicsToUnsubscribe = new Set();

        if (topicsList && topicsList.length === 0) {
            return topicsList;
        }

        await this.createTopics(topicsList);

        topicsList.forEach((topic) => {
            const subscribersSet = this.subscriptionMap.get(topic) || new Set();
            const groupSubscribersMap =
                this.subscriptionGroupMap.get(topic) || new Map();
            const groupSubscribersSet =
                groupSubscribersMap.get(subscriptionGroup) || new Set();

            if (
                Utils.isDefined(subscriptionGroup) &&
                !groupSubscribersSet.has(subscriberId)
            ) {
                topicsToSubscribe.add(topic);

                if (subscribersSet.has(subscriberId)) {
                    topicsToUnsubscribe.add(topic);
                }

                groupSubscribersMap.forEach((groupSubscribersSet) => {
                    if (groupSubscribersSet.has(subscriberId)) {
                        topicsToUnsubscribe.add(topic);
                    }
                });
            } else if (
                !Utils.isDefined(subscriptionGroup) &&
                !subscribersSet.has(topic)
            ) {
                topicsToSubscribe.add(topic);

                groupSubscribersMap.forEach((groupSubscribersSet) => {
                    if (groupSubscribersSet.has(subscriberId)) {
                        topicsToUnsubscribe.add(topic);
                    }
                });
            }
        });

        await this.unsubscribe(subscriberId, Array.from(topicsToUnsubscribe));

        const consumer = await this.getConsumer();

        if (Utils.isDefined(subscriptionGroup)) {
            await Promise.all(
                Array.from(topicsToSubscribe).map(async (topic) => {
                    const groupConsumer = this.kafka.consumer({
                        groupId: `${KafkaConfig.CONSUMER_GROUP_ID_PREFIX}-${subscriptionGroup}`,
                        // maxWaitTimeInMs: KafkaConfig.CONSUMER_MAX_WAIT_TIME,
                        // maxBytes: KafkaConfig.CONSUMER_MAX_BYTES
                    });

                    this.groupConsumersMap.set(
                        InternalKafka._generateSubscriptionGroupKey(
                            subscriberId,
                            subscriptionGroup,
                            topic
                        ),
                        groupConsumer
                    );

                    await groupConsumer.connect();
                    await groupConsumer.subscribe({ topic });

                    await groupConsumer.run({
                        eachMessage: async ({ topic, partition, message }) =>
                            this._onMessage(
                                topic,
                                partition,
                                message,
                                subscriptionGroup,
                                subscriberId
                            ),
                    });
                })
            );
        } else {
            await consumer.stop();
            await consumer.subscribe({ topics: Array.from(topicsToSubscribe) });
            await consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    this._onMessage(topic, partition, message);
                },
            });
        }

        Array.from(topicsToSubscribe).forEach((topic) => {
            if (Utils.isDefined(subscriptionGroup)) {
                const groupSubscribersMap =
                    this.subscriptionGroupMap.get(topic) || new Map();
                const groupSubscribersSet =
                    groupSubscribersMap.get(subscriptionGroup) || new Set();

                groupSubscribersSet.add(subscriberId);
                groupSubscribersMap.set(subscriptionGroup, groupSubscribersSet);
                this.subscriptionGroupMap.set(topic, groupSubscribersMap);
            } else {
                const subscribersSet =
                    this.subscriptionMap.get(topic) || new Set();

                subscribersSet.add(subscriberId);
                this.subscriptionMap.set(topic, subscribersSet);
            }
        });

        debug(
            `Subscriber with id: ${subscriberId} has subscribed to the next topics: ${topicsList}`
        );

        return topicsList;
    }

    /**
     * Unsubscribes consumer from each topic of topicsList and removes subscriberId from each subscription
     * @param {string} subscriberId
     * @param {Array<string>} topicsList
     * @return {Promise<Array<string>>}
     */
    async unsubscribe(subscriberId, topicsList) {
        const topicsToUnsubscribe = [];
        // const consumer = await this.getConsumer()

        topicsList.forEach((topic) => {
            const subscribersSet = this.subscriptionMap.get(topic) || new Set();
            const groupSubscribersMap =
                this.subscriptionGroupMap.get(topic) || new Map();

            if (subscribersSet.has(subscriberId)) {
                subscribersSet.delete(subscriberId);

                if (subscribersSet.size === 0) {
                    topicsToUnsubscribe.push(topic);
                }
            }

            groupSubscribersMap.forEach((groupSubscribersSet, group) => {
                if (groupSubscribersSet.has(subscriberId)) {
                    const groupConsumerKey =
                        InternalKafka._generateSubscriptionGroupKey(
                            subscriberId,
                            group,
                            topic
                        );
                    const groupConsumer =
                        this.groupConsumersMap.get(groupConsumerKey);

                    groupConsumer.disconnect();
                    this.groupConsumersMap.delete(groupConsumerKey);
                    groupSubscribersSet.delete(subscriberId);
                }
            });
        });

        // await consumer.pause(topicsToUnsubscribe.map(topicName => ({ topic: topicName })));

        topicsToUnsubscribe.forEach((topic) => {
            this.subscriptionMap.delete(topic);
        });

        debug(
            `Subscriber with id: ${subscriberId} has unsubscribed from the next topics: ${topicsList}`
        );

        return topicsList;
    }

    /**
     * Sends payload to Kafka over Kafka Producer
     * @param {string} payload
     * @return {Promise}
     */
    async send(payload) {
        const producer = await this.getProducer();

        return producer.send({
            topic: payload.topic,
            messages: [{ value: payload.message.value }],
        });
    }

    /**
     * Removes subscriberId from each Consumer subscription
     * @param {string} subscriberId
     */
    removeSubscriber(subscriberId) {
        const topicsToUnsubscribeSet = new Set();

        this.subscriptionMap.forEach((subscribersSet, topic) => {
            if (subscribersSet.has(subscriberId)) {
                topicsToUnsubscribeSet.add(topic);
            }
        });

        this.subscriptionGroupMap.forEach((groupSubscribersMap, topic) => {
            groupSubscribersMap.forEach((subscribersSet) => {
                if (subscribersSet.has(subscriberId)) {
                    topicsToUnsubscribeSet.add(topic);
                }
            });
        });

        if (topicsToUnsubscribeSet.size > 0) {
            this.unsubscribe(
                subscriberId,
                Array.from(topicsToUnsubscribeSet)
            ).catch((err) => debug(`Unsubscription error: ${err}`));
        }
    }

    /**
     * Checks if Kafka is available
     * @return {boolean}
     */
    isAvailable() {
        return this.isConsumerReady;
    }

    /**
     * Message handler
     * Emits next events:
     *      - message
     * @param {string} topic
     * @param {number} partition
     * @param {string} message
     * @param {string} subscriptionGroup
     * @param {string} subscriber
     * @private
     */
    _onMessage(topic, partition, message, subscriptionGroup, subscriber) {
        if (Utils.isDefined(subscriptionGroup)) {
            this.emit(
                InternalKafka.MESSAGE_EVENT,
                subscriber,
                topic,
                message.value,
                partition
            );
        } else {
            const subscriptionSet = this.subscriptionMap.get(topic);

            if (subscriptionSet) {
                subscriptionSet.forEach((subscriberId) => {
                    this.emit(
                        InternalKafka.MESSAGE_EVENT,
                        subscriberId,
                        topic,
                        message.value,
                        partition
                    );
                });
            }
        }
    }
}

module.exports = InternalKafka;
