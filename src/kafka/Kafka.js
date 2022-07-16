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
     * Creates new Kafka
     */
    constructor() {
        super();

        const me = this;

        me.clientUUID = uuid();
        me.isProducerReady = false;
        me.isConsumerReady = false;
        me.isAdminReady = false;
        me.available = true;
        me.subscriptionMap = new Map();
        me.subscriptionGroupMap = new Map();
        me.groupConsumersMap = new Map();

        me.kafka = new Kafka({
            clientId: me.clientUUID,
            brokers: KafkaConfig.KAFKA_HOSTS.split(','),
        })

        me.admin = me.kafka.admin()
        me.producer = me.kafka.producer({});
        me.consumer = me.kafka.consumer({
            groupId: `${KafkaConfig.CONSUMER_GROUP_ID_PREFIX}-${me.clientUUID}`,
            maxWaitTimeInMs: KafkaConfig.CONSUMER_MAX_WAIT_TIME,
            maxBytes: KafkaConfig.CONSUMER_MAX_BYTES
        });

        debug(`Started trying connect to server`);

        me.admin.connect()
            .then(() => {
                me.isAdminReady = true;
                debug(`Admin is ready`);
                me.emit(`adminReady`);
            })
            .catch((error) => {
                debug(`Admin error: ${error}`);
                me.isAdminReady = false;
            });

        me.producer.connect()
            .then(() => {
                me.isProducerReady = true;
                debug(`Producer is ready`);
                me.emit(`producerReady`);
            })
            .catch((error) => {
                debug(`Producer error: ${error}`);
                me.isProducerReady = false;
            });

        me.consumer.connect()
            .then(async () => {
                me.isConsumerReady = true;
                debug(`Consumer is ready`);
                me.emit(`consumerReady`);
                me.emit(InternalKafka.AVAILABLE_EVENT);

                await me.consumer.run({
                    eachMessage: async ({ topic, partition, message }) =>
                        this._onMessage(topic, partition, message),
                })
            })
            .catch((error) => {
                debug(`Consumer error: ${error}`);
                me.isConsumerReady = false;
            });

        me._metadata = null;
        me._topicArray = [];
        me._topicRequestSet = new Set();
        me._topicRequestEmitter = new EventEmitter();
    }

    /**
     * Returns ready producer
     * @returns {Admin}
     */
    getAdmin() {
        return this.isAdminReady ?
            Promise.resolve(this.admin) :
            new Promise((resolve) => this.on(`adminReady`, () => resolve(this.admin)));
    }

    /**
     * Returns ready producer
     * @returns {Producer}
     */
    getProducer() {
        return this.isProducerReady ?
            Promise.resolve(this.producer) :
            new Promise((resolve) => this.on(`producerReady`, () => resolve(this.producer)));
    }

    /**
     * Returns ready consumer
     * @returns {NoKafka.SimpleConsumer}
     */
    getConsumer() {
        return this.isConsumerReady ?
            Promise.resolve(this.consumer) :
            new Promise((resolve) => this.on(`consumerReady`, () => resolve(this.consumer)));
    }

    /**
     * Creates Kafka topics by topicsList
     * @param topicsList
     * @returns {Promise<Array>}
     */
    async createTopics(topicsList) {
        const admin = await this.getAdmin();

        await admin.createTopics({topics: topicsList.map(t => ({topic: t}))});

        return topicsList;
    }

    /**
     * Returns list of all existing topics
     * @returns {Promise<Array>}
     */
    async listTopics() {
        const admin = await this.getAdmin();

        return admin.listTopics();
    }

    /**
     * Subscribes consumer to each topic of topicsList and adds subscriberId to each subscription
     * @param subscriberId
     * @param subscriptionGroup
     * @param topicsList
     * @returns {Promise<Array>}
     */
    async subscribe(subscriberId, subscriptionGroup, topicsList) {
        const topicsToSubscribe = new Set();
        const topicsToUnsubscribe = new Set();

        if ( topicsList && topicsList.length === 0) {
            return topicsList
        }

        await this.createTopics(topicsList)

        topicsList.forEach((topic) => {
            const subscribersSet = this.subscriptionMap.get(topic) || new Set;
            const groupSubscribersMap = this.subscriptionGroupMap.get(topic) || new Map;
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

        await this.unsubscribe(subscriberId, Array.from(topicsToUnsubscribe));

        const consumer = await this.getConsumer()

        if (Utils.isDefined(subscriptionGroup)) {
            await Promise.all(Array.from(topicsToSubscribe).map(async topic => {
                const groupConsumer = this.kafka.consumer({
                    groupId: `${KafkaConfig.CONSUMER_GROUP_ID_PREFIX}-${subscriptionGroup}`,
                    // maxWaitTimeInMs: KafkaConfig.CONSUMER_MAX_WAIT_TIME,
                    // maxBytes: KafkaConfig.CONSUMER_MAX_BYTES
                });

                this.groupConsumersMap.set(
                    InternalKafka._generateSubscriptionGroupKey(subscriberId, subscriptionGroup, topic), groupConsumer);

                await groupConsumer.connect();
                await groupConsumer.subscribe({topic});

                await groupConsumer.run({
                    eachMessage: async ({ topic, partition, message }) =>
                        this._onMessage(topic, partition, message, subscriptionGroup, subscriberId),
                })
            }))
        } else {
            await consumer.stop();
            await Promise.all(Array.from(topicsToSubscribe).map(topic => consumer.subscribe({topic})))
            await consumer.run({
                eachMessage: async ({ topic, partition, message }) =>
                    this._onMessage(topic, partition, message),
            });
        }

        Array.from(topicsToSubscribe).forEach((topic) => {
            if (Utils.isDefined(subscriptionGroup)) {
                const groupSubscribersMap = this.subscriptionGroupMap.get(topic) || new Map;
                const groupSubscribersSet = groupSubscribersMap.get(subscriptionGroup) || new Set;

                groupSubscribersSet.add(subscriberId);
                groupSubscribersMap.set(subscriptionGroup, groupSubscribersSet);
                this.subscriptionGroupMap.set(topic, groupSubscribersMap);
            } else {
                const subscribersSet = this.subscriptionMap.get(topic) || new Set;

                subscribersSet.add(subscriberId);
                this.subscriptionMap.set(topic, subscribersSet);
            }
        });

        debug(`Subscriber with id: ${subscriberId} has subscribed to the next topics: ${topicsList}`);

        return topicsList;
    }

    /**
     * Unsubscribes consumer from each topic of topicsList and removes subscriberId from each subscription
     * @param subscriberId
     * @param topicsList
     * @returns {Bluebird<any>}
     */
    async unsubscribe(subscriberId, topicsList) {
        const topicsToUnsubscribe = [];
        // const consumer = await this.getConsumer()

        topicsList.forEach((topic) => {
            const subscribersSet = this.subscriptionMap.get(topic) || new Set;
            const groupSubscribersMap = this.subscriptionGroupMap.get(topic) || new Map;

            if (subscribersSet.has(subscriberId)) {
                subscribersSet.delete(subscriberId);

                if (subscribersSet.size === 0) {
                    topicsToUnsubscribe.push(topic);
                }
            }

            groupSubscribersMap.forEach((groupSubscribersSet, group) => {
                if (groupSubscribersSet.has(subscriberId)) {
                    const groupConsumerKey = InternalKafka._generateSubscriptionGroupKey(subscriberId, group, topic);
                    const groupConsumer = this.groupConsumersMap.get(groupConsumerKey);

                    groupConsumer.disconnect();
                    this.groupConsumersMap.delete(groupConsumerKey);
                    groupSubscribersSet.delete(subscriberId);
                }
            });
        });

        // await Promise.all(topicsToUnsubscribe.map(topicName => consumer.pause([{ topic: topicName }])));

        topicsToUnsubscribe.forEach((topic) => {
            this.subscriptionMap.delete(topic);
        });

        debug(`Subscriber with id: ${subscriberId} has unsubscribed from the next topics: ${topicsList}`);

        return topicsList;
    }

    /**
     * Sends payload to Kafka over Kafka Producer
     * @param payload
     * @returns {Promise<>}
     */
    async send(payload) {
        const producer = await this.getProducer();

        return producer.send({
            topic: payload.topic,
            messages: [
                { value: payload.message.value, partition: payload.partition }
            ]
        });
    }

    /**
     * Removes subscriberId from each Consumer subscription
     * @param subscriberId
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
            this.unsubscribe(subscriberId, Array.from(topicsToUnsubscribeSet));
        }
    }

    /**
     * Checks if Kafka is available
     * @returns {boolean}
     */
    isAvailable() {
        return this.isConsumerReady
    }

    /**
     * Message handler
     * Emits next events:
     *      - message
     * @param topic
     * @param partition
     * @param message
     * @param subscriptionGroup
     * @param subscriber
     * @private
     */
    _onMessage(topic, partition, message, subscriptionGroup, subscriber) {
        if (Utils.isDefined(subscriptionGroup)) {
            this.emit(InternalKafka.MESSAGE_EVENT, subscriber, topic, message.value, partition);
        } else {
            const subscriptionSet = this.subscriptionMap.get(topic);

            if (subscriptionSet) {
                subscriptionSet.forEach((subscriberId) => {
                    this.emit(InternalKafka.MESSAGE_EVENT, subscriberId, topic, message.value, partition);
                });
            }
        }
    }
}


module.exports = InternalKafka;
