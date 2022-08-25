const EventEmitter = require("events");
const Kafka = require("kafkajs").Kafka;
const Worker = require("node:worker_threads").Worker;
const Mutex = require("async-mutex").Mutex;
const FranzConfig = require(`../../config`).franz;
const path = require("node:path");
const { v4: uuid } = require("uuid");

/**
 * Base class for Franz worker
 */
class FranzWorker extends EventEmitter {
    /**
     * @param {string} clientId
     * @param {Array<string>} brokers
     * @constructor
     */
    constructor({ clientId, brokers }) {
        super();

        this.clientId = clientId;
        this.brokers = brokers;
        this._isWorkerReady = false;
        this._worker = null;
    }

    /**
     * @abstract
     */
    createWorker() {}

    /**
     * @param {string} message
     */
    postMessage(message) {
        this._worker.postMessage(message);
    }

    /**
     * @return {Promise}
     */
    async waitForReady() {
        if (this._isWorkerReady) {
            return true;
        } else {
            return new Promise((resolve) =>
                this.once("ready", () => resolve())
            );
        }
    }

    /**
     *
     * @return {Promise}
     */
    async terminate() {
        this._worker.postMessage("terminate");
        this.removeAllListeners();
    }
}

/**
 * Consumer worker for Franz communicator
 */
class FranzConsumer extends FranzWorker {
    /**
     *
     * @param {string} topic
     * @param {string} groupId
     * @param {object} config
     * @constructor
     */
    constructor(topic, groupId, config) {
        super(config);

        this.topic = topic;
        this.groupId = groupId;

        this._worker = this.createWorker();
    }

    /**
     * @return {module:worker_threads.Worker}
     */
    createWorker() {
        const worker = new Worker(
            path.join(__dirname, "./workers/consumer.mjs"),
            {
                workerData: {
                    id: uuid(),
                    topic: this.topic,
                    groupId: this.groupId,
                    clientId: this.clientId,
                    brokers: this.brokers,
                },
            }
        );

        worker.on("message", (message) => {
            if (message === "ready") {
                this._isWorkerReady = true;
                this.emit("ready");
            } else {
                this.emit("message", message);
            }
        });
        worker.on("error", (error) => this.emit("error", error));
        worker.on("exit", (code) => this.emit("exit", code));

        return worker;
    }
}

/**
 * Producer worker for Franz communicator
 */
class FranzProducer extends FranzWorker {
    /**
     * @param {object} config
     * @constructor
     */
    constructor(config) {
        super(config);

        this._worker = this.createWorker();
    }

    /**
     * @return {module:worker_threads.Worker}
     */
    createWorker() {
        const worker = new Worker(
            path.join(__dirname, "./workers/producer.mjs"),
            {
                workerData: {
                    clientId: this.clientId,
                    brokers: this.brokers,
                },
            }
        );

        worker.on("error", (error) => this.emit("error", error));
        worker.on("exit", (code) => this.emit("exit", code));
        worker.on("message", (message) => {
            if (message === "ready") {
                this._isWorkerReady = true;
                this.emit("ready");
            }
        });

        return worker;
    }

    /**
     *
     * @param {string} topic
     * @param {string} message
     */
    send(topic, message) {
        this.postMessage({ topic, message });
    }
}

/**
 * @param {Mutex} mutex
 * @param {Function} fn
 * @return {Promise}
 */
async function underMutex(mutex, fn) {
    const resolve = await mutex.acquire();

    try {
        return await fn();
    } catch (error) {
        throw error;
    } finally {
        resolve();
    }
}

/**
 * Implementation of Kafka communicator based on workers threads
 */
class Franz extends EventEmitter {
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
     * @constructor
     */
    constructor() {
        super();

        this._consumersMap = new Map();
        this._producersSet = new Set();
        this._producersMutex = new Mutex();
        this._consumersMutex = new Mutex();
        this._subscribtionMutex = new Mutex();
        this._isAdminReady = false;
        this._config = {
            clientId: FranzConfig.KAFKA_CLIENT_ID,
            brokers: FranzConfig.KAFKA_HOSTS.split(","),
        };
        this._kafka = new Kafka(this._config);
        this._admin = this._kafka.admin();

        this._admin
            .connect()
            .then(() => {
                this._isAdminReady = true;
                this.emit(`adminReady`);
                this.emit(Franz.AVAILABLE_EVENT);
            })
            .catch(() => {
                this._isAdminReady = false;
            });
    }

    /**
     * @return {Promise}
     * @private
     */
    _getAdmin() {
        return this._isAdminReady
            ? Promise.resolve(this._admin)
            : new Promise((resolve) =>
                  this.on(`adminReady`, () => resolve(this._admin))
              );
    }

    /**
     * @return {Promise<FranzProducer>}
     * @private
     */
    async _createProducer() {
        return await underMutex(this._producersMutex, async () => {
            let [producer] = this._producersSet;

            if (!producer) {
                producer = new FranzProducer(this._config);

                await producer.waitForReady();

                this._producersSet.add(producer);
            }

            return producer;
        });
    }

    /**
     * @param {string} subscriberId
     * @param {string} groupId
     * @param {string} topic
     * @return {Promise<FranzConsumer>}
     * @private
     */
    async _createConsumer(subscriberId, groupId, topic) {
        return await underMutex(this._consumersMutex, async () => {
            let topicConsumerMap = this._consumersMap.get(subscriberId);

            if (!topicConsumerMap) {
                topicConsumerMap = new Map();

                this._consumersMap.set(subscriberId, topicConsumerMap);
            }

            const consumer = new FranzConsumer(topic, groupId, this._config);

            consumer.on("message", (message) =>
                this.emit("message", subscriberId, topic, message)
            );

            await consumer.waitForReady();

            topicConsumerMap.set(topic, consumer);

            return consumer;
        });
    }

    /**
     *
     * @param {string} subscriberId
     * @param {string} topic
     * @return {Promise}
     * @private
     */
    async _deleteConsumer(subscriberId, topic) {
        return underMutex(this._consumersMutex, async () => {
            const topicConsumerMap = this._consumersMap.get(subscriberId);

            if (!topicConsumerMap) {
                return subscriberId;
            }

            const consumer = topicConsumerMap.get(topic);

            if (!consumer) {
                return subscriberId;
            }

            consumer.terminate();
            topicConsumerMap.delete(topic);

            return subscriberId;
        });
    }

    /**
     *
     * @param {string} subscriberId
     * @return {Promise}
     * @private
     */
    async _deleteAllConsumer(subscriberId) {
        return underMutex(this._consumersMutex, async () => {
            const topicConsumerMap = this._consumersMap.get(subscriberId);

            if (!topicConsumerMap) {
                return subscriberId;
            }

            for (const consumer of topicConsumerMap.values()) {
                consumer.terminate();
            }

            topicConsumerMap.clear();

            this._consumersMap.delete(subscriberId);

            return subscriberId;
        });
    }

    /**
     *
     * @return {Promise<Array<string>>}
     */
    async listTopics() {
        const admin = await this._getAdmin();

        return await admin.listTopics();
    }

    /**
     *
     * @param {Array<string>} topics
     * @return {Promise<Array<string>>}
     */
    async createTopics(topics = []) {
        const admin = await this._getAdmin();
        const topicsToCreate = topics.map((topic) => ({ topic }));

        if (topicsToCreate) {
            await admin.createTopics({ topics: topicsToCreate });
        }

        return topics;
    }

    /**
     *
     * @param {string} subscriberId
     * @param {string} groupId
     * @param {Array<string>} topicList
     * @return {Promise<Array<string>>}
     */
    async subscribe(subscriberId, groupId, topicList = []) {
        return await underMutex(this._subscribtionMutex, async () => {
            await Promise.all(
                topicList.map((topic) =>
                    this._createConsumer(
                        subscriberId,
                        groupId ? `${topic}_${groupId}` : uuid(),
                        topic
                    )
                )
            );

            return topicList;
        });
    }

    /**
     *
     * @param {string} subscriberId
     * @param {Array<string>} topicList
     * @return {Promise<Array<string>>}
     */
    async unsubscribe(subscriberId, topicList) {
        return await underMutex(this._subscribtionMutex, async () => {
            await Promise.all(
                topicList.map((topic) =>
                    this._deleteConsumer(subscriberId, topic)
                )
            );

            return topicList;
        });
    }

    /**
     *
     * @param {string} subscriberId
     * @return {Promise<string>}
     */
    async removeSubscriber(subscriberId) {
        return await underMutex(this._subscribtionMutex, async () => {
            await this._deleteAllConsumer(subscriberId);

            return subscriberId;
        });
    }

    /**
     *
     * @param {string} topic
     * @param {string} message
     * @param {number} partition
     * @return {Promise}
     */
    async send({ topic, message, partition }) {
        let [producer] = this._producersSet;

        if (!producer) {
            producer = await this._createProducer();
        }

        producer.send(topic, message.value);
    }

    /**
     *
     * @return {Promise<boolean>}
     */
    async isAvailable() {
        return true;
    }
}

module.exports = { Franz };
