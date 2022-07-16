const EventEmitter = require("events");
const Kafka = require("kafkajs").Kafka;
const Worker = require("node:worker_threads").Worker;
const Mutex = require("async-mutex").Mutex;
const FranzConfig = require(`../../config`).franz;
const path = require('node:path');
const { v4: uuid } = require('uuid');


class FranzWorker extends EventEmitter {
    constructor({ clientId, brokers }) {
        super();

        this.clientId = clientId;
        this.brokers = brokers;
        this._isWorkerReady = false;
        this._worker = null;
    }

    createWorker() {}
    postMessage(message) {
        this._worker.postMessage(message);
    }
    async waitForReady() {
        if (this._isWorkerReady) {
            return true;
        } else {
            return new Promise(resolve => this.once('ready', () => resolve()));
        }
    }
    async terminate() {
        this._worker.postMessage('terminate');
        this.removeAllListeners();
    }
}


class FranzConsumer extends FranzWorker {
    constructor(topic, groupId, config) {
        super(config);

        this.topic = topic;
        this.groupId = groupId;

        this._worker = this.createWorker();
    }

    createWorker() {
        const worker = new Worker(path.join(__dirname, './workers/consumer.mjs'), {
            workerData: {
                id: uuid(),
                topic: this.topic,
                groupId: this.groupId,
                clientId: this.clientId,
                brokers: this.brokers
            }
        });

        worker.on('message', (message) => {
            if (message === 'ready') {
                this._isWorkerReady = true;
                this.emit('ready');
            } else {
                this.emit('message', message);
            }
        });
        worker.on('error', (error) => this.emit('error', error));
        worker.on('exit', (code) => this.emit('exit', code));

        return worker;
    }
}


class FranzProducer extends FranzWorker {
    constructor(config) {
        super(config);

        this._worker = this.createWorker();
    }

    createWorker() {
        const worker = new Worker(path.join(__dirname, './workers/producer.mjs'), {
            workerData: {
                clientId: this.clientId,
                brokers: this.brokers
            }
        });

        worker.on('error', (error) => this.emit('error', error));
        worker.on('exit', (code) => this.emit('exit', code));
        worker.on('message', (message) => {
            if (message === 'ready') {
                this._isWorkerReady = true;
                this.emit('ready');
            }
        });

        return worker;
    }

    send(topic, message) {
        this.postMessage({topic, message});
    }
}


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


class Franz extends EventEmitter {
    static get MESSAGE_EVENT() { return `message`; }
    static get AVAILABLE_EVENT() { return `available`; }
    static get NOT_AVAILABLE_EVENT() { return `notAvailable`; }

    constructor() {
        super();

        this._consumersMap = new Map();
        this._producersSet = new Set();
        this._producersMutex = new Mutex();
        this._consumersMutex = new Mutex();
        this._subscribtionMutex = new Mutex();
        this._isAdminReady = false;
        this._config = {clientId: FranzConfig.KAFKA_CLIENT_ID, brokers: FranzConfig.KAFKA_HOSTS.split(',')};
        this._kafka = new Kafka(this._config);
        this._admin = this._kafka.admin();

        this._admin.connect()
            .then(() => {
                this._isAdminReady = true;
                this.emit(`adminReady`);
                this.emit(Franz.AVAILABLE_EVENT);
            })
            .catch((error) => {
                this._isAdminReady = false
            });
    }

    _getAdmin() {
        return this._isAdminReady ?
            Promise.resolve(this._admin) :
            new Promise((resolve) => this.on(`adminReady`, () => resolve(this._admin)));
    }

    async _createProducer() {
        return await underMutex( this._producersMutex, async () => {
            let [producer] = this._producersSet;

            if (!producer) {
                producer = new FranzProducer(this._config);

                await producer.waitForReady();

                this._producersSet.add(producer);
            }

            return producer;
        });
    }

    async _createConsumer(subscriberId, groupId, topic) {
        return await underMutex( this._consumersMutex, async () => {
            let topicConsumerMap = this._consumersMap.get(subscriberId);

            if (!topicConsumerMap) {
                topicConsumerMap = new Map();

                this._consumersMap.set(subscriberId, topicConsumerMap);
            }

            const consumer = new FranzConsumer(topic, groupId, this._config);

            consumer.on('message', message =>
                this.emit('message', subscriberId, topic, message));

            await consumer.waitForReady();


            topicConsumerMap.set(topic, consumer);

            return consumer;
        });
    }

    async _deleteConsumer(subscriberId, topic) {
        return underMutex( this._consumersMutex, async () => {
            let topicConsumerMap = this._consumersMap.get(subscriberId);

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

    async _deleteAllConsumer(subscriberId) {
        return underMutex( this._consumersMutex, async () => {
            let topicConsumerMap = this._consumersMap.get(subscriberId);

            if (!topicConsumerMap) {
                return subscriberId;
            }

            for(let consumer of topicConsumerMap.values()) {
                consumer.terminate();
            }

            topicConsumerMap.clear();

            this._consumersMap.delete(subscriberId);

            return subscriberId;
        });
    }

    async listTopics() {
        const admin = await this._getAdmin();

        return await admin.listTopics();
    }

    async createTopics(topics=[]) {
        const admin = await this._getAdmin();
        const topicsToCreate = topics.map(topic => ({ topic }));

        if (topicsToCreate) {
            await admin.createTopics({ topics: topicsToCreate });
        }

        return topics;
    }

    async subscribe(subscriberId, groupId, topicList=[]) {
        return await underMutex(this._subscribtionMutex, async () => {
            await Promise.all(topicList.map(topic =>
                this._createConsumer(subscriberId, groupId ? `${topic}_${groupId}` : uuid(), topic)));

            return topicList;
        });
    }

    async unsubscribe(subscriberId, topicList) {
        return await underMutex(this._subscribtionMutex, async () => {
            await Promise.all(topicList.map(topic => this._deleteConsumer(subscriberId, topic)));

            return topicList;
        });
    }

    async removeSubscriber(subscriberId) {
        return await underMutex(this._subscribtionMutex, async () => {
            await this._deleteAllConsumer(subscriberId);

            return subscriberId;
        });
    }

    async send({topic, message, partition}) {
        let [producer] = this._producersSet;

        if (!producer) {
            producer = await this._createProducer();
        }

        producer.send(topic, message.value);
    }

    async isAvailable() {
        return true;
    }
}


module.exports = { Franz };
