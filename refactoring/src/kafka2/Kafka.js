const EventEmitter = require(`events`);
const Utils = require(`../../utils`);
const Config = require(`./config.json`);
const NoKafka = require(`no-kafka`);
const uuid = require(`uuid/v1`);
const stringHash = require("string-hash");
const debug = require(`debug`)(`kafka`);


/**
 *
 */
class Kafka extends EventEmitter {

	constructor() {
		super();

        const me = this;

        me.isProducerReady = false;
        me.isConsumerReady = false;
        me.subscriptionMap = new Map();

        me.producer = new NoKafka.Producer(); // TODO options

        me.producer
			.init()
			.then(() => {
                me.isProducerReady = true;
                me.emit(`producerReady`);
			})
			.catch(() => {
                me.isProducerReady = false;
			});


        me.consumer = new NoKafka.GroupConsumer(); // TODO options

        me.consumer
			.init() //TODO strategies
            .then(() => {
                me.isConsumerReady = true;
                me.emit(`consumerReady`);
            })
            .catch(() => {
                me.isConsumerReady = false;
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

		return new Promise((resolve, reject) => {

		});
	}

	listTopics() {
		const me = this;

		return new Promise((resolve, reject) => {
            me.getProducer()
                .then((producer) => {
                    producer.client.metadataRequest()
						.then((metadata) => {
                    		debugger;
                    		const result = [];

                            metadata.topicMetadata.forEach(() => {

							})
						})
						.catch((error) => {
                            debugger;
						})
                });
		});
	}

	subscribe(subscriberId, topicsList) {
		const me = this;

        return new Promise(() => {

        });
	}

	unsubscribe(subscriberId, topicsList) {
		const me = this;

		return new Promise(() => {

        });
	}

	send(payload) {
		const me = this;

		return new Promise((resolve, reject) => {
			me.getProducer()
				.then((producer) => {

				});
		});
	}
}


module.exports = Kafka;