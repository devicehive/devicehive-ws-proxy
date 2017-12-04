const EventEmitter = require(`events`);
const Kafka = require(`./kafka/Kafka`);


/**
 *
 */
class InternalCommunicatorFacade extends EventEmitter {

	constructor() {
		super();

		const me = this;

		me.kafka = new Kafka();
	}

	createTopics(topicsList) {
		const me = this;

		return me.kafka.createClientTopics(topicsList);
	}

	listTopics() {
		const me = this;

		return me.kafka.listTopics();
	}

	subscribe(subscriberId, topicsList) {
		const me = this;

		return me.kafka.subscribe(subscriberId, topicsList);
	}

	unsubscribe(subscriberId, topicsList) {
		const me = this;

		return me.kafka.unsubscribe(subscriberId, topicsList);
	}

	send(payload) {
		const me = this;

		return me.kafka.send(payload);
	}
}


module.exports = new InternalCommunicatorFacade();