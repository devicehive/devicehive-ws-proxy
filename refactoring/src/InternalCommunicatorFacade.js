const EventEmitter = require(`events`);
const Kafka2 = require(`./kafka2/Kafka`);


/**
 *
 */
class InternalCommunicatorFacade extends EventEmitter {

	constructor() {
		super();

		const me = this;

		me.kafka = new Kafka2();
	}

	createTopics(topicsList) {
		const me = this;

		return me.kafka.createTopics(topicsList);
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

		return me.kafka.sendData(payload);
	}
}


module.exports = new InternalCommunicatorFacade();