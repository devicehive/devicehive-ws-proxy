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

		me.kafka.on(`message`,
			(subscriberId, topic, message, partition) => me.emit(`message`, subscriberId, topic, message, partition));
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

		return me.kafka.send(payload);
	}

	removeSubscriber(subscriberId) {
		const me = this;

		return me.kafka.removeSubscriber(subscriberId);
	}

	isAvailable() {
		const me = this;

		return me.kafka.isAvailable();
	}
}


module.exports = new InternalCommunicatorFacade();