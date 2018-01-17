const EventEmitter = require(`events`);
const Kafka = require(`./kafka/Kafka`);
const debug = require(`debug`)(`internalcommunicatorfacade`);


/**
 * Communicator (Message Broker) Proxy facade
 * For now implements next communicators:
 * 		- kafka
 * @event "message"
 */
class InternalCommunicatorFacade extends EventEmitter {

    static get MESSAGE_EVENT() { return `message`; }
    static get AVAILABLE_EVENT() { return `available`; }
    static get NOT_AVAILABLE_EVENT() { return `notAvailable`; }

    static get KAFKA_COMMUNICATOR() { return `kafka`; }
    static get DEFAULT_COMMUNICATOR() { return InternalCommunicatorFacade.KAFKA_COMMUNICATOR; }

	static createCommunicator(communicatorType) {
		let communicator;

        debug(`${communicatorType} used as internal communicator`);

		switch(communicatorType) {
			case InternalCommunicatorFacade.KAFKA_COMMUNICATOR:
                communicator =  new Kafka();
				break;
			default:
                debug(`${communicatorType} communicator is not supported. Will be used default communicator`);

                communicatorType = InternalCommunicatorFacade.DEFAULT_COMMUNICATOR;
                communicator = InternalCommunicatorFacade(communicatorType);
				break;
		}

		return communicator;
	}

    /**
	 * Creates new InternalCommunicatorFacade
     */
	constructor(communicatorType) {
		super();

		const me = this;

		me.communicator = InternalCommunicatorFacade.createCommunicator(communicatorType);

		me.communicator.on(InternalCommunicatorFacade.MESSAGE_EVENT, (subscriberId, topic, payload) => {
			me.emit(InternalCommunicatorFacade.MESSAGE_EVENT, subscriberId, topic, payload);
        });

        me.communicator.on(InternalCommunicatorFacade.AVAILABLE_EVENT, () => {
            me.emit(InternalCommunicatorFacade.AVAILABLE_EVENT);
        });

        me.communicator.on(InternalCommunicatorFacade.NOT_AVAILABLE_EVENT, () => {
            me.emit(InternalCommunicatorFacade.NOT_AVAILABLE_EVENT);
        });
	}

    /**
	 * Creates new topic
     * @param topicsList
     * @returns {*}
     */
	createTopics(topicsList) {
		const me = this;

		return me.communicator.createTopics(topicsList);
	}

    /**
	 * Returns list of existing topics
     * @returns {*}
     */
	listTopics() {
		const me = this;

		return me.communicator.listTopics();
	}

    /**
	 * Subscribes subscriberId to each topic in topicsList
     * @param subscriberId
     * @param subscriptionGroup
     * @param topicsList
     * @returns {*|Promise|Promise<void>|Promise<PushSubscription>}
     */
	subscribe(subscriberId, subscriptionGroup, topicsList) {
		const me = this;

		return me.communicator.subscribe(subscriberId, subscriptionGroup, topicsList);
	}

    /**
	 * Unsubscribes subscriberId from each topic in topicsList
     * @param subscriberId
     * @param subscriptionGroup
     * @param topicsList
     * @returns {*|Promise|Promise<number[]>|Promise<boolean>}
     */
	unsubscribe(subscriberId, topicsList) {
		const me = this;

		return me.communicator.unsubscribe(subscriberId, topicsList);
	}

    /**
	 * Sends payload to communicator
     * @param payload
     */
	send(payload) {
		const me = this;

		return me.communicator.send(payload);
	}

    /**
	 * Removes subscriberId from communicator
     * @param subscriberId
     * @returns {*|void}
     */
	removeSubscriber(subscriberId) {
		const me = this;

		return me.communicator.removeSubscriber(subscriberId);
	}

    /**
	 * Checks if the communicator is available
     * @returns {*}
     */
	isAvailable() {
		const me = this;

		return me.communicator.isAvailable();
	}
}


module.exports = InternalCommunicatorFacade;