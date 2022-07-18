const EventEmitter = require(`events`);
const Kafka = require(`./kafka/Kafka`);
const Franz = require("./franz/Franz.js").Franz;
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
    static get FRANZ_COMMUNICATOR() { return `franz`; }
    static get DEFAULT_COMMUNICATOR() { return InternalCommunicatorFacade.FRANZ_COMMUNICATOR; }

	static createCommunicator(communicatorType) {
		let communicator;

        debug(`${communicatorType} used as internal communicator`);

		switch(communicatorType) {
			case InternalCommunicatorFacade.KAFKA_COMMUNICATOR:
                communicator =  new Kafka();
				break;
			case InternalCommunicatorFacade.FRANZ_COMMUNICATOR:
				communicator =  new Franz({});
				break;
			default:
                debug(`${communicatorType} communicator is not supported. Will be used default communicator`);

                communicatorType = InternalCommunicatorFacade.DEFAULT_COMMUNICATOR;
                communicator = new InternalCommunicatorFacade(communicatorType);
				break;
		}

		return communicator;
	}

    /**
	 * Creates new InternalCommunicatorFacade
     */
	constructor(communicatorType) {
		super();

		this.communicator = InternalCommunicatorFacade.createCommunicator(communicatorType);

		this.communicator.on(InternalCommunicatorFacade.MESSAGE_EVENT, (subscriberId, topic, payload) => {
			this.emit(InternalCommunicatorFacade.MESSAGE_EVENT, subscriberId, topic, payload);
        });

        this.communicator.on(InternalCommunicatorFacade.AVAILABLE_EVENT, () => {
            this.emit(InternalCommunicatorFacade.AVAILABLE_EVENT);
        });

        this.communicator.on(InternalCommunicatorFacade.NOT_AVAILABLE_EVENT, () => {
            this.emit(InternalCommunicatorFacade.NOT_AVAILABLE_EVENT);
        });
	}

    /**
	 * Creates new topic
     * @param topicsList
     * @returns {*}
     */
	createTopics(topicsList) {
		return this.communicator.createTopics(topicsList);
	}

    /**
	 * Returns list of existing topics
     * @returns {*}
     */
	listTopics() {
		return this.communicator.listTopics();
	}

    /**
	 * Subscribes subscriberId to each topic in topicsList
     * @param subscriberId
     * @param subscriptionGroup
     * @param topicsList
     * @returns {*|Promise|Promise<void>|Promise<PushSubscription>}
     */
	subscribe(subscriberId, subscriptionGroup, topicsList) {
		return this.communicator.subscribe(subscriberId, subscriptionGroup, topicsList);
	}

    /**
	 * Unsubscribes subscriberId from each topic in topicsList
     * @param subscriberId
     * @param topicsList
     * @returns {*|Promise|Promise<number[]>|Promise<boolean>}
     */
	unsubscribe(subscriberId, topicsList) {
		return this.communicator.unsubscribe(subscriberId, topicsList);
	}

    /**
	 * Sends payload to communicator
     * @param payload
     */
	send(payload) {
		return this.communicator.send(payload);
	}

    /**
	 * Removes subscriberId from communicator
     * @param subscriberId
     * @returns {*|void}
     */
	removeSubscriber(subscriberId) {
		return this.communicator.removeSubscriber(subscriberId);
	}

    /**
	 * Checks if the communicator is available
     * @returns {*}
     */
	isAvailable() {
		return this.communicator.isAvailable();
	}
}


module.exports = InternalCommunicatorFacade;
