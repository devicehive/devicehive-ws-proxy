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
    static get KAFKA_COMMUNICATOR() {
        return `kafka`;
    }
    /**
     * @return {string}
     */
    static get FRANZ_COMMUNICATOR() {
        return `franz`;
    }
    /**
     * @return {string}
     */
    static get DEFAULT_COMMUNICATOR() {
        return InternalCommunicatorFacade.FRANZ_COMMUNICATOR;
    }

    /**
     * @param {string} communicatorType
     * @return {Object}
     */
    static createCommunicator(communicatorType) {
        let communicator;

        debug(`${communicatorType} used as internal communicator`);

        switch (communicatorType) {
            case InternalCommunicatorFacade.KAFKA_COMMUNICATOR:
                communicator = new Kafka();
                break;
            case InternalCommunicatorFacade.FRANZ_COMMUNICATOR:
                communicator = new Franz({});
                break;
            default:
                debug(
                    `${communicatorType} communicator is not supported. Will be used default communicator`
                );

                communicatorType =
                    InternalCommunicatorFacade.DEFAULT_COMMUNICATOR;
                communicator = new InternalCommunicatorFacade(communicatorType);
                break;
        }

        return communicator;
    }

    /**
     * Creates new InternalCommunicatorFacade
     * @param {string} communicatorType
     * @constructor
     */
    constructor(communicatorType) {
        super();

        this.communicator =
            InternalCommunicatorFacade.createCommunicator(communicatorType);

        this.communicator.on(
            InternalCommunicatorFacade.MESSAGE_EVENT,
            (subscriberId, topic, payload) => {
                this.emit(
                    InternalCommunicatorFacade.MESSAGE_EVENT,
                    subscriberId,
                    topic,
                    payload
                );
            }
        );

        this.communicator.on(InternalCommunicatorFacade.AVAILABLE_EVENT, () => {
            this.emit(InternalCommunicatorFacade.AVAILABLE_EVENT);
        });

        this.communicator.on(
            InternalCommunicatorFacade.NOT_AVAILABLE_EVENT,
            () => {
                this.emit(InternalCommunicatorFacade.NOT_AVAILABLE_EVENT);
            }
        );
    }

    /**
     * Creates new topic
     * @param {Array} topicsList
     * @return {Promise}
     */
    createTopics(topicsList) {
        return this.communicator.createTopics(topicsList);
    }

    /**
     * Returns list of existing topics
     * @return {Array<string>}
     */
    listTopics() {
        return this.communicator.listTopics();
    }

    /**
     * Subscribes subscriberId to each topic in topicsList
     * @param {string} subscriberId
     * @param {string} subscriptionGroup
     * @param {Array<string>} topicsList
     * @return {Promise}
     */
    subscribe(subscriberId, subscriptionGroup, topicsList) {
        return this.communicator.subscribe(
            subscriberId,
            subscriptionGroup,
            topicsList
        );
    }

    /**
     * Unsubscribes subscriberId from each topic in topicsList
     * @param {string} subscriberId
     * @param {Array<string>} topicsList
     * @return {Promise}
     */
    unsubscribe(subscriberId, topicsList) {
        return this.communicator.unsubscribe(subscriberId, topicsList);
    }

    /**
     * Sends payload to communicator
     * @param {string} payload
     * @return {Promise}
     */
    send(payload) {
        return this.communicator.send(payload);
    }

    /**
     * Removes subscriberId from communicator
     * @param {string} subscriberId
     * @return {Promise}
     */
    removeSubscriber(subscriberId) {
        return this.communicator.removeSubscriber(subscriberId);
    }

    /**
     * Checks if the communicator is available
     * @return {boolean}
     */
    isAvailable() {
        return this.communicator.isAvailable();
    }
}

module.exports = InternalCommunicatorFacade;
