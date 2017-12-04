const { ConsumerGroup } = require(`kafka-node`);
const debug = require(`debug`)(`kafkaconsumergroup`);


class KafkaConsumerGroup extends ConsumerGroup {

    constructor(client, config) {
        super (client, config);

        const me = this;

        me.client = client;
    }
}


module.exports = KafkaConsumerGroup;