const { Producer } = require(`kafka-node`);
const debug = require(`debug`)(`kafkaproducer`);


class KafkaProducer extends Producer {

    constructor(client, config) {
        super (client, config);

        const me = this;

        me.client = client;
    }
}


module.exports = KafkaProducer;