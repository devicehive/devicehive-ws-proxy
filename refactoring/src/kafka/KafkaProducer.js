const { Producer } = require(`kafka-node`);
const debug = require(`debug`)(`kafkaproducer`);


class KafkaProducer extends Producer {

    constructor(client) {
        super (client, {
	        requireAcks: 1,
	        ackTimeoutMs: 100,
	        partitionerType: 2
        });

        const me = this;

        me.client = client;
        me.isReady = false;

	    me.on(`ready`, () => {
		    me.isReady = true;
	    });

	    me.on(`error`, () => {
		    me.isReady = false;
	    })
    }
}


module.exports = KafkaProducer;