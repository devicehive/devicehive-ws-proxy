const Config = require(`./config.json`);
const Utils = require(`../../utils`);


/**
 * Kafka configuration class
 */
class KafkaConfig {

    /**
     * Creates new KafkaConfig
     * Corresponding config.json file lies in the same directory
     */
    constructor() {
        const me = this;

        me.KAFKA_HOSTS = Utils.value(process.env[`KAFKA.KAFKA_HOSTS`], Config.KAFKA_HOSTS);
        me.KAFKA_CLIENT_ID = Utils.value(process.env[`KAFKA.KAFKA_CLIENT_ID`], Config.KAFKA_CLIENT_ID);
        me.CONSUMER_GROUP_ID = Utils.value(process.env[`KAFKA.CONSUMER_GROUP_ID`], Config.CONSUMER_GROUP_ID);
        me.LOGGER_LEVEL = Utils.value(process.env[`KAFKA.LOGGER_LEVEL`], Config.LOGGER_LEVEL);
    }
}


module.exports = new KafkaConfig();