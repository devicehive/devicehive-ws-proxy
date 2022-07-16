const path = require(`path`);
const configurator = require(`json-evn-configurator`);


module.exports = {
    proxy: configurator(path.join(__dirname, `../src/config.json`), `PROXY`),
    pluginManager: configurator(path.join(__dirname, `../src/pluginManager/config.json`), `PLUGIN_MANAGER`),
    messageBuffer: configurator(path.join(__dirname, `../src/messageBuffer/config.json`), `MESSAGE_BUFFER`),
    kafka: configurator(path.join(__dirname, `../src/kafka/config.json`), `KAFKA`),
    franz: configurator(path.join(__dirname, `../src/franz/config.json`), `FRANZ`)
};
