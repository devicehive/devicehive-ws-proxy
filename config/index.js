const path = require(`path`);
const configurator = require(`json-evn-configurator`);


module.exports = {
    proxy: configurator(path.join(__dirname, `../src/config.json`), `PROXY`),
    messageBuffer: configurator(path.join(__dirname, `../src/messageBuffer/config.json`), `MESSAGE_BUFFER`),
    kafka: configurator(path.join(__dirname, `../src/kafka/config.json`), `KAFKA`)
};