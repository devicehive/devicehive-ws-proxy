const path = require(`path`);
const configurator = require(`json-evn-configurator`);


module.exports = {
    proxy: configurator(path.join(__dirname, `../src/config.json`), `PROXY`),
    kafka: configurator(path.join(__dirname, `../src/kafka/config.json`), `KAFKA`)
};