const kafka = require(`./kafka/newKafkaExt`);

/**
 * Returns appropriate broker by type
 * 
 * @param {any} type 
 * @returns 
 */
function getBroker(type){
  switch (type){
  case `kafka`:
    return kafka;
  }
}

module.exports = {
  getBroker
};