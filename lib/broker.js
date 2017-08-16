const kafka = require(`./kafka/kafka-extension`);

/**
 * Returns appropriate broker by type
 * 
 * @param {any} type 
 * @returns 
 */
function getBrocker(type){
  switch (type){
  case `kafka`:
    return kafka;
  }
}

module.exports = {
  getBrocker
};