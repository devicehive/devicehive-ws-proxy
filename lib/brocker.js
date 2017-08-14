const kafka = require(`./kafka/kafka-extension`);

function getBrocker(type){
  switch (type){
  case `kafka`:
    return kafka;
  }
}

module.exports = {
  getBrocker
};