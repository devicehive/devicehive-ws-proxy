const params = {
  kc : {
    a : 1
  },
  wsc : undefined, 
  prod_c : undefined, 
  consumer_c : undefined
}

class A {
  constructor(config){
    if (Object.keys(config).length === 1){
      ({ 
        kafka_config : this.kafka_config, 
        websocket_config : this.websocket_config,
        consumer_config : this.consumer_config,
        producer_config : this.producer_config
      } = config.kc);
    } else {
      ({
        kc : this.kafka_config,
        wsc : this.websocket_config,
        prod_c : this.producer_config,
        consumer_c : this.consumer_config
      } = config);
    }
  }
}

console.log(new A({
  kc : { a : 1 },
  wsc : { b : 2 },
  prod_c : { d : 4 },
  consumer_c : { c : 3 }
  
}))

console.log(new A({
  kc : {
    kafka_config : { a : 1 },
    websocket_config : { b : 2 },
    producer_config : { d : 4 },
    consumer_config : { c : 3 }
  }
}))