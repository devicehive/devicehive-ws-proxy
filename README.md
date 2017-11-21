[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# WebSocket proxy to Kafka
The project wrap some of the essential kafka functionality 
allowing you to communicate with Kafka through WebSockets.
WebSocket implements JSON based message protocol and have the following features:
 - Topics
    - Create
    - List
    - Subscribe (incl. consumer groups)
    - Un-subscribe
 - Push messages to topics
 
This project is based on [SOHU-Co/kafka-node](https://github.com/SOHU-Co/kafka-node) 
and [WebSockets](https://github.com/websockets/ws)


# Checkout and Build
```
git clone https://github.com/devicehive/devicehive-ws-kafka-proxy.git
cd devicehive-ws-kafka-proxy
docker-compose up -d --build
```

After running these commands, docker starts all images (Kafka, ZK, Proxy), 
you will have 3 instances websocket-kafka proxies available on 8080, 8081, 8082 ports. 

# Launch Locally
You can launch kafka proxy locally. You will need Kafka and ZK running.

```javascript
const WSKafka = require('../ws-kafka').WSKafkaProxy,
        debug = require('debug')('ws-kafka:test');


conf_module.clientConfig = {
        //node-kafka options
        kafkaHost: getBrokerList(),
        clientId: 'test-kafka-client-2',
        connectTimeout: 1000,
        requestTimeout: 60000,
        autoConnect: true,
        //custom options
        no_zookeeper_client: true
    };

    conf_module.webSocketConfig ={
        port: getWebSocketPort()
    };

    conf_module.producerConfig = {
        requireAcks: 1,
        ackTimeoutMs: 100,
        partitionerType: 2,
        // custom options
        mq_limit: 20000,
        mq_interval: 200 //if null, then messages published immediately
    };

    conf_module.consumerConfig ={
        // host: 'zookeeper:2181',  // zookeeper host omit if connecting directly to broker (see kafkaHost below)
        kafkaHost: getBrokerList(),
        ssl: true, // optional (defaults to false) or tls options hash
        groupId: 'kafka-node-group', //should be set by message to ws
        autoCommit: true,
        autoCommitIntervalMs: 500,
        // Fetch message config
        fetchMaxWaitMs: 100,
        paused: false,
        maxNumSegments: 1000,
        fetchMinBytes: 1,
        fetchMaxBytes: 1024 * 1024,
        maxTickMessages: 1000,
        fromOffset: 'latest',
        outOfRangeOffset: 'earliest',
        sessionTimeout: 30000,
        retries: 10,
        retryFactor: 1.8,
        retryMinTimeout: 1000,
        connectOnReady: true,
        migrateHLC: false,
        migrateRolling: true,
        protocol: ['roundrobin'],
        // custom options
        mq_limit: 5000,
        mq_interval: 50 //if null, then messages published immediately
    };

    conf_module.brokerType = `kafka`;

const wsk = new WSKafka(conf_module);

wsk.on('ws-connection', (ws, req) => debug('connection'))
    .on('ws-close', () => debug('ws-close'))
    .on('wss-ready', () => debug('wss-ready'))
    .on('producer-ready', () => debug('producer-ready'))
    .on('producer-error', (e) => console.log(`producer-error ${e}`))
    .on('consumer-ready', () => debug('consumer-ready'))
    .on('consumer-error', (e) => console.log(`consumer-error ${e}`))
    .on('consumer-message', () => {})
    .on('error', (e) => console.log(`error ${e}`));

wsk.start();

```

Each WebSocket connection to the proxy may have one Kafka producer and one consumer. 
The producer is initialized with each WebSocket connection. The consumer is initialized after subscription message received.   

## Notes on configuration
producer_config and consumer_config objects both have two custom options 
`mq_limit: 5000` specifies the maximum message number to be stored in the buffer before sending them. 
`mq_interval: 50` specifies the number in ms how often should message from the buffer be sent to Kafka brokers or the WebSocket.  

# Message Structure
## General
All messages are `JSON` based. Generic message structure looks like this:
```json
{
  "id":"id or original message",
  "t":"message type",
  "a":"action",
  "s":"success",
  "p":"payload"
}
```

| Param | Type | Description |
| --- | --- | --- |
| id | `String or Int` | Original Message Id |
| t | `String` | Type: ["topic","notif","health"] |
| a | `String` | Action: ["create","list","subscribe","unsubscribe", "ack"]|
| s | `Int` | Status, returned by the server, 0 if OK. |
| p | `String` | Custom payload |

Server can receive an list of messages in one batch.

`NOTE:` You will get ack message on any message you send to Proxy. This means that Proxy has received message, will process and push it to Kafka.

Ack message:
```json
{
  "id" : 1, 
  "t" : "ack",
  "s" : 0,
  "p" : {}
}
```

In case of errors you will have error message in `"p"` field and `"s" : 1`

## Topics
### Create
```json
{
  "id":1, 
  "t":"topic",
  "a":"create", 
  "p":["topic1", "topic2", "topicN"]
}
```
`NOTE:` kafka-node does not support configuration of number of partitions per particular topic.
You can only use Kafka broker setting for now `KAFKA_NUM_PARTITIONS` in docker or `num.partitions` in Kafka config. 

Response message:

```json
{"id":1, "t":"topic","a":"create","s":0,"p":["topic1","topic2","topicN"]}
```

#### List
```json
{
  "id":23, 
  "t":"topic",
  "a":"list"
}
```
Response message:

```json
{
  "id":23,
  "t":"topic",
  "a":"list",
  "s":0,
  "p":
  [{"topic":"topic2","partition":0},{"topic":"topic2","partition":1},{"topic":"topic2","partition":2},{"topic":"topic1","partition":0},{"topic":"topic1","partition":1},{"topic":"topic1","partition":2},{"topic":"topicN","partition":0},{"topic":"topicN","partition":1},{"topic":"topicN","partition":2}]}
```
Payload is the list of topic-partitions structures.

### Subscribe
Subscribe to topic and join consumer group
```json
{
    "id":1000,
    "t":"topic",
    "a":"subscribe",
    "p":
      {
        "t":["topic1","topic2"],
        "consumer_group":"ingestion_1",
        "customConsumerConfig" : {}
      }
}
```

Payload is the structure of `t` - list of topics and `consumer_group` to join. 
if `consumer_group` is not specified then default `groupId` from `conf_module.consumer_config`
will be used.
You could also specify your own consumer config or just change some of default config fields by passing object in `customConsumerConfig` field.

Response message:
```json
{"id":1000,"t":"topic","a":"subscribe","p":["topic1","topic2"],"s":0}
```

All notifications from Kafka will be sent to the same WebSocket connection where the subscription was made.  

### Unsubscribe
Unsubscribe from topics and consumer group
```json
{
    "id":1300,
    "t":"topic",
    "a":"unsubscribe"
}
```

Response message:
```json
{"id":1300,"t":"topic","a":"unsubscribe","s":0}
```

## Notification
### Send
```json
{"id":1320,"t":"notif","a":"create","p":{"t":"topic1", "m":"{custom_message:'msg'}", "part" : 1}}
```
Response message:

```json
{"id" : 1320, "t" : "notif", "a" : "create", "s" : 0, "p":{}}
```

### Receive
Notifications are received automatically after subscription.
```json
[{"id":1320,"t":"notif","p":"hello"}]
```
A list of notifications with the payload which was sent using send notification message.

## Healthcheck
```json
{
    "id":1500,
    "t":"health"
}
```

Response message:
```json
{"id":1500,"t":"health","s":0,"p":{"status": "available|failed"}}
```
Payload contains current status of Kafka broker.
