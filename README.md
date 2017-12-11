[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# WebSocket proxy to message broker
The project wrap some of the essential message broker functionality 
allowing you to communicate with the next message brokers through WebSockets:

- Kafka


# Configuration
## Proxy
    [path-to-proxy-project]/src/config.json    

- **_WEB_SOCKET_SERVER_HOST_** - WebSocket server host address (default: "localhost");  
- **_WEB_SOCKET_SERVER_PORT_** - WebSocket server port to listen (default: 3000);  
- **_WEB_SOCKET_PING_INTERVAL_S_** - Time interval in seconds between ping messages (default: 30);  
- **_ACK_ON_EVERY_MESSAGE_ENABLED_** - Enable/disable acknowledgment for every received message (default: false);  
- **_AUTH_SERVICE_ENDPOINT_** - DeviceHive Auth REST service address (default: "http://localhost:8090/dh/rest");  
- **_ENABLE_PLUGIN_MANGER_** - Enable plugin manager (default: false);  
- **_COMMUNICATOR_TYPE_** - Message broker that will be used internally (default: "kafka");  
- **_APP_LOG_LEVEL_** - Proxy logger level: debug, info, warn, error (default: "info");  
- **_MESSAGE_BUFFER:_**  
    - **_BUFFER_POLLING_INTERVAL_MS_** - Message buffer polling interval in ms (default: 50);  
    - **_BUFFER_POLLING_MESSAGE_AMOUNT_** - Amount of messages that will be shifted from message buffer on each buffer polling (default: 500);  
    - **_MAX_SIZE_MB_** - Maximum Message Buffer size in MB (default: 128);  

Each configuration field can be overridden with corresponding environmental variable with "PROXY" prefix, for example:

    PROXY.WEB_SOCKET_SERVER_PORT=6000

## Message Brokers
### Kafka
    [path-to-proxy-project]/src/kafka/config.json   
    
- **_KAFKA_HOSTS_** - Address to Kafka server (default: "localhost:9092");  
- **_KAFKA_CLIENT_ID_** - Kafka client name prefix (default: "ws-proxy-kafka-client");  
- **_CONSUMER_GROUP_ID_** - Kafka consumer group prefix (default: "ws-proxy-consumer-group");  
- **_LOGGER_LEVEL_** Kafka logger level (default: 0);  
    
Each configuration field can be overridden with corresponding environmental variable with "KAFKA" prefix, for example:

    KAFKA.KAFKA_HOSTS=localhost:9094
    
# Start the proxy

    - node ./src/proxy.js


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

| Param | Type            | Description |
| ---   | ---             | --- |
| id    | `String or Int` | Original Message Id |
| t     | `String`        | Type: ["topic","notif","health","plugin"] |
| a     | `String`        | Action: ["create","list","subscribe","unsubscribe","authenticate" "ack"]|
| s     | `Int`           | Status, returned by the server, 0 if OK. |
| p     | `Object`        | Payload object |

Server can receive an list of messages in one batch.

### Ack message:
Success ACK:
```json
{
    "t" : "ack",
    "s" : 0
}
```

Failure ACK:
```json
{
    "t" : "ack",
    "s" : 1,
    "p" : { "m": [errorMessage] }
}
```

## Topics
### Create
Request message:
```json
{
    "t": "topic",
    "a": "create", 
    "p": { "t": ["topic1", "topic2", "topicN"] }
}
```

Response message:
```json
{
    "t": "topic",
    "a": "create",
    "p": { "t": ["topic1","topic2","topicN"] },
    "s": 0 
}
```

Error message:
```json
{
    "t": "topic",
    "a": "create",
    "p": { "m": [errorMessage] },
    "s": 1 
}
```

#### List
Request message:
```json
{
    "t": "topic",
    "a": "list"
}
```

Response message:
```json
{
    "t": "topic",
    "a": "list",
    "p": { "t": ["topic1", "topic2", "topicN"] },
    "s": 0
}
```

Error message:
```json
{
    "t": "topic",
    "a": "list",
    "p": { "m": [errorMessage] },
    "s": 1 
}
```

### Subscribe
Request message:
```json
{
    "t": "topic",
    "a": "subscribe",
    "p": { "t": ["topic1","topic2"] }
}
```

Response message:
```json
{
    "t": "topic",
    "a": "subscribe",
    "p": { "t": ["topic1","topic2"] },
    "s": 0
}
```

Error message:
```json
{
    "t": "topic",
    "a": "subscribe",
    "p": { "m": [errorMessage] },
    "s": 1 
}
```

### Unsubscribe
Request message:
```json
{
    "t": "topic",
    "a": "unsubscribe",
    "p": { "t": ["topic1","topic2"] }
}
```

Response message:
```json
{
    "t":"topic",
    "a":"unsubscribe",
    "p": { "t": ["topic1","topic2"] },
    "s":0
}
```

Error message:
```json
{
    "t": "topic",
    "a": "unsubscribe",
    "p": { "m": [errorMessage] },
    "s": 1 
}
```

## Plugin
### Authenticate
Request message:
```json
{
    "t": "plugin",
    "a": "authenticate",
    "p": { "token": [pluginAccessToken] }
}
```

Response message:
```json
{
    "t": "plugin", 
    "a": "authenticate",
    "p": {
        "tpc": [pluginTopicName],
        "e": [pluginAccessTokenExpirationDate],
        "t": 1 
    },
    "s": 0
}
```
Where:

**_tpc_** - plugin topic name  
**_e_** - plugin access token expiration date  
**_t_** - plugin token type (0 - refresh token, 1 - access token)  

Error message:
```json
{
    "t": "plugin",
    "a": "authenticate",
    "p": { "m": [errorMessage] },
    "s": 1 
}
```

## Notification
### Send
Request message:
```json
{
    "t": "notif",
    "a": "create",
    "p": {
        "t": "topic1", 
        "m": [notificationMessageSrting], 
        "part": 1
    }
}
```

Response message:
```json
{
    "t": "notif", 
    "a": "create",
    "s": 0
}
```

Error message:
```json
{
    "t": "notif",
    "a": "create",
    "p": { "m": [errorMessage] },
    "s": 1 
}
```

### Receive
Notifications are received automatically after subscription.
Notification message
```json
{
    "t": "notif",
    "p": { "m": [notificationMessageString] }
}
```

## Healthcheck
Request message
```json
{
    "t": "health"
}
```

Response message:
```json
{
    "id":1500,
    "t":"health",
    "s":0,
    "p": {
        "prx": "Available|Not Available",
        "mb": "Available|Not Available",
        "mbfp": [0-100%],
        "comm": "Available|Not Available"
    }
}
```
Where:

 **_prx_** - Proxy Status  
 **_mb_** - Message buffer status  
 **_mbfp_** - Message Buffer fill percentage  
 **_comm_** - Internal message broker status  
