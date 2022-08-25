[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# WebSocket proxy to message broker

The project wrap some of the essential message broker functionality
allowing you to communicate with the next message brokers through WebSockets:

-   Kafka

# Start the proxy

## Internal mode

Run the next command:

    - node ./src/proxy.js

## External mode (with enabled PluginManager)

In configuration file:

    [path-to-proxy-project]/src/config.json

set the **ENABLE_PLUGIN_MANAGER** field to **true**
**or**  
Set environmental variable **PROXY.ENABLE_PLUGIN_MANAGER** to **true**  
And run the next command:

    - node ./src/proxy.js

By default proxy listening for WS connections on **ws://localhost:3000** (independent from the mode).
To change it set the **WEB_SOCKET_SERVER_HOST** and **WEB_SOCKET_SERVER_PORT** fields in the configuration file or
set environmental variables: **PROXY.WEB_SOCKET_SERVER_HOST** and **PROXY.WEB_SOCKET_SERVER_PORT**

# Configuration

## Proxy

    [path-to-proxy-project]/src/config.json

-   **_WEB_SOCKET_SERVER_HOST_** - WebSocket server host address (default: "localhost");
-   **_WEB_SOCKET_SERVER_PORT_** - WebSocket server port to listen (default: 3000);
-   **_WEB_SOCKET_PING_INTERVAL_SEC_** - Time interval in seconds between ping messages (default: 30);
-   **_ACK_ON_EVERY_MESSAGE_ENABLED_** - Enable/disable acknowledgment for every received message (default: false);
-   **_ENABLE_PLUGIN_MANAGER_** - Enable plugin manager (default: false);
-   **_COMMUNICATOR_TYPE_** - Message broker that will be used internally (default: "kafka");
-   **_APP_LOG_LEVEL_** - Proxy logger level: debug, info, warn, error (default: "info");

Each configuration field can be overridden with corresponding environmental variable with "PROXY" prefix, for example:

    PROXY.WEB_SOCKET_SERVER_PORT=6000

Prefix separator can be overridden by **_ENVSEPARATOR_** environmental variable. Example:

    ENVSEPARATOR=_
    PROXY_WEB_SOCKET_SERVER_PORT=6000

### Message Buffer configuration

    [path-to-proxy-project]/src/messageBuffer/config.json

-   **_MAX_SIZE_MB_** - Maximum Message Buffer size in MB (default: 128);

Each configuration field can be overridden with corresponding environmental variable with "MESSAGE_BUFFER" prefix, for example:

    MESSAGE_BUFFER.MAX_SIZE_MB=256

Prefix separator can be overridden by **_ENVSEPARATOR_** environmental variable. Example:

    ENVSEPARATOR=_
    MESSAGE_BUFFER_MAX_SIZE_MB=256

### Plugin Manager configuration

    [path-to-proxy-project]/src/pluginManager/config.json

-   **_AUTH_SERVICE_ENDPOINT_** - DeviceHive auth service REST endpoint (default: http://localhost:8090/dh/rest);
-   **_PLUGIN_MANAGEMENT_SERVICE_ENDPOINT_** - DeviceHive plugin management service REST endpoint (default: http://localhost:8110/dh/rest);

Each configuration field can be overridden with corresponding environmental variable with "PLUGIN_MANAGER" prefix, for example:

    PLUGIN_MANAGER.AUTH_SERVICE_ENDPOINT=http://localhost:9090/dh/rest

Prefix separator can be overridden by **_ENVSEPARATOR_** environmental variable. Example:

    ENVSEPARATOR=_
    PLUGIN_MANAGER_AUTH_SERVICE_ENDPOINT=http://localhost:9090/dh/rest

## Message Brokers

### Kafka

    [path-to-proxy-project]/src/kafka/config.json

-   **_KAFKA_HOSTS_** - Address to Kafka server (default: "localhost:9092");
-   **_KAFKA_CLIENT_ID_** - Kafka client name prefix (default: "ws-proxy-kafka-client");
-   **_CONSUMER_GROUP_ID_** - Kafka consumer group prefix (default: "ws-proxy-consumer-group");
-   **_LOGGER_LEVEL_** Kafka logger level (default: 0);
-   **_METADATA_POLLING_INTERVAL_MS_** Kafka metadata polling interval (default: 1000);
-   **_PRODUCER_MINIMAL_BATCHING_THROUGHPUT_PER_SEC_B_** Maximum throughput on producer side to work without batching (default: 1000);
-   **_CONSUMER_IDLE_TIMEOUT_** Timeout between consumer fetching requests (default: 0);
-   **_CONSUMER_MAX_WAIT_TIME_** Maximum wait time to collect batch with size of **_CONSUMER_MAX_BYTES_** (default: 0);
-   **_CONSUMER_MAX_BYTES_** Maximum batch size on consumer side in bytes (default: 1000000);

Each configuration field can be overridden with corresponding environmental variable with "KAFKA" prefix, for example:

    KAFKA.KAFKA_HOSTS=localhost:9094

Prefix separator can be overridden by **_ENVSEPARATOR_** environmental variable. Example:

    ENVSEPARATOR=_
    KAFKA_KAFKA_HOSTS=localhost:9094

## Proxy modules logging

Through the "DEBUG" environment variable you are able to specify next modules loggers:

-   **_websocketserver_** - WebSocket Server module logging;
-   **_internalcommunicatorfacade_** - Facade between WebSocket server and internal message broker module logging;
-   **_pluginmanager_** - Plugin Manager module logging;
-   **_messagebuffer_** - Message Buffer module logging;
-   **_kafka_** - Kafka Client module logging;

Example:

    DEBUG=kafka,messagebuffer,websocketserver

# Message Structure

## General

All messages are `JSON` based. Generic message structure looks like this:

```
{
  "id": "id of original message",
  "t": "message type",
  "a": "action",
  "s": "success",
  "p": "payload"
}
```

| Param | Type            | Description                                                              |
| ----- | --------------- | ------------------------------------------------------------------------ |
| id    | `String or Int` | Original Message Id                                                      |
| t     | `String`        | Type: ["topic","notif","health","plugin"]                                |
| a     | `String`        | Action: ["create","list","subscribe","unsubscribe","authenticate" "ack"] |
| s     | `Int`           | Status, returned by the server, 0 if OK.                                 |
| p     | `Object`        | Payload object                                                           |

Server can receive a list of messages in one batch.

### Ack message:

Success ACK:

```
{
    "t": "ack",
    "s": 0
}
```

Failure ACK:

```
{
    "t": "ack",
    "s": 1,
    "p": { "m": <error message> }
}
```

## Topics

### Create

Request message:

```
{
    "t": "topic",
    "a": "create",
    "p": { "t": ["topic1", "topic2", "topicN"] }
}
```

Response message:

```
{
    "t": "topic",
    "a": "create",
    "p": { "t": ["topic1", "topic2", "topicN"] },
    "s": 0
}
```

Error message:

```
{
    "t": "topic",
    "a": "create",
    "p": { "m": <error message> },
    "s": 1
}
```

#### List

Request message:

```
{
    "t": "topic",
    "a": "list"
}
```

Response message:

```
{
    "t": "topic",
    "a": "list",
    "p": { "t": ["topic1", "topic2", "topicN"] },
    "s": 0
}
```

Error message:

```
{
    "t": "topic",
    "a": "list",
    "p": { "m": <error message> },
    "s": 1
}
```

### Subscribe

Request message:

```
{
    "t": "topic",
    "a": "subscribe",
    "p": {
        "sg": "subscriptionGroup",
        "t": ["topic1", "topic2"]
    }
}
```

Response message:

```
{
    "t": "topic",
    "a": "subscribe",
    "p": { "sg": "subscriptionGroup", "t": ["topic1", "topic2"] },
    "s": 0
}
```

Error message:

```
{
    "t": "topic",
    "a": "subscribe",
    "p": { "m": <error message> },
    "s": 1
}
```

Where **_subscriptionGroup_** - group of consumers where messages apportions via balancing (RoundRobin) logic

### Unsubscribe

Request message:

```
{
    "t": "topic",
    "a": "unsubscribe",
    "p": { "t": ["topic1", "topic2"] }
}
```

Response message:

```
{
    "t": "topic",
    "a": "unsubscribe",
    "p": { "t": ["topic1", "topic2"] },
    "s": 0
}
```

Error message:

```
{
    "t": "topic",
    "a": "unsubscribe",
    "p": { "m": <error message> },
    "s": 1
}
```

## Plugin

### Authenticate

Request message:

```
{
    "t": "plugin",
    "a": "authenticate",
    "p": { "token": <plugin access token> }
}
```

Response message:

```
{
    "t": "plugin",
    "a": "authenticate",
    "p": {
        "tpc": <plugin topic name>,
        "e": <plugin access token expiration date>,
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

```
{
    "t": "plugin",
    "a": "authenticate",
    "p": { "m": <error message> },
    "s": 1
}
```

## Notification

### Send

Request message:

```
{
    "t": "notif",
    "a": "create",
    "p": {
        "t": "topic1",
        "m": <notification message srting>,
        "part": 1
    }
}
```

Response message:

```
{
    "t": "notif",
    "a": "create",
    "s": 0
}
```

Error message:

```
{
    "t": "notif",
    "a": "create",
    "p": { "m": <error message> },
    "s": 1
}
```

### Receive

Notifications are received automatically after subscription.
Notification message

```
{
    "t": "notif",
    "p": { "m": <notification message string> }
}
```

## Healthcheck

Request message

```
{
    "t": "health"
}
```

Response message:

```
{
    "t": "health",
    "s": 0,
    "p": {
        "prx": "Available|Not Available",
        "mb": "Available|Not Available",
        "mbfp": <0-100>,
        "comm": "Available|Not Available"
    }
}
```

Where:

**_prx_** - Proxy Status  
 **_mb_** - Message buffer status  
 **_mbfp_** - Message Buffer fill percentage  
 **_comm_** - Internal message broker status
