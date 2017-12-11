const ProxyError = require(`../ProxyError`);


class FullMessageBufferError extends ProxyError {

    constructor(messageObject) {
        super(`Message buffer is full. Please, try again after a while`, messageObject);
    }
}


module.exports = FullMessageBufferError;
