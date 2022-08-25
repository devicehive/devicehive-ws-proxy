const ProxyError = require(`../ProxyError`);

/**
 * Message buffer overfilling error
 */
class FullMessageBufferError extends ProxyError {
    /**
     * @param {Object} messageObject
     * @constructor
     */
    constructor(messageObject) {
        super(
            `Message buffer is full. Please, try again after a while`,
            messageObject
        );
    }
}

module.exports = FullMessageBufferError;
