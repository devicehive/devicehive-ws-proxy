const MessagePayload = require(`./MessagePayload`);

class NotificationMessagePayload extends MessagePayload {

    static normalize({ m } = {}) {
        return new NotificationMessagePayload({
            message: m
        });
    }

    constructor({ message } = {}) {
        super();

        const me = this;

        me.message = message;
    }

    get message() {
        const me = this;

        return me._message;
    }

    set message(value) {
        const me = this;

        me._message = value;
    }

    toObject() {
        const me = this;

        return {
            m: me.message
        }
    }
}


module.exports = NotificationMessagePayload;