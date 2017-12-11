const MessagePayload = require(`./MessagePayload`);

class PluginAuthenticateMessagePayload extends MessagePayload {

    static normalize({ token } = {}) {
        return new PluginAuthenticateMessagePayload({
            token: token
        });
    }

    constructor({ token } = {}) {
        super();

        const me = this;

        me.token = token;
    }

    get token() {
        const me = this;

        return me._token;
    }

    set token(value) {
        const me = this;

        me._token = value;
    }

    toObject() {
        const me = this;

        return {
            t: me.token
        }
    }
}


module.exports = PluginAuthenticateMessagePayload;