
class TokenPayload {

    static get REFRESH_TOKEN_TYPE() { return 0; }
    static get ACCESS_TOKEN_TYPE() { return 1; }

    static normalize({ e, t, tpc } = {}) {
        return new TokenPayload({
            expirationDate: e,
            type: t,
            topic: tpc
        });
    }

    constructor({ expirationDate, type, topic } = {}) {
        const me = this;

        me.expirationDate = expirationDate;
        me.type = type;
        me.topic = topic;
    }

    get topic() {
        const me = this;

        return me._topic;
    }

    set topic(value) {
        const me = this;

        me._topic = value;
    }

    get expirationDate() {
        const me = this;

        return me._expirationDate;
    }

    set expirationDate(value) {
        const me = this;

        me._expirationDate = value;
    }

    get type() {
        const me = this;

        return me._type;
    }

    set type(value) {
        const me = this;

        me._type = value;
    }

    toObject() {
        const me = this;

        return {
            e: me.expirationDate,
            t: me.type,
            tpc: me.topic
        }
    }

    toString() {
        const me = this;

        return JSON.stringify(me.toObject());
    }
}


module.exports = TokenPayload;