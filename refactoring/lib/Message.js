
class Message {

    static get TOPIC_TYPE() { return `topic`; }
    static get NOTIFICATION_TYPE() { return `notif`; }
    static get HEALTH_CHECK_TYPE() { return `health`; }
    static get PLUGIN_TYPE() { return `plugin`; }
    static get ACK_TYPE() { return `ack`; }

    static get CREATE_ACTION() { return `create`; }
    static get LIST_ACTION() { return `list`; }
    static get SUBSCRIBE_ACTION() { return `subscribe`; }
    static get UNSUBSCRIBE_ACTION() { return `unsubscribe`; }
    static get AUTHENTICATE_ACTION() { return `authenticate`; }

    static get SUCCESS_STATUS() { return 0; }
    static get FAILED_STATUS() { return 1; }

    static normalize(data) {
        return new Message({
            id: data.id,
            type: data.t,
            action: data.a,
            payload: data.p,
            status: data.s
        })
    }

    constructor({ id, type, action, payload, status } = {}) {
        const me = this;

        me.id = id;
        me.type = type;
        me.action = action;
        me.payload = payload;
        me.status = status;
    }

    get id() {
        const me = this;

        return me._id;
    }

    set id(value) {
        const me = this;

        me._id = value;
    }

    get type() {
        const me = this;

        return me._type;
    }

    set type(value) {
        const me = this;

        me._type = value;
    }

    get action() {
        const me = this;

        return me._action;
    }

    set action(value) {
        const me = this;

        me._action = value;
    }

    get payload() {
        const me = this;

        return me._payload;
    }

    set payload(value) {
        const me = this;

        me._payload = value;
    }

    get status() {
        const me = this;

        return me._status;
    }

    set status(value) {
        const me = this;

        me._status = value;
    }

    toObject() {
        const me = this;

        return {
            id: me.id,
            t: me.type,
            a: me.action,
            p: me.payload,
            s: me.status
        }
    }

    toString() {
        const me = this;

        return JSON.stringify(me.toObject());
    }
}


module.exports = Message;