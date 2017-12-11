
class MessagePayload {

    toString() {
        const me = this;

        return JSON.stringify(me.toObject());
    }
}


module.exports = MessagePayload;