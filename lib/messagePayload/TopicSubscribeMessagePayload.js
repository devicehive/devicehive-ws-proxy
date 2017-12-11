const MessagePayload = require(`./MessagePayload`);

class TopicSubscribeMessagePayload extends MessagePayload {

    static normalize({ t } = {}) {
        return new TopicSubscribeMessagePayload({
            topicList: t
        });
    }

    constructor({ topicList } = {}) {
        super();

        const me = this;

        me.topicList = topicList;
    }

    get topicList() {
        const me = this;

        return me._topicList;
    }

    set topicList(value) {
        const me = this;

        me._topicList = value;
    }

    toObject() {
        const me = this;

        return {
            t: me.topicList
        }
    }
}


module.exports = TopicSubscribeMessagePayload;