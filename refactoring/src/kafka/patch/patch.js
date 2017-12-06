const NoKafka = require(`no-kafka`);
const Promise      = require('./bluebird-configured');
const _            = require('lodash');


NoKafka.GroupConsumer.prototype._heartbeat = function () {
    var self = this;

    return self.client.heartbeatRequest(self.options.groupId, self.memberId, self.generationId)
        .then(() => {
           self.isAvailable = true;
        })
        .catch({ code: 'RebalanceInProgress' }, function () {
            self.isAvailable = false;
            // new group member has joined or existing member has left
            self.client.log('Rejoining group on RebalanceInProgress');
            return self._rejoin();
        })
        .tap(function () {
            self._heartbeatTimeout = setTimeout(function () {
                self._heartbeatPromise = self._heartbeat();
            }, self.options.heartbeatTimeout);
        })
        .catch(function (err) {
            self.isAvailable = false;
            // some severe error, such as GroupCoordinatorNotAvailable or network error
            // in this case we should start trying to rejoin from scratch
            self.client.error('Sending heartbeat failed: ', err);
            return self._fullRejoin().catch(function (_err) {
                self.client.error(_err);
            });
        });
};

NoKafka.GroupConsumer.prototype._syncGroup = function () {
    var self = this;

    return Promise.try(function () {
        if (self.memberId === self.leaderId) { // leader should generate group assignments
            return self.client.updateMetadata().then(function () {
                var r = [];
                _.each(self.members, function (member) {
                    _.each(_.union(member.subscriptions,
                        _.uniq(Object.keys(self.subscriptions).map(key => key.split(`:`)[0]))), function (topic) {
                        r.push([topic, member]);
                    });
                });
                r = _(r).groupBy(0).map(function (val, key) {
                    if (!self.client.topicMetadata[key]) {
                        self.client.error('Sync group: unknown topic:', key);
                    }
                    return {
                        topic: key,
                        members: _.map(val, 1),
                        partitions: _.map(self.client.topicMetadata[key], 'partitionId')
                    };
                }).value();

                return self.strategies[self.strategyName].strategy.assignment(r);
            });
        }
        return [];
    })
        .then(function (result) {
            var assignments = _(result).groupBy('memberId').mapValues(function (mv, mk) {
                return {
                    memberId: mk,
                    memberAssignment: {
                        version: 0,
                        metadata: null,
                        partitionAssignment: _(mv).groupBy('topic').map(function (tv, tk) {
                            return {
                                topic: tk,
                                partitions: _.map(tv, 'partition')
                            };
                        }).value()
                    }
                };
            }).values().value();

            // console.log(require('util').inspect(assignments, true, 10, true));
            return self.client.syncConsumerGroupRequest(self.options.groupId, self.memberId, self.generationId, assignments);
        })
        .then(function (response) {
            return self._updateSubscriptions(_.get(response, 'memberAssignment.partitionAssignment', []));
        });
};

NoKafka.GroupConsumer.prototype._updateSubscriptions = function (partitionAssignment) {
    var self = this, offsetRequests = [],
        handler = self.strategies[self.strategyName].handler;
    var previousSubscriptions = Object.assign({}, self.subscriptions);

    self.subscriptions = {};

    if (_.isEmpty(partitionAssignment)) {
        return self.client.warn('No partition assignment received');
    }

    // should probably wait for current fetch/handlers to finish before fetching offsets and re-subscribing

    _.each(partitionAssignment, function (a) {
        _.each(a.partitions, function (p) {
            offsetRequests.push({
                topic: a.topic,
                partition: p
            });
        });
    });

    return self.client.updateMetadata().then(function () {
        return self.fetchOffset(offsetRequests).map(function (p) {
            var options = {
                offset: previousSubscriptions[`${p.topic}:${p.partition}`] ?
                    previousSubscriptions[`${p.topic}:${p.partition}`].offset : p.offset
            };

            if (p.error || p.offset < 0) {
                options = {
                    time: self.options.startingOffset
                };
            }

            return self.subscribe(p.topic, p.partition, options, handler).catch(function (err) {
                self.client.error('Failed to subscribe to', p.topic + ':' + p.partition, err);
            });
        });
    });
};
