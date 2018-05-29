const debug = require(`debug`)(`loadbalancer`);

/**
 *
 */
class LoadBalancer {

    /**
     *
     * @param workersAmount
     */
    constructor(workersAmount) {

        const me = this;

        me.workersAmout = workersAmount;
        me.workersMap = new Map();

        for (let workerCount = 0; workerCount < workersAmount; workerCount++) {
            me.workersMap.set(workerCount, {});
        }

    }

    /**
     *
     * @param type
     * @param role
     * @returns {number}
     */
    getWorker(type, role) {
        const me = this;
        const loadMap = { key: 0, load: 0 };
        const tag = type && role ? `${type}-${role}` : 'unknown';

        me.workersMap.forEach((load, key) => {
            load[tag] = load[tag] || 0;

            if (key === 0 || load[tag] < loadMap.load) {
                loadMap.load = load[tag];
                loadMap.key = key;
            }
        });

        me.workersMap.get(loadMap.key)[tag]++;

        debug(`The key: ${loadMap.key} will be used as worker for the tag: ${tag}`);

        return loadMap.key;
    }

    /**
     *
     * @param number
     * @param type
     * @param role
     */
    freeWorker(number, type, role) {
        const me = this;
        const tag = type && role ? `${type}-${role}` : 'unknown';

        debug(`The key: ${number} will be freed from the tag: ${tag}`);

        me.workersMap.get(number)[tag]--;
    }
}


module.exports = LoadBalancer;

