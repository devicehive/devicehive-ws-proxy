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

        me.workersMap.get(number)[tag]--;
    }
}


module.exports = LoadBalancer;

