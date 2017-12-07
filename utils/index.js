
/**
 *
 */
class Utils {

    /**
     *
     * @param array
     * @param callback
     */
    static forEach(array, callback) {
        if (Array.isArray(array)) {
            array.forEach(callback);
        } else {
            callback(array);
        }
    }

    /**
     *
     * @param array
     * @param callback
     * @returns {any[]}
     */
    static map(array, callback) {
        if (Array.isArray(array)) {
            return array.map(callback);
        } else {
            return [array].map(callback);
        }
    }

    /**
     *
     * @param array
     * @returns {*}
     */
    static toArray(array) {
        if (Array.isArray(array)) {
            return array;
        } else {
            return [array];
        }
    }

    /**
     *
     * @param variable
     * @returns {boolean}
     */
    static isDefined(variable) {
        return !(typeof variable === 'undefined' || variable === null);
    }

    /**
     *
     * @param value
     * @param defaultValue
     * @returns {*}
     */
    static value(value, defaultValue) {
        return (Utils.isDefined(value) && value !== ``) ? value : defaultValue;
    }
}


module.exports = Utils;