/**
 * Utility methods class
 */
class Utils {
    /**
     * @return {number}
     */
    static get MS_IN_S() {
        return 1000;
    }

    /**
     * @return {number}
     */
    static get B_IN_KB() {
        return 1024;
    }

    /**
     * @return {number}
     */
    static get B_IN_MB() {
        return 1024 * 1024;
    }

    /**
     * @return {string}
     */
    static get EMPTY_STRING() {
        return ``;
    }

    /**
     * Iterate item or array of items
     * @param {Array} array
     * @param {Function} callback
     */
    static forEach(array, callback) {
        if (Array.isArray(array)) {
            array.forEach(callback);
        } else {
            callback(array);
        }
    }

    /**
     * Convert single item as array with one element or just returns array in case of array
     * @param {*} array
     * @return {Array}
     */
    static toArray(array) {
        if (Array.isArray(array)) {
            return array;
        } else {
            return [array];
        }
    }

    /**
     * Checks is variable not undefined or null
     * @param {*} variable
     * @return {boolean}
     */
    static isDefined(variable) {
        return !(typeof variable === "undefined" || variable === null);
    }

    /**
     * Returns value if it's defined, not null, and not an empty string, or else returns defaultValue
     * @param {*} value
     * @param {*} defaultValue
     * @return {*}
     */
    static value(value, defaultValue) {
        return Utils.isDefined(value) && value !== `` ? value : defaultValue;
    }

    /**
     * Checks that value is true or "true"
     * @param {boolean|string} value
     * @return {Boolean}
     */
    static isTrue(value) {
        return value === true ? true : value === `true`;
    }
}

module.exports = Utils;
