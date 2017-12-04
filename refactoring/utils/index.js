
class Utils {

    static forEach(array, callback) {
        if (Array.isArray(array)) {
            array.forEach(callback);
        } else {
            callback(array);
        }
    }

}


module.exports = Utils;