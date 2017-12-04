
class Utils {

    static forEach(array, callback) {
        if (Array.isArray(array)) {
            array.forEach(callback);
        } else {
            callback(array);
        }
    }

	static map(array, callback) {
		if (Array.isArray(array)) {
			return array.map(callback);
		} else {
			return [array].map(callback);
		}
	}

	static toArray(array) {
		if (Array.isArray(array)) {
			return array;
		} else {
			return [array];
		}
    }
}


module.exports = Utils;