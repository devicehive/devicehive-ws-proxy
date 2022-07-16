/**
 * Application logger facade class.
 */
class ApplicationLogger {

    /**
     * Create new ApplicationLogger
     */
    constructor (applicationTag, loggerLevel = `LOGGER`) {}

    /**
     * Error log
     * @param str
     */
    err (str) {
        console.error(str);
    }

    /**
     * Warning log
     * @param str
     */
    warn (str) {
        console.warn(str);
    }

    /**
     * Information log
     * @param str
     */
    info (str) {
        console.info(str);
    }

    /**
     * Debug log
     * @param str
     */
    debug (str) {
        console.debug(str);
    }
}


module.exports = ApplicationLogger;
