class Mq extends Array {
  /**
   * Creates an instance of Mq.
   * @memberof Mq
   */
  constructor() {
    super();
    this._int = undefined;
    this._limit = 0;
  }

  /**
   * Set message queue limit
   * 
   * @memberof Mq
   */
  set limit(v) {
    this._limit = v;
  }

  /**
   * Returns message queue limit
   * 
   * @readonly
   * @memberof Mq
   */
  get limit() {
    return this._limit;
  }

  /**
   * Set message queue check interval
   * 
   * @memberof Mq
   */
  set interval(v) {
    this._int = v;
  }

  /**
   * Returns message queue check interval
   * 
   * @readonly
   * @memberof Mq
   */
  get interval() {
    return this._int;
  }

  /**
   * Close message queue check interval
   * 
   * @memberof Mq
   */
  shutdown() {
    if (this._int) {
      clearInterval(this._int);
    }
    super.length = 0;
  }
}

module.exports = Mq;