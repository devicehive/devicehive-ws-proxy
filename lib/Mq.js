class Mq extends Array {
  constructor() {
    super();
    this._int = undefined;
    this._limit = 0;
  }

  set limit(v) {
    this._limit = v;
  }

  get limit() {
    return this._limit;
  }

  set interval(v) {
    this._int = v;
  }

  get interval() {
    return this._int;
  }

  shutdown() {
    if (this._int) {
      clearInterval(this._int);
    }
    super.length = 0;
  }
}

module.exports = Mq;