export class BaseData {
  static schema() {
    return { required: {}, optional: {} };
  }

  constructor(inData = {}) {
    if (!inData || typeof inData !== "object" || Array.isArray(inData)) {
      throw new Error("basedata_requires_object");
    }
    this._dirty = { ...inData };
    const processed = this.do_pre_process({ ...inData });
    if (!processed || typeof processed !== "object" || Array.isArray(processed)) {
      throw new Error("do_pre_process_must_return_object");
    }
    const schema = this.constructor.schema();
    const required = schema.required;
    const optional = schema.optional;
    const out = { ...processed };

    for (const key of Object.keys(required)) {
      if (!(key in out)) {
        throw new Error(`missing_required_key:${key}`);
      }
      out[key] = this._validateAndConvert(key, out[key], required[key]);
    }

    for (const key of Object.keys(optional)) {
      if (key in out) {
        out[key] = this._validateAndConvert(key, out[key], optional[key]);
      }
    }

    for (const [key, value] of Object.entries(out)) {
      const [next, msg] = this.do_every_validation(key, value);
      if (msg.length > 0) {
        throw new Error(msg);
      }
      out[key] = next;
    }

    for (const [key, value] of Object.entries(out)) {
      this[key] = value;
    }
  }

  GetDirtyDict() {
    return this._dirty;
  }

  do_pre_process(inData) {
    return inData;
  }

  do_validation(_key, value) {
    return [value, ""];
  }

  do_every_validation(_key, value) {
    return [value, ""];
  }

  _validateAndConvert(key, value, expectedType) {
    const [next, msg] = this.do_validation(key, value);
    if (msg.length > 0) {
      throw new Error(msg);
    }
    if (!this._validType(next, expectedType)) {
      throw new Error(`invalid_type:${key}`);
    }
    return next;
  }

  _validType(value, expectedType) {
    if (expectedType === "string") return typeof value === "string";
    if (expectedType === "number") return typeof value === "number" && Number.isFinite(value);
    if (expectedType === "object") return value !== null && typeof value === "object" && !Array.isArray(value);
    if (expectedType === "array") return Array.isArray(value);
    if (expectedType === "any") return true;
    if (expectedType && expectedType.listOf === "string") {
      return Array.isArray(value) && value.every((item) => typeof item === "string");
    }
    if (expectedType && expectedType.mapOf === "object") {
      if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
      return Object.values(value).every((item) => item !== null && typeof item === "object");
    }
    return false;
  }
}
