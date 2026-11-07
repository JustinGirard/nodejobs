// export class BaseData {
//   static schema() {
//     return { required: {}, optional: {} };
//   }

//   constructor(inData = {}) {
//     if (!inData || typeof inData !== "object" || Array.isArray(inData)) {
//       throw new Error("basedata_requires_object");
//     }
//     this._dirty = { ...inData };
//     const processed = this.do_pre_process({ ...inData });
//     if (!processed || typeof processed !== "object" || Array.isArray(processed)) {
//       throw new Error("do_pre_process_must_return_object");
//     }
//     const schema = this.constructor.schema();
//     const required = schema.required;
//     const optional = schema.optional;
//     const out = { ...processed };

//     for (const key of Object.keys(required)) {
//       if (!(key in out)) {
//         throw new Error(`missing_required_key:${key}`);
//       }
//       out[key] = this._validateAndConvert(key, out[key], required[key]);
//     }

//     for (const key of Object.keys(optional)) {
//       if (key in out) {
//         out[key] = this._validateAndConvert(key, out[key], optional[key]);
//       }
//     }

//     for (const [key, value] of Object.entries(out)) {
//       const [next, msg] = this.do_every_validation(key, value);
//       if (msg.length > 0) {
//         throw new Error(msg);
//       }
//       out[key] = next;
//     }

//     for (const [key, value] of Object.entries(out)) {
//       this[key] = value;
//     }
//   }

//   GetDirtyDict() {
//     return this._dirty;
//   }

//   do_pre_process(inData) {
//     return inData;
//   }

//   do_validation(_key, value) {
//     return [value, ""];
//   }

//   do_every_validation(_key, value) {
//     return [value, ""];
//   }

//   _validateAndConvert(key, value, expectedType) {
//     const [next, msg] = this.do_validation(key, value);
//     if (msg.length > 0) {
//       throw new Error(msg);
//     }
//     if (!this._validType(next, expectedType)) {
//       throw new Error(`invalid_type:${key}`);
//     }
//     return next;
//   }

//   _validType(value, expectedType) {
//     if (expectedType === "string") return typeof value === "string";
//     if (expectedType === "number") return typeof value === "number" && Number.isFinite(value);
//     if (expectedType === "object") return value !== null && typeof value === "object" && !Array.isArray(value);
//     if (expectedType === "array") return Array.isArray(value);
//     if (expectedType === "any") return true;
//     if (expectedType && expectedType.listOf === "string") {
//       return Array.isArray(value) && value.every((item) => typeof item === "string");
//     }
//     if (expectedType && expectedType.mapOf === "object") {
//       if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
//       return Object.values(value).every((item) => item !== null && typeof item === "object");
//     }
//     return false;
//   }
// }

// JavaScript counterpart of the Python BaseData contract.
// Instances are ordinary enumerable key/value objects; undeclared keys survive.
const STATE = Symbol("BaseData.state");
const has = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
const isObject = (value) =>
  value !== null && typeof value === "object" && !Array.isArray(value) &&
  (value instanceof BaseData || Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
const DIAGNOSTICS = Object.create(null);
const DIAGNOSTIC_STARTS = Object.create(null);
const DIAGNOSTIC_ENDS = Object.create(null);

function cloneStore(store) {
  return Object.fromEntries(
    Object.entries(store).map(([key, record]) => [key, { ...record }]),
  );
}

export class BaseData {
  static schema() {
    return { required: {}, optional: {}, defaults: {} };
  }

  // Override in browser applications to provide explicitly available env values.
  static environment() {
    return typeof process !== "undefined" ? process.env : {};
  }

  constructor(inData = {}) {
    if (!isObject(inData)) {
      throw new TypeError("basedata_requires_object");
    }

    const state = { dirty: { ...inData }, required: {}, optional: {}, defaults: {}, converters: {} };
    Object.defineProperty(this, STATE, { value: state });

    const processed = this.do_pre_process({ ...inData });
    if (!isObject(processed)) {
      throw new TypeError("do_pre_process_must_return_object");
    }

    const schema = this.constructor.schema();
    state.required = schema.required ?? {};
    state.optional = schema.optional ?? {};
    state.defaults = this.do_env_mapping(schema.defaults ?? {});
    state.converters = schema.converters ?? {};

    for (const key of Object.keys(state.required)) {
      if (has(state.optional, key)) {
        throw new Error(`duplicate_key_definition:${key}`);
      }
    }

    const out = this.do_env_mapping(processed);
    // Defaults are contract policy and are validated just like incoming values.
    for (const [key, value] of Object.entries(state.defaults)) {
      if (!has(state.required, key) && !has(state.optional, key)) {
        throw new Error(`default_for_undeclared_key:${key}`);
      }
      if (!has(out, key)) {
        Object.defineProperty(out, key, { value, writable: true, enumerable: true, configurable: true });
      }
    }

    for (const [key, expected] of Object.entries(state.required)) {
      if (key === "*") continue;
      if (!has(out, key)) throw new TypeError(`missing_required_key:${key}`);
      out[key] = this._validateAndConvert(key, out[key], expected);
    }
    for (const [key, expected] of Object.entries(state.optional)) {
      if (key !== "*" && has(out, key)) {
        out[key] = this._validateAndConvert(key, out[key], expected);
      }
    }

    const wildcard = state.required["*"] ?? state.optional["*"];
    if (wildcard !== undefined) {
      for (const key of Object.keys(out)) {
        if (!has(state.required, key) && !has(state.optional, key)) {
          out[key] = this._validateAndConvert(key, out[key], wildcard);
        }
      }
    }

    for (const [key, value] of Object.entries(out)) {
      const [next, message] = this.do_every_validation(key, value);
      if (message) throw new Error(message);
      const expected = state.required[key] ?? state.optional[key] ?? wildcard;
      if (expected !== undefined && !this._validTypeOrNullDefault(key, next, expected)) {
        throw new TypeError(`invalid_type:${key}`);
      }
      out[key] = next;
    }

    for (const [key, value] of Object.entries(out)) {
      Object.defineProperty(this, key, { value, writable: true, enumerable: true, configurable: true });
    }
    state.dirty = null;

    // Reading and writing normal fields stays dictionary-like, but mutations of
    // declared fields go through the same conversion and validation pipeline.
    return new Proxy(this, {
      get(target, key, receiver) {
        if (typeof key === "string" && has(target, key)) {
          return target.do_get_filtered(key, Reflect.get(target, key, receiver));
        }
        return Reflect.get(target, key, receiver);
      },
      set(target, key, value, receiver) {
        if (typeof key !== "string") {
          return Reflect.set(target, key, value, receiver);
        }
        // Allow subclass accessors to implement typed properties using this[field].
        let prototype = Object.getPrototypeOf(target);
        while (prototype && prototype !== BaseData.prototype) {
          const descriptor = Object.getOwnPropertyDescriptor(prototype, key);
          if (descriptor?.set) return Reflect.set(target, key, value, receiver);
          prototype = Object.getPrototypeOf(prototype);
        }

        const contract = target[STATE];
        const expected = contract.required[key] ?? contract.optional[key] ??
          contract.required["*"] ?? contract.optional["*"];
        if (expected !== undefined) {
          value = target._validateAndConvert(key, value, expected);
        }
        const [next, message] = target.do_every_validation(key, value);
        if (message) throw new Error(message);
        if (expected !== undefined && !target._validTypeOrNullDefault(key, next, expected)) {
          throw new TypeError(`invalid_type:${key}`);
        }
        Object.defineProperty(target, key, { value: next, writable: true, enumerable: true, configurable: true });
        return true;
      },
      deleteProperty(target, key) {
        if (typeof key === "string" && has(target[STATE].required, key)) {
          throw new Error(`cannot_remove_required_key:${key}`);
        }
        return Reflect.deleteProperty(target, key);
      },
    });
  }

  GetDirtyDict() {
    return this[STATE].dirty;
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

  do_get_filtered(_key, value) {
    return value;
  }

  do_env_mapping(value) {
    if (value instanceof BaseData) return value;
    if (Array.isArray(value)) return value.map((item) => this.do_env_mapping(item));
    if (isObject(value)) {
      const out = {};
      for (const [key, entry] of Object.entries(value)) {
        Object.defineProperty(out, key, {
          value: this.do_env_mapping(entry), writable: true, enumerable: true, configurable: true,
        });
      }
      return out;
    }
    if (typeof value === "string") {
      return value.replace(/<<([A-Z_][A-Z0-9_]*)>>/g, (_match, name) => {
        const environment = this.constructor.environment();
        if (!has(environment, name) || environment[name] == null) {
          throw new Error(`Missing required environment variable: '${name}' for '${this.constructor.name}'`);
        }
        return String(environment[name]);
      });
    }
    return value;
  }

  _validateAndConvert(key, value, expectedType) {
    const converter = this[STATE].converters[key];
    if (converter !== undefined) value = converter(value);
    value = this._convert(value, expectedType);
    const [next, message] = this.do_validation(key, value);
    if (message) throw new Error(message);
    if (!this._validTypeOrNullDefault(key, next, expectedType)) {
      throw new TypeError(`invalid_type:${key}`);
    }
    return next;
  }

  _convert(value, expectedType) {
    if (typeof expectedType === "function" && (expectedType === BaseData || expectedType.prototype instanceof BaseData)) {
      if (value instanceof expectedType) return value;
      if (isObject(value)) return new expectedType(value);
      return value; // _validType reports the declared type violation.
    }
    if (expectedType && typeof expectedType === "object") {
      if (has(expectedType, "listOf") && Array.isArray(value)) {
        return value.map((item) => this._convert(item, expectedType.listOf));
      }
      if (has(expectedType, "mapOf") && isObject(value)) {
        return Object.fromEntries(
          Object.entries(value).map(([key, item]) => [key, this._convert(item, expectedType.mapOf)]),
        );
      }
    }
    return value;
  }

  _validTypeOrNullDefault(key, value, expectedType) {
    if (value === null && has(this[STATE].defaults, key) && this[STATE].defaults[key] === null) {
      return true;
    }
    return this._validType(value, expectedType);
  }

  _validType(value, expectedType) {
    if (expectedType === "any") return true;
    if (expectedType === "string" || expectedType === String) return typeof value === "string";
    if (expectedType === "number" || expectedType === Number) return typeof value === "number" && Number.isFinite(value);
    if (expectedType === "integer") return typeof value === "number" && Number.isInteger(value);
    if (expectedType === "boolean" || expectedType === Boolean) return typeof value === "boolean";
    if (expectedType === "object" || expectedType === Object) return isObject(value);
    if (expectedType === "array" || expectedType === Array) return Array.isArray(value);
    if (typeof expectedType === "function") return value instanceof expectedType;
    if (expectedType && typeof expectedType === "object") {
      if (has(expectedType, "listOf")) {
        return Array.isArray(value) && value.every((item) => this._validType(item, expectedType.listOf));
      }
      if (has(expectedType, "mapOf")) {
        return isObject(value) && Object.values(value).every((item) => this._validType(item, expectedType.mapOf));
      }
    }
    return false;
  }

  set(key, value) {
    this[key] = value;
    return true;
  }

  clean() {
    const { required, optional } = this[STATE];
    const out = {};
    for (const key of [...Object.keys(required), ...Object.keys(optional)]) {
      if (key !== "*" && has(this, key)) out[key] = this[key];
    }
    return new this.constructor(out);
  }

  static DiagnosticPrint(title, index, start_id, end_id, description_of_location, interesting_conditionals, repeat_delay) {
    const key = String(index);
    const nowTs = Date.now() / 1000;

    let delay = repeat_delay;
    if (delay !== null && delay !== undefined) {
      if (typeof delay !== "number" || !Number.isFinite(delay) || (delay < 0 && delay !== -1)) {
        throw new Error("invalid_repeat_delay");
      }
    }

    const existing = DIAGNOSTICS[key];
    let allowed = false;
    if (delay === null || delay === undefined || delay === 0) {
      allowed = true;
    } else if (delay === -1) {
      allowed = existing === undefined;
    } else if (existing === undefined) {
      allowed = true;
    } else {
      const lastTs = Number(existing.timestamp);
      allowed = (nowTs - lastTs) >= delay;
    }

    if (!allowed) return false;

    const record = {
      title,
      index,
      start_id,
      end_id,
      description_of_location,
      interesting_conditionals,
      timestamp: nowTs,
    };
    console.log(record);
    DIAGNOSTICS[key] = record;
    if (start_id !== null && start_id !== undefined) DIAGNOSTIC_STARTS[key] = { ...record };
    if (end_id !== null && end_id !== undefined) DIAGNOSTIC_ENDS[key] = { ...record };
    return true;
  }

  static DiagnosticDump() {
    return {
      diagnostics: cloneStore(DIAGNOSTICS),
      starts: cloneStore(DIAGNOSTIC_STARTS),
      ends: cloneStore(DIAGNOSTIC_ENDS),
    };
  }
}
