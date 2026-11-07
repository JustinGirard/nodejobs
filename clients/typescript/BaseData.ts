
export type DataRecord = Record<string, unknown>;
export type BaseDataConstructor = new (input: DataRecord) => BaseData;

export type FieldType =
  | "string"
  | "number"
  | "integer"
  | "boolean"
  | "object"
  | "array"
  | "any"
  | StringConstructor
  | NumberConstructor
  | BooleanConstructor
  | ObjectConstructor
  | ArrayConstructor
  | BaseDataConstructor
  | { listOf: FieldType }
  | { mapOf: FieldType };

type InferField<T extends FieldType> =
  T extends "string" | StringConstructor ? string :
  T extends "number" | "integer" | NumberConstructor ? number :
  T extends "boolean" | BooleanConstructor ? boolean :
  T extends { listOf: infer U extends FieldType } ? InferField<U>[] :
  T extends { mapOf: infer U extends FieldType } ? Record<string, InferField<U>> :
  T extends BaseDataConstructor ? InstanceType<T> :
  T extends "array" | ArrayConstructor ? unknown[] :
  T extends "object" | ObjectConstructor ? DataRecord :
  unknown;

// Plain type = required; [type] = optional; [type, default] = defaulted.
export type FieldDefinition =
  | FieldType
  | readonly [FieldType]
  | readonly [FieldType, unknown];

type InferDefinition<T extends FieldDefinition> =
  T extends readonly [infer F extends FieldType, infer D]
    ? InferField<F> | (
        null extends D ? null | undefined :
        undefined extends D ? null | undefined : never
      )
    : T extends readonly [infer F extends FieldType]
      ? InferField<F>
      : T extends FieldType ? InferField<T> : never;

type DefinedFields<T extends Record<string, FieldDefinition>> = {
  [K in keyof T as T[K] extends readonly [FieldType] ? never : K]:
    InferDefinition<T[K]>;
} & {
  [K in keyof T as T[K] extends readonly [FieldType] ? K : never]?:
    InferDefinition<T[K]>;
};

export type DefinedBaseData<T extends Record<string, FieldDefinition>> =
  (new (input: DataRecord) => BaseData & DefinedFields<T>) & {
    readonly keys: { readonly [K in keyof T]: K & string };
  };

export interface BaseDataSchema {
  required: Record<string, FieldType>;
  optional: Record<string, FieldType>;
  defaults?: DataRecord;
  converters?: Record<string, (value: unknown) => unknown>;
}

interface DataState {
  dirty: DataRecord | null;
  required: Record<string, FieldType>;
  optional: Record<string, FieldType>;
  defaults: DataRecord;
  converters: Record<string, (value: unknown) => unknown>;
}

const STATE = Symbol("BaseData.state");

const has = (object: object, key: PropertyKey): boolean =>
  Object.prototype.hasOwnProperty.call(object, key);

function isObject(value: unknown): value is DataRecord {
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value)
  ) {
    return false;
  }

  return (
    value instanceof BaseData ||
    Object.getPrototypeOf(value) === Object.prototype ||
    Object.getPrototypeOf(value) === null
  );
}

function isBaseDataConstructor(
  value: unknown
): value is BaseDataConstructor {
  return (
    typeof value === "function" &&
    (value === BaseData || value.prototype instanceof BaseData)
  );
}

export class BaseData {
  // Dictionary-like access. Subclasses may declare typed fields.
  [key: string]: unknown;

  private declare [STATE]: DataState;
  private static _diagnostics: Record<string, DataRecord> = {};
  private static _diagnosticStarts: Record<string, DataRecord> = {};
  private static _diagnosticEnds: Record<string, DataRecord> = {};

  static schema(): BaseDataSchema {
    return {
      required: {},
      optional: {},
      defaults: {},
    };
  }

  static define<const T extends Record<string, FieldDefinition>>(
    fields: T
  ): DefinedBaseData<T> {
    const required: Record<string, FieldType> = {};
    const optional: Record<string, FieldType> = {};
    const defaults: DataRecord = {};

    for (const [key, definition] of Object.entries(fields)) {
      if (Array.isArray(definition)) {
        optional[key] = definition[0] as FieldType;
        if (definition.length === 2) defaults[key] = definition[1];
      } else {
        required[key] = definition as FieldType;
      }
    }

    class Defined extends BaseData {
      static override schema(): BaseDataSchema {
        return { required, optional, defaults };
      }
    }

    Object.defineProperty(Defined, "keys", {
      value: Object.freeze(
        Object.fromEntries(
          Object.keys(fields).map((key) => [key, key])
        )
      ),
    });

    return Defined as unknown as DefinedBaseData<T>;
  }

  // Override to supply environment values in browser applications.
  static environment(): Record<string, string | undefined> {
    const scope = globalThis as typeof globalThis & {
      process?: {
        env?: Record<string, string | undefined>;
      };
    };

    return scope.process?.env ?? {};
  }

  constructor(inData: unknown = {}) {
    if (!isObject(inData)) {
      throw new TypeError("basedata_requires_object");
    }

    const state: DataState = {
      dirty: { ...inData },
      required: {},
      optional: {},
      defaults: {},
      converters: {},
    };

    Object.defineProperty(this, STATE, { value: state });

    const processed = this.do_pre_process({ ...inData });

    if (!isObject(processed)) {
      throw new TypeError("do_pre_process_must_return_object");
    }

    const schema = (this.constructor as typeof BaseData).schema();

    state.required = schema.required ?? {};
    state.optional = schema.optional ?? {};
    state.defaults = this.do_env_mapping(
      schema.defaults ?? {}
    ) as DataRecord;
    state.converters = schema.converters ?? {};

    for (const key of Object.keys(state.required)) {
      if (has(state.optional, key)) {
        throw new Error(`duplicate_key_definition:${key}`);
      }
    }

    const out = this.do_env_mapping(processed) as DataRecord;

    // Validate default declarations, but do not inject yet.
    // Python/C# parity: omitted defaults are applied after validation hooks.
    for (const [key, value] of Object.entries(state.defaults)) {
      if (!has(state.required, key) && !has(state.optional, key)) {
        throw new Error(`default_for_undeclared_key:${key}`);
      }
    }

    // Required fields.
    for (const [key, expected] of Object.entries(state.required)) {
      if (key === "*") continue;

      if (!has(out, key)) {
        throw new TypeError(`missing_required_key:${key}`);
      }

      out[key] = this._validateAndConvert(
        key,
        out[key],
        expected
      );
    }

    // Optional fields.
    for (const [key, expected] of Object.entries(state.optional)) {
      if (key !== "*" && has(out, key)) {
        out[key] = this._validateAndConvert(
          key,
          out[key],
          expected
        );
      }
    }

    // Wildcard validation for undeclared keys.
    const wildcard =
      state.required["*"] ?? state.optional["*"];

    if (wildcard !== undefined) {
      for (const key of Object.keys(out)) {
        if (!has(state.required, key) && !has(state.optional, key)) {
          out[key] = this._validateAndConvert(
            key,
            out[key],
            wildcard
          );
        }
      }
    }

    // Final validation.
    for (const [key, value] of Object.entries(out)) {
      const [next, message] = this.do_every_validation(
        key,
        value
      );

      if (message) {
        throw new Error(message);
      }

      const expected =
        state.required[key] ??
        state.optional[key] ??
        wildcard;

      const normalized = this._normalizeNullishToDeclaredDefault(
        key,
        next
      );
      if (
        expected !== undefined &&
        !this._validTypeOrNullDefault(key, normalized, expected)
      ) {
        throw new TypeError(`invalid_type:${key}`);
      }

      out[key] = normalized;
    }

    // Apply defaults after validation hooks for Python/C# parity.
    for (const [key, value] of Object.entries(state.defaults)) {
      if (!has(out, key) && value !== null) {
        Object.defineProperty(out, key, {
          value,
          writable: true,
          enumerable: true,
          configurable: true,
        });
      }
    }

    // Retain all keys, including undeclared ones.
    for (const [key, value] of Object.entries(out)) {
      Object.defineProperty(this, key, {
        value,
        writable: true,
        enumerable: true,
        configurable: true,
      });
    }

    state.dirty = null;

    // Validated mutation through ordinary property assignment.
    return new Proxy(this, {
      get(target, key, receiver) {
        if (typeof key === "string" && has(target, key)) {
          return target.do_get_filtered(
            key,
            Reflect.get(target, key, receiver)
          );
        }

        return Reflect.get(target, key, receiver);
      },

      set(target, key, value: unknown, receiver) {
        if (typeof key !== "string") {
          return Reflect.set(target, key, value, receiver);
        }

        // Allow subclass accessor setters to forward to this[field].
        let prototype = Object.getPrototypeOf(target) as object | null;

        while (
          prototype &&
          prototype !== BaseData.prototype
        ) {
          const descriptor = Object.getOwnPropertyDescriptor(
            prototype,
            key
          );

          if (descriptor?.set) {
            return Reflect.set(target, key, value, receiver);
          }

          prototype = Object.getPrototypeOf(prototype) as object | null;
        }

        const contract = target[STATE];

        const expected =
          contract.required[key] ??
          contract.optional[key] ??
          contract.required["*"] ??
          contract.optional["*"];

        if (expected !== undefined) {
          value = target._validateAndConvert(
            key,
            value,
            expected
          );
        }

        const [next, message] = target.do_every_validation(
          key,
          value
        );

        if (message) {
          throw new Error(message);
        }

        const normalized = target._normalizeNullishToDeclaredDefault(
          key,
          next
        );
        if (
          expected !== undefined &&
          !target._validTypeOrNullDefault(
            key,
            normalized,
            expected
          )
        ) {
          throw new TypeError(`invalid_type:${key}`);
        }

        Object.defineProperty(target, key, {
          value: normalized,
          writable: true,
          enumerable: true,
          configurable: true,
        });

        return true;
      },

      deleteProperty(target, key) {
        if (
          typeof key === "string" &&
          has(target[STATE].required, key)
        ) {
          throw new Error(`cannot_remove_required_key:${key}`);
        }

        return Reflect.deleteProperty(target, key);
      },
    });
  }

  GetDirtyDict(): DataRecord | null {
    return this[STATE].dirty;
  }

  do_pre_process(inData: DataRecord): DataRecord {
    return inData;
  }

  do_validation(
    _key: string,
    value: unknown
  ): [unknown, string] {
    return [value, ""];
  }

  do_every_validation(
    _key: string,
    value: unknown
  ): [unknown, string] {
    return [value, ""];
  }

  do_get_filtered(
    _key: string,
    value: unknown
  ): unknown {
    return value;
  }

  do_env_mapping(value: unknown): unknown {
    if (value instanceof BaseData) {
      return value;
    }

    if (Array.isArray(value)) {
      return value.map((item: unknown) =>
        this.do_env_mapping(item)
      );
    }

    if (isObject(value)) {
      const out: DataRecord = {};

      for (const [key, entry] of Object.entries(value)) {
        Object.defineProperty(out, key, {
          value: this.do_env_mapping(entry),
          writable: true,
          enumerable: true,
          configurable: true,
        });
      }

      return out;
    }

    if (typeof value === "string") {
      return value.replace(
        /<<([A-Z_][A-Z0-9_]*)>>/g,
        (_match, name: string) => {
          const environment = (
            this.constructor as typeof BaseData
          ).environment();

          if (
            !has(environment, name) ||
            environment[name] == null
          ) {
            throw new Error(
              `Missing required environment variable: '${name}' for '${this.constructor.name}'`
            );
          }

          return String(environment[name]);
        }
      );
    }

    return value;
  }

  private _validateAndConvert(
    key: string,
    value: unknown,
    expected: FieldType
  ): unknown {
    const converter = this[STATE].converters[key];

    if (converter !== undefined) {
      value = converter(value);
    }

    value = this._convert(value, expected);

    const [next, message] = this.do_validation(
      key,
      value
    );

    if (message) {
      throw new Error(message);
    }

    const normalized = this._normalizeNullishToDeclaredDefault(
      key,
      next
    );

    if (
      !this._validTypeOrNullDefault(
        key,
        normalized,
        expected
      )
    ) {
      throw new TypeError(`invalid_type:${key}`);
    }

    return normalized;
  }

  private _convert(
    value: unknown,
    expected: FieldType
  ): unknown {
    // Recursive BaseData construction.
    if (isBaseDataConstructor(expected)) {
      if (value instanceof expected) {
        return value;
      }

      if (isObject(value)) {
        return new expected(value);
      }

      return value;
    }

    if (
      typeof expected === "object" &&
      expected !== null
    ) {
      // Typed lists.
      if ("listOf" in expected && Array.isArray(value)) {
        return value.map((item: unknown) =>
          this._convert(item, expected.listOf)
        );
      }

      // Typed dictionaries.
      if ("mapOf" in expected && isObject(value)) {
        return Object.fromEntries(
          Object.entries(value).map(([key, item]) => [
            key,
            this._convert(item, expected.mapOf),
          ])
        );
      }
    }

    return value;
  }

  private _normalizeNullishToDeclaredDefault(
    key: string,
    value: unknown
  ): unknown {
    if (
      (value === null || value === undefined) &&
      has(this[STATE].defaults, key) &&
      (
        this[STATE].defaults[key] === null ||
        this[STATE].defaults[key] === undefined
      )
    ) {
      return this[STATE].defaults[key];
    }
    return value;
  }

  private _validTypeOrNullDefault(
    key: string,
    value: unknown,
    expected: FieldType
  ): boolean {
    if ((
      value === null || value === undefined
    ) &&
      has(this[STATE].defaults, key) &&
      (this[STATE].defaults[key] === null ||
        this[STATE].defaults[key] === undefined)
    ) {
      return true;
    }

    return this._validType(value, expected);
  }

  private _validType(
    value: unknown,
    expected: FieldType
  ): boolean {
    if (expected === "any") {
      return true;
    }

    if (expected === "string" || expected === String) {
      return typeof value === "string";
    }

    if (expected === "number" || expected === Number) {
      return (
        typeof value === "number" &&
        Number.isFinite(value)
      );
    }

    if (expected === "integer") {
      return (
        typeof value === "number" &&
        Number.isInteger(value)
      );
    }

    if (expected === "boolean" || expected === Boolean) {
      return typeof value === "boolean";
    }

    if (expected === "object" || expected === Object) {
      return isObject(value);
    }

    if (expected === "array" || expected === Array) {
      return Array.isArray(value);
    }

    if (typeof expected === "function") {
      return value instanceof expected;
    }

    if (
      typeof expected === "object" &&
      expected !== null
    ) {
      if ("listOf" in expected) {
        return (
          Array.isArray(value) &&
          value.every((item: unknown) =>
            this._validType(item, expected.listOf)
          )
        );
      }

      if ("mapOf" in expected) {
        return (
          isObject(value) &&
          Object.values(value).every((item: unknown) =>
            this._validType(item, expected.mapOf)
          )
        );
      }
    }

    return false;
  }

  set(key: string, value: unknown): boolean {
    this[key] = value;
    return true;
  }

  clean(): this {
    const { required, optional } = this[STATE];

    const out: DataRecord = {};

    for (const key of [
      ...Object.keys(required),
      ...Object.keys(optional),
    ]) {
      if (key !== "*" && has(this, key)) {
        out[key] = this[key];
      }
    }

    const Constructor = this.constructor as BaseDataConstructor;

    return new Constructor(out) as this;
  }

  private static _cloneStore(
    store: Record<string, DataRecord>
  ): Record<string, DataRecord> {
    return Object.fromEntries(
      Object.entries(store).map(([key, record]) => [
        key,
        { ...record },
      ])
    );
  }

  static DiagnosticPrint(
    title: unknown,
    index: unknown,
    start_id: unknown,
    end_id: unknown,
    description_of_location: unknown,
    interesting_conditionals: unknown,
    repeat_delay: number | null | undefined
  ): boolean {
    const key = String(index);
    const nowTs = Date.now() / 1000;

    let delay = repeat_delay;
    if (delay !== null && delay !== undefined) {
      if (
        typeof delay !== "number" ||
        !Number.isFinite(delay) ||
        (delay < 0 && delay !== -1)
      ) {
        throw new Error("invalid_repeat_delay");
      }
    }

    const existing = BaseData._diagnostics[key];
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

    if (!allowed) {
      return false;
    }

    const record: DataRecord = {
      title,
      index,
      start_id,
      end_id,
      description_of_location,
      interesting_conditionals,
      timestamp: nowTs,
    };
    console.log(record);
    BaseData._diagnostics[key] = record;
    if (start_id !== null && start_id !== undefined) {
      BaseData._diagnosticStarts[key] = { ...record };
    }
    if (end_id !== null && end_id !== undefined) {
      BaseData._diagnosticEnds[key] = { ...record };
    }
    return true;
  }

  static DiagnosticDump(): {
    diagnostics: Record<string, DataRecord>;
    starts: Record<string, DataRecord>;
    ends: Record<string, DataRecord>;
  } {
    return {
      diagnostics: BaseData._cloneStore(BaseData._diagnostics),
      starts: BaseData._cloneStore(BaseData._diagnosticStarts),
      ends: BaseData._cloneStore(BaseData._diagnosticEnds),
    };
  }
}
/**
 * 
import {
  BaseData,
  type BaseDataSchema,
} from "./BaseData";

class Child extends BaseData {
  declare value: number;

  static override schema(): BaseDataSchema {
    return {
      required: {
        value: "integer",
      },
      optional: {},
    };
  }
}

class Parent extends BaseData {
  declare name: string;
  declare child: Child;
  declare children: Child[];
  declare retries: number;

  static override schema(): BaseDataSchema {
    return {
      required: {
        name: "string",
        child: Child,
        children: { listOf: Child },
      },
      optional: {
        retries: "integer",
      },
      defaults: {
        retries: 3,
      },
    };
  }
}

const parent = new Parent({
  name: "example",
  child: { value: 1 },
  children: [{ value: 2 }, { value: 3 }],
  undeclared: "preserved",
});

console.log(parent.name);       // "example"
console.log(parent.child.value); // 1
console.log(parent.retries);    // 3
console.log(parent.undeclared); // "preserved"

parent.retries = 5;             // Validated mutation
parent.retries = "invalid";     // TypeScript error

 * 
 * 
 */
