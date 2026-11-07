import assert from "node:assert/strict";
import test from "node:test";

import { BaseData, type DataRecord } from "../../../clients/typescript/BaseData.ts";

test("required_fields_and_type_validation", () => {
  class RequiredSpec extends BaseData.define({
    name: "string",
    count: "integer",
  }) {}

  const item = new RequiredSpec({ name: "alpha", count: 2 });
  assert.equal(item.name, "alpha");
  assert.equal(item.count, 2);

  assert.throws(
    () => new RequiredSpec({ name: "alpha" }),
    (error: unknown) =>
      error instanceof TypeError && error.message.includes("missing_required_key:count")
  );

  assert.throws(
    () => new RequiredSpec({ name: "alpha", count: "2" as unknown as number }),
    (error: unknown) =>
      error instanceof TypeError && error.message.includes("invalid_type:count")
  );
});

test("do_pre_process_and_do_validation", () => {
  class NormalizedTag extends BaseData.define({
    tag: "string",
  }) {
    override do_pre_process(input: DataRecord): DataRecord {
      const out = { ...input };
      const raw = out[NormalizedTag.keys.tag];
      if (typeof raw === "string") out[NormalizedTag.keys.tag] = raw.trim();
      return out;
    }

    override do_validation(key: string, value: unknown): [unknown, string] {
      if (key === NormalizedTag.keys.tag && value === "") {
        throw new Error("tag_required");
      }
      return [value, ""];
    }
  }

  const cleaned = new NormalizedTag({ tag: "  hello  " });
  assert.equal(cleaned.tag, "hello");

  assert.throws(
    () => new NormalizedTag({ tag: "   " }),
    (error: unknown) => error instanceof Error && error.message.includes("tag_required")
  );
});

test("preserves_undeclared_input_keys", () => {
  class DeclaredOnly extends BaseData.define({
    title: "string",
  }) {}

  const payload = {
    title: "doc",
    extra_flag: true,
    metadata: { author: "test" },
  };
  const item = new DeclaredOnly(payload);

  assert.equal(item.title, "doc");
  assert.equal(item["extra_flag"], true);
  assert.equal((item["metadata"] as DataRecord).author, "test");
});

test("combinatorial_contracts_share_full_input", () => {
  class Data1 extends BaseData.define({
    a: "string",
    b: "string",
  }) {}

  class Data2 extends BaseData.define({
    b: "string",
    c: "string",
  }) {}

  const payload = {
    a: "one",
    b: "two",
    c: "three",
    undeclared: "kept",
  };
  const data1 = new Data1(payload);
  const data2 = new Data2(payload);

  assert.equal(data1.a, "one");
  assert.equal(data1.b, "two");
  assert.equal(data2.b, "two");
  assert.equal(data2.c, "three");

  assert.equal(data1[Data2.keys.c], "three");
  assert.equal(data2[Data1.keys.a], "one");
  assert.equal(data1.undeclared, "kept");
  assert.equal(data2.undeclared, "kept");
});

test("contract_defined_defaults", () => {
  class DefaultsSpec extends BaseData.define({
    name: "string",
    timeout_s: ["number", 1.5],
    retries: ["integer", 3],
  }) {}

  const defaults = new DefaultsSpec({ name: "job" });
  assert.equal(defaults.timeout_s, 1.5);
  assert.equal(defaults.retries, 3);

  const overridden = new DefaultsSpec({ name: "job", retries: 9 });
  assert.equal(overridden.timeout_s, 1.5);
  assert.equal(overridden.retries, 9);
});

test("nullish_default_type_compatibility", () => {
  class NullishDefaultsSpec extends BaseData.define({
    null_default: ["string", null],
    undefined_default: ["string", undefined],
  }) {}

  const item = new NullishDefaultsSpec({
    null_default: "alpha",
    undefined_default: "beta",
  });
  const cross_ctor = new NullishDefaultsSpec({
    null_default: undefined,
    undefined_default: null,
  });

  // Constructor-time cross assignments:
  // null-default field accepts undefined and normalizes to null.
  assert.equal(cross_ctor.null_default, null);
  // undefined-default field accepts null and normalizes to undefined.
  assert.equal(cross_ctor.undefined_default, undefined);

  item.null_default = undefined;
  assert.equal(item.null_default, null);

  item.undefined_default = null;
  assert.equal(item.undefined_default, undefined);

  item.null_default = null;
  assert.equal(item.null_default, null);

  item.undefined_default = undefined;
  assert.equal(item.undefined_default, undefined);

  class RequiredStringSpec extends BaseData.define({
    required_name: "string",
  }) {}

  const required = new RequiredStringSpec({ required_name: "ok" });

  assert.throws(
    () => {
      required.required_name = undefined as unknown as string;
    },
    (error: unknown) =>
      error instanceof TypeError && error.message.includes("invalid_type:required_name")
  );

  assert.throws(
    () => {
      required.required_name = null as unknown as string;
    },
    (error: unknown) =>
      error instanceof TypeError && error.message.includes("invalid_type:required_name")
  );
});

test("stateful_dictionary_mutation", () => {
  class MutableSpec extends BaseData.define({
    label: "string",
    qty: ["integer", 0],
  }) {}

  const item = new MutableSpec({ label: "seed", note: "keep" });
  item["qty"] = 5;
  item["new_blob"] = { steps: [1, 2, 3] };

  assert.equal(item["label"], "seed");
  assert.equal(item["qty"], 5);
  assert.equal(item["note"], "keep");
  assert.deepEqual((item["new_blob"] as DataRecord).steps, [1, 2, 3]);

  assert.throws(
    () => {
      item["qty"] = "bad";
    },
    (error: unknown) =>
      error instanceof TypeError && error.message.includes("invalid_type:qty")
  );
});

test("recursive_nested_basedata_validation_and_error_propagation", () => {
  class Child extends BaseData.define({
    value: "integer",
  }) {
    override do_validation(key: string, value: unknown): [unknown, string] {
      if (key === Child.keys.value && (value as number) < 0) {
        throw new Error("value_must_be_non_negative");
      }
      return [value, ""];
    }
  }

  class Parent extends BaseData.define({
    child: Child,
    children: { listOf: Child },
  }) {}

  const parent = new Parent({
    child: { value: 1 },
    children: [{ value: 2 }, { value: 3 }],
    extra: "preserved",
  });

  assert.equal(parent.child instanceof Child, true);
  assert.equal(Array.isArray(parent.children), true);
  assert.equal(parent.children[0] instanceof Child, true);
  assert.equal(parent.child.value, 1);
  assert.equal(parent.children[1].value, 3);
  assert.equal(parent.extra, "preserved");

  assert.throws(
    () => new Parent({ child: { value: -1 }, children: [] }),
    (error: unknown) =>
      error instanceof Error && error.message.includes("value_must_be_non_negative")
  );
});

test("optional_fields_shorthand_and_override_forms", () => {
  class ShortOptionalSpec extends BaseData.define({
    tag: ["string"],
  }) {
    static validation_calls = 0;

    override do_validation(key: string, value: unknown): [unknown, string] {
      if (key === ShortOptionalSpec.keys.tag) {
        ShortOptionalSpec.validation_calls += 1;
        if (value === "bad") throw new Error("tag_invalid");
      }
      return [value, ""];
    }
  }

  class LongOptionalSpec extends BaseData {
    static validation_calls = 0;
    static readonly tag = "tag";

    static override schema() {
      return {
        required: {},
        optional: { [LongOptionalSpec.tag]: "string" as const },
      };
    }

    override do_validation(key: string, value: unknown): [unknown, string] {
      if (key === LongOptionalSpec.tag) {
        LongOptionalSpec.validation_calls += 1;
        if (value === "bad") throw new Error("tag_invalid");
      }
      return [value, ""];
    }
  }

  ShortOptionalSpec.validation_calls = 0;
  const shortMissing = new ShortOptionalSpec({});
  assert.equal(shortMissing.tag, undefined);
  assert.equal(ShortOptionalSpec.validation_calls, 0);

  const shortPresent = new ShortOptionalSpec({ tag: "ok" });
  assert.equal(shortPresent.tag, "ok");
  assert.equal(ShortOptionalSpec.validation_calls, 1);

  assert.throws(
    () => new ShortOptionalSpec({ tag: "bad" }),
    (error: unknown) => error instanceof Error && error.message.includes("tag_invalid")
  );

  LongOptionalSpec.validation_calls = 0;
  const longMissing = new LongOptionalSpec({});
  assert.equal(Object.prototype.hasOwnProperty.call(longMissing, "tag"), false);
  assert.equal(LongOptionalSpec.validation_calls, 0);

  const longPresent = new LongOptionalSpec({ tag: "ok" });
  assert.equal(longPresent["tag"], "ok");
  assert.equal(LongOptionalSpec.validation_calls, 1);

  assert.throws(
    () => new LongOptionalSpec({ tag: "bad" }),
    (error: unknown) => error instanceof Error && error.message.includes("tag_invalid")
  );
});

test("required_fields_shorthand_and_override_forms", () => {
  class ShortRequiredSpec extends BaseData.define({
    name: "string",
  }) {
    static validation_calls = 0;

    override do_validation(key: string, value: unknown): [unknown, string] {
      if (key === ShortRequiredSpec.keys.name) {
        ShortRequiredSpec.validation_calls += 1;
        if (value === "") throw new Error("name_required");
      }
      return [value, ""];
    }
  }

  class LongRequiredSpec extends BaseData {
    static validation_calls = 0;
    static readonly name = "name";

    static override schema() {
      return {
        required: { [LongRequiredSpec.name]: "string" as const },
        optional: {},
      };
    }

    override do_validation(key: string, value: unknown): [unknown, string] {
      if (key === LongRequiredSpec.name) {
        LongRequiredSpec.validation_calls += 1;
        if (value === "") throw new Error("name_required");
      }
      return [value, ""];
    }
  }

  ShortRequiredSpec.validation_calls = 0;
  const shortOk = new ShortRequiredSpec({ name: "alpha" });
  assert.equal(shortOk.name, "alpha");
  assert.equal(ShortRequiredSpec.validation_calls, 1);

  assert.throws(
    () => new ShortRequiredSpec({}),
    (error: unknown) =>
      error instanceof TypeError && error.message.includes("missing_required_key:name")
  );

  assert.throws(
    () => new ShortRequiredSpec({ name: "" }),
    (error: unknown) => error instanceof Error && error.message.includes("name_required")
  );

  LongRequiredSpec.validation_calls = 0;
  const longOk = new LongRequiredSpec({ name: "alpha" });
  assert.equal(longOk["name"], "alpha");
  assert.equal(LongRequiredSpec.validation_calls, 1);

  assert.throws(
    () => new LongRequiredSpec({}),
    (error: unknown) =>
      error instanceof TypeError && error.message.includes("missing_required_key:name")
  );

  assert.throws(
    () => new LongRequiredSpec({ name: "" }),
    (error: unknown) => error instanceof Error && error.message.includes("name_required")
  );
});

test("defaults_shorthand_and_override_forms", () => {
  class ShortDefaultsSpec extends BaseData.define({
    retries: ["integer", 3],
  }) {
    static validation_calls = 0;

    override do_validation(key: string, value: unknown): [unknown, string] {
      if (key === ShortDefaultsSpec.keys.retries) {
        ShortDefaultsSpec.validation_calls += 1;
        if ((value as number) <= 0) throw new Error("retries_must_be_positive");
      }
      return [value, ""];
    }
  }

  class LongDefaultsSpec extends BaseData {
    static validation_calls = 0;
    static readonly retries = "retries";

    static override schema() {
      return {
        required: {},
        optional: { [LongDefaultsSpec.retries]: "integer" as const },
        defaults: { [LongDefaultsSpec.retries]: 3 },
      };
    }

    override do_validation(key: string, value: unknown): [unknown, string] {
      if (key === LongDefaultsSpec.retries) {
        LongDefaultsSpec.validation_calls += 1;
        if ((value as number) <= 0) throw new Error("retries_must_be_positive");
      }
      return [value, ""];
    }
  }

  ShortDefaultsSpec.validation_calls = 0;
  const shortDefaulted = new ShortDefaultsSpec({});
  assert.equal(shortDefaulted.retries, 3);
  // Python parity expectation: omitted defaults do not trigger do_validation.
  assert.equal(ShortDefaultsSpec.validation_calls, 0);

  const shortOverridden = new ShortDefaultsSpec({ retries: 7 });
  assert.equal(shortOverridden.retries, 7);
  assert.equal(ShortDefaultsSpec.validation_calls, 1);

  assert.throws(
    () => new ShortDefaultsSpec({ retries: 0 }),
    (error: unknown) =>
      error instanceof Error && error.message.includes("retries_must_be_positive")
  );

  LongDefaultsSpec.validation_calls = 0;
  const longDefaulted = new LongDefaultsSpec({});
  assert.equal(longDefaulted["retries"], 3);
  // Python parity expectation: omitted defaults do not trigger do_validation.
  assert.equal(LongDefaultsSpec.validation_calls, 0);

  const longOverridden = new LongDefaultsSpec({ retries: 7 });
  assert.equal(longOverridden["retries"], 7);
  assert.equal(LongDefaultsSpec.validation_calls, 1);

  assert.throws(
    () => new LongDefaultsSpec({ retries: 0 }),
    (error: unknown) =>
      error instanceof Error && error.message.includes("retries_must_be_positive")
  );
});

test("static_diagnostic_print_and_dump_contract", () => {
  const unique = Math.random().toString(36).slice(2);

  const idx = `diag_ts_delay_${unique}`;
  assert.equal(
    BaseData.DiagnosticPrint("t", idx, null, null, "loc", { a: 1 }, 2),
    true
  );
  const dumpA = BaseData.DiagnosticDump();
  const tsA = Number(dumpA.diagnostics[idx].timestamp);

  assert.equal(
    BaseData.DiagnosticPrint("t", idx, null, null, "loc", { a: 2 }, 2),
    false
  );
  const dumpB = BaseData.DiagnosticDump();
  assert.equal(Number(dumpB.diagnostics[idx].timestamp), tsA);

  const onceIdx = `diag_ts_once_${unique}`;
  assert.equal(
    BaseData.DiagnosticPrint("t", onceIdx, null, null, "loc", {}, -1),
    true
  );
  assert.equal(
    BaseData.DiagnosticPrint("t", onceIdx, null, null, "loc", {}, -1),
    false
  );

  const freeIdx = `diag_ts_free_${unique}`;
  assert.equal(
    BaseData.DiagnosticPrint("t", freeIdx, null, null, "loc", {}, null),
    true
  );
  assert.equal(
    BaseData.DiagnosticPrint("t", freeIdx, null, null, "loc", {}, 0),
    true
  );

  const startIdx = `diag_ts_start_${unique}`;
  const endIdx = `diag_ts_end_${unique}`;
  assert.equal(
    BaseData.DiagnosticPrint("t", startIdx, "s1", null, "loc", {}, 0),
    true
  );
  assert.equal(
    BaseData.DiagnosticPrint("t", endIdx, null, "e1", "loc", {}, 0),
    true
  );
  const dumpC = BaseData.DiagnosticDump();
  assert.equal(Object.prototype.hasOwnProperty.call(dumpC.starts, startIdx), true);
  assert.equal(Object.prototype.hasOwnProperty.call(dumpC.ends, endIdx), true);

  const beforeCount = Object.keys(BaseData.DiagnosticDump().diagnostics).length;
  const boundedIdx = `diag_ts_bounded_${unique}`;
  assert.equal(
    BaseData.DiagnosticPrint("t", boundedIdx, null, null, "loc", {}, 0),
    true
  );
  const midCount = Object.keys(BaseData.DiagnosticDump().diagnostics).length;
  assert.equal(
    BaseData.DiagnosticPrint("t", boundedIdx, null, null, "loc", {}, 0),
    true
  );
  const afterCount = Object.keys(BaseData.DiagnosticDump().diagnostics).length;
  assert.equal(midCount, beforeCount + 1);
  assert.equal(afterCount, midCount);

  assert.throws(
    () => BaseData.DiagnosticPrint("t", "bad_delay", null, null, "loc", {}, -2),
    (error: unknown) =>
      error instanceof Error && error.message.includes("invalid_repeat_delay")
  );
});
