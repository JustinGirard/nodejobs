import sys
import uuid
from pathlib import Path

import pytest

# Ensure repo root is importable when this file is executed directly.
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from nodejobs.dependencies.BaseData import BaseData


def test_required_fields_and_type_validation():
    class RequiredSpec(BaseData):
        name: str
        count: int

    item = RequiredSpec({"name": "alpha", "count": 2})
    assert item["name"] == "alpha"
    assert item["count"] == 2

    with pytest.raises(TypeError, match="do_validation did not pass"):
        RequiredSpec({"name": "alpha"})

    with pytest.raises(TypeError, match="Expected type"):
        RequiredSpec({"name": "alpha", "count": "2"})


def test_do_pre_process_and_do_validation():
    class NormalizedTag(BaseData):
        tag: str

        def do_pre_process(self, in_dict):
            out = dict(in_dict)
            if "tag" in out and isinstance(out["tag"], str):
                out["tag"] = out["tag"].strip()
            return out

        def do_validation(self, key, value):
            if key == "tag" and value == "":
                raise ValueError("tag_required")
            return value, ""

    cleaned = NormalizedTag({"tag": "  hello  "})
    assert cleaned.tag == "hello"

    with pytest.raises(ValueError, match="tag_required"):
        NormalizedTag({"tag": "   "})


def test_preserves_undeclared_input_keys():
    class DeclaredOnly(BaseData):
        title: str

    payload = {
        "title": "doc",
        "extra_flag": True,
        "metadata": {"author": "test"},
    }
    item = DeclaredOnly(payload)
    assert item.title == "doc"
    assert item["extra_flag"] is True
    assert item["metadata"]["author"] == "test"


def test_combinatorial_contracts_share_full_input():
    class Data1(BaseData):
        a: str
        b: str

    class Data2(BaseData):
        b: str
        c: str

    payload = {
        "a": "one",
        "b": "two",
        "c": "three",
        "undeclared": "kept",
    }
    data1 = Data1(payload)
    data2 = Data2(payload)

    assert data1.a == "one"
    assert data1.b == "two"
    assert data2.b == "two"
    assert data2.c == "three"

    assert data1["c"] == "three"
    assert data2["a"] == "one"
    assert data1["undeclared"] == "kept"
    assert data2["undeclared"] == "kept"


def test_contract_defined_defaults():
    class DefaultsSpec(BaseData):
        name: str
        timeout_s: (float, 1.5)
        retries: (int, 3)

    defaults = DefaultsSpec({"name": "job"})
    assert defaults["timeout_s"] == 1.5
    assert defaults["retries"] == 3

    overridden = DefaultsSpec({"name": "job", "retries": 9})
    assert overridden["timeout_s"] == 1.5
    assert overridden["retries"] == 9


def test_stateful_dictionary_mutation():
    class MutableSpec(BaseData):
        label: str
        qty: (int, 0)

    item = MutableSpec({"label": "seed", "note": "keep"})
    item["qty"] = 5
    item["new_blob"] = {"steps": [1, 2, 3]}

    assert item["label"] == "seed"
    assert item["qty"] == 5
    assert item["note"] == "keep"
    assert item["new_blob"]["steps"] == [1, 2, 3]

    with pytest.raises(TypeError, match="Expected type"):
        item["qty"] = "bad"


def test_recursive_nested_basedata_validation_and_error_propagation():
    class Child(BaseData):
        value: int

        def do_validation(self, key, value):
            if key == "value" and value < 0:
                raise ValueError("value_must_be_non_negative")
            return value, ""

    class Parent(BaseData):
        child: Child
        children: list[Child]

    parent = Parent(
        {
            "child": {"value": 1},
            "children": [{"value": 2}, {"value": 3}],
            "extra": "preserved",
        }
    )

    assert isinstance(parent["child"], Child)
    assert isinstance(parent["children"][0], Child)
    assert parent["child"]["value"] == 1
    assert parent["children"][1]["value"] == 3
    assert parent["extra"] == "preserved"

    with pytest.raises(ValueError, match="value_must_be_non_negative"):
        Parent({"child": {"value": -1}, "children": []})


def test_optional_fields_shorthand_and_override_forms():
    class ShortOptionalSpec(BaseData):
        tag: (str, None)
        validation_calls = 0

        def do_validation(self, key, value):
            if key == ShortOptionalSpec.tag:
                ShortOptionalSpec.validation_calls += 1
                if value == "bad":
                    raise ValueError("tag_invalid")
            return value, ""

    class LongOptionalSpec(BaseData):
        validation_calls = 0

        def get_keys(self):
            return {}, {"tag": str}

        def do_validation(self, key, value):
            if key == "tag":
                LongOptionalSpec.validation_calls += 1
                if value == "bad":
                    raise ValueError("tag_invalid")
            return value, ""

    short_missing = ShortOptionalSpec({})
    assert short_missing.tag is None
    assert ShortOptionalSpec.validation_calls == 0

    short_present = ShortOptionalSpec({"tag": "ok"})
    assert short_present["tag"] == "ok"
    assert ShortOptionalSpec.validation_calls == 1

    with pytest.raises(ValueError, match="tag_invalid"):
        ShortOptionalSpec({"tag": "bad"})

    long_missing = LongOptionalSpec({})
    assert "tag" not in long_missing
    assert LongOptionalSpec.validation_calls == 0

    long_present = LongOptionalSpec({"tag": "ok"})
    assert long_present["tag"] == "ok"
    assert LongOptionalSpec.validation_calls == 1

    with pytest.raises(ValueError, match="tag_invalid"):
        LongOptionalSpec({"tag": "bad"})


def test_required_fields_shorthand_and_override_forms():
    class ShortRequiredSpec(BaseData):
        name: str
        validation_calls = 0

        def do_validation(self, key, value):
            if key == ShortRequiredSpec.name:
                ShortRequiredSpec.validation_calls += 1
                if value == "":
                    raise ValueError("name_required")
            return value, ""

    class LongRequiredSpec(BaseData):
        validation_calls = 0

        def get_keys(self):
            return {"name": str}, {}

        def do_validation(self, key, value):
            if key == "name":
                LongRequiredSpec.validation_calls += 1
                if value == "":
                    raise ValueError("name_required")
            return value, ""

    short_ok = ShortRequiredSpec({"name": "alpha"})
    assert short_ok.name == "alpha"
    assert ShortRequiredSpec.validation_calls == 1

    with pytest.raises(TypeError, match="do_validation did not pass"):
        ShortRequiredSpec({})

    with pytest.raises(ValueError, match="name_required"):
        ShortRequiredSpec({"name": ""})

    long_ok = LongRequiredSpec({"name": "alpha"})
    assert long_ok["name"] == "alpha"
    assert LongRequiredSpec.validation_calls == 1

    with pytest.raises(TypeError, match="do_validation did not pass"):
        LongRequiredSpec({})

    with pytest.raises(ValueError, match="name_required"):
        LongRequiredSpec({"name": ""})


def test_defaults_shorthand_and_override_forms():
    class ShortDefaultsSpec(BaseData):
        retries: (int, 3)
        validation_calls = 0

        def do_validation(self, key, value):
            if key == ShortDefaultsSpec.retries:
                ShortDefaultsSpec.validation_calls += 1
                if value <= 0:
                    raise ValueError("retries_must_be_positive")
            return value, ""

    class LongDefaultsSpec(BaseData):
        validation_calls = 0

        def get_keys(self):
            return {}, {"retries": int}

        def get_defaults(self):
            return {"retries": 3}

        def do_validation(self, key, value):
            if key == "retries":
                LongDefaultsSpec.validation_calls += 1
                if value <= 0:
                    raise ValueError("retries_must_be_positive")
            return value, ""

    short_defaulted = ShortDefaultsSpec({})
    assert short_defaulted["retries"] == 3
    assert ShortDefaultsSpec.validation_calls == 0

    short_overridden = ShortDefaultsSpec({"retries": 7})
    assert short_overridden["retries"] == 7
    assert ShortDefaultsSpec.validation_calls == 1

    with pytest.raises(ValueError, match="retries_must_be_positive"):
        ShortDefaultsSpec({"retries": 0})

    long_defaulted = LongDefaultsSpec({})
    assert long_defaulted["retries"] == 3
    assert LongDefaultsSpec.validation_calls == 0

    long_overridden = LongDefaultsSpec({"retries": 7})
    assert long_overridden["retries"] == 7
    assert LongDefaultsSpec.validation_calls == 1

    with pytest.raises(ValueError, match="retries_must_be_positive"):
        LongDefaultsSpec({"retries": 0})


def test_null_default_accepts_explicit_none_assignment():
    class NullDefaultSpec(BaseData):
        note: (str, None)

    explicit_null = NullDefaultSpec({"note": None})
    assert explicit_null.note is None
    assert explicit_null[NullDefaultSpec.note] is None
    assert NullDefaultSpec.note in explicit_null

    # Python equivalent of undefined<->null uses key absence <-> None.
    missing = NullDefaultSpec({})
    assert missing.note is None
    assert NullDefaultSpec.note not in missing

    missing.note = None
    assert missing.note is None
    assert NullDefaultSpec.note in missing

    del missing[NullDefaultSpec.note]
    assert NullDefaultSpec.note not in missing
    assert missing.note is None


def test_static_diagnostic_print_and_dump_contract(monkeypatch):
    clock = {"now": 1000.0}
    monkeypatch.setattr("nodejobs.dependencies.BaseData.time.time", lambda: clock["now"])

    idx = f"diag_py_delay_{uuid.uuid4().hex}"
    assert BaseData.DiagnosticPrint("t", idx, None, None, "loc", {"a": 1}, 2.0) is True
    dump_a = BaseData.DiagnosticDump()
    ts_a = dump_a["diagnostics"][idx]["timestamp"]

    assert BaseData.DiagnosticPrint("t", idx, None, None, "loc", {"a": 2}, 2.0) is False
    dump_b = BaseData.DiagnosticDump()
    assert dump_b["diagnostics"][idx]["timestamp"] == ts_a

    clock["now"] = 1002.0
    assert BaseData.DiagnosticPrint("t", idx, None, None, "loc", {"a": 3}, 2.0) is True
    dump_c = BaseData.DiagnosticDump()
    assert dump_c["diagnostics"][idx]["timestamp"] == 1002.0

    once_idx = f"diag_py_once_{uuid.uuid4().hex}"
    assert BaseData.DiagnosticPrint("t", once_idx, None, None, "loc", {}, -1) is True
    assert BaseData.DiagnosticPrint("t", once_idx, None, None, "loc", {}, -1) is False

    free_idx = f"diag_py_free_{uuid.uuid4().hex}"
    assert BaseData.DiagnosticPrint("t", free_idx, None, None, "loc", {}, None) is True
    assert BaseData.DiagnosticPrint("t", free_idx, None, None, "loc", {}, 0) is True

    start_idx = f"diag_py_start_{uuid.uuid4().hex}"
    end_idx = f"diag_py_end_{uuid.uuid4().hex}"
    assert BaseData.DiagnosticPrint("t", start_idx, "s1", None, "loc", {}, 0) is True
    assert BaseData.DiagnosticPrint("t", end_idx, None, "e1", "loc", {}, 0) is True
    dump_d = BaseData.DiagnosticDump()
    assert start_idx in dump_d["starts"]
    assert end_idx in dump_d["ends"]

    before_count = len(BaseData.DiagnosticDump()["diagnostics"])
    bounded_idx = f"diag_py_bounded_{uuid.uuid4().hex}"
    assert BaseData.DiagnosticPrint("t", bounded_idx, None, None, "loc", {}, 0) is True
    mid_count = len(BaseData.DiagnosticDump()["diagnostics"])
    assert BaseData.DiagnosticPrint("t", bounded_idx, None, None, "loc", {}, 0) is True
    after_count = len(BaseData.DiagnosticDump()["diagnostics"])
    assert mid_count == before_count + 1
    assert after_count == mid_count

    with pytest.raises(ValueError, match="invalid_repeat_delay"):
        BaseData.DiagnosticPrint("t", "bad_delay", None, None, "loc", {}, -2)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
