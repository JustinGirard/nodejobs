import os
import shutil
import unittest
from typing import Tuple, Dict, Any, cast

from nodejobs.dependencies.BaseSession import DataSession
from nodejobs.dependencies.field_set import FieldSet

BASE_TMP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fieldset_tests")
THIS_DIR = os.path.dirname(os.path.abspath(__file__))


def fresh_root_and_session(test_name: str) -> Tuple[str, DataSession]:
    root = os.path.join(BASE_TMP, test_name)
    if os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root, exist_ok=True)
    session = DataSession({"root": root, DataSession.f_unlocked: True})
    return root, session


class FieldSetTests(unittest.TestCase):
    def _new_comp(self, session: DataSession, cache_path: str = "./cache1") -> FieldSet:
        return FieldSet({"data_session": session, "cache_path": cache_path})

    def _assert_no_error(self, res: Any) -> None:
        if isinstance(res, dict) and "error" in res:
            self.fail(res["error"])

    def test_creating_fields_roundtrip(self):
        """
        creating fields results in getting the fields having the same list
        (i.e., schema roundtrip with normalized type dicts)
        """
        root, session = fresh_root_and_session("test_creating_fields_roundtrip")
        comp = self._new_comp(session)

        input_fields: Dict[str, Any] = {
            "name": {"type": "str"},
            "age": {"type": "int"},
        }

        res_after_set = comp.set_fields(input_fields)
        self._assert_no_error(res_after_set)

        fields = comp.get_fields()
        self.assertIsInstance(fields, dict)
        self.assertNotIn("error", fields)

        expected_fields = {
            "name": {"type": "str", "arg_format": "-<{argname}>=<{value}>"},
            "age": {"type": "int", "arg_format": "-<{argname}>=<{value}>"},
            "_in_json": {"type": "json_list", "optional": True},
        }
        self.assertEqual(expected_fields, fields)

    def test_setting_values_with_no_fields_works(self):
        """
        setting values with no set fields works
        """
        root, session = fresh_root_and_session(
            "test_setting_values_with_no_fields_works"
        )
        comp = self._new_comp(session)

        res = comp.set_data(foo="bar", num=123)  # type: ignore[arg-type]
        res = cast(Dict[str, Any], res)
        self.assertIsInstance(res, dict)
        self.assertNotIn("error", res)

        self.assertEqual(res["foo"], "bar")
        self.assertEqual(res["num"], 123)

    def test_setting_values_with_incompatible_types_fails(self):
        """
        setting values with incompatible types fails
        """
        root, session = fresh_root_and_session(
            "test_setting_values_with_incompatible_types_fails"
        )
        comp = self._new_comp(session)

        # Define schema: age must be int
        res_fields = comp.set_fields({"age": {"type": "int"}})
        self._assert_no_error(res_fields)

        # First set a valid value so we can prove the bad write does not overwrite it
        res_good = comp.set_data(age=123)  # type: ignore[arg-type]
        res_good = cast(Dict[str, Any], res_good)
        self.assertIsInstance(res_good, dict)
        self.assertNotIn("error", res_good)
        self.assertEqual(res_good["age"], 123)

        # Now send incompatible type
        res_bad = comp.set_data(age="not-an-int")  # type: ignore[arg-type]
        res_bad = cast(Dict[str, Any], res_bad)
        self.assertIsInstance(res_bad, dict)
        self.assertIn("error", res_bad)
        self.assertIn("expected int", res_bad["error"])

        # Data should remain as last good value (no successful overwrite)
        final_state = cast(Dict[str, Any], comp.get_data())
        self.assertIsInstance(final_state, dict)
        self.assertNotIn("error", final_state)
        self.assertEqual(final_state["age"], 123)

    def test_set_data_in_json_splits_by_schema(self):
        root, session = fresh_root_and_session("test_set_data_in_json_splits_by_schema")
        comp = self._new_comp(session)

        res_fields = comp.set_fields({"name": {"type": "str"}, "age": {"type": "int"}})
        self._assert_no_error(res_fields)

        res = comp.set_data(_in_json='{"name":"Alice","age":30,"extra":"ignored"}', age=31)  # type: ignore[arg-type]
        res = cast(Dict[str, Any], res)
        self.assertIsInstance(res, dict)
        self.assertNotIn("error", res)

        self.assertEqual(res["name"], "Alice")
        self.assertEqual(res["age"], 31)
        self.assertIn("extra", res)
        self.assertNotIn("_in_json", res)

    def test_set_then_clear_fields_then_set_new_untyped_values(self):
        """
        setting, then removing fields, then setting new unseen fields as values only should work
        """
        root, session = fresh_root_and_session(
            "test_set_then_clear_fields_then_set_new_untyped_values"
        )
        comp = self._new_comp(session)

        # Step 1: set initial schema and data
        res_fields = comp.set_fields({"name": {"type": "str"}})
        self._assert_no_error(res_fields)

        res_data = comp.set_data(name="Alice")  # type: ignore[arg-type]
        res_data = cast(Dict[str, Any], res_data)
        self.assertIsInstance(res_data, dict)
        self.assertNotIn("error", res_data)
        self.assertEqual(res_data["name"], "Alice")

        # Step 2: "remove" fields by setting empty schema
        res_clear = comp.set_fields({})
        self._assert_no_error(res_clear)

        fields_after_clear = comp.get_fields()
        self.assertIsInstance(fields_after_clear, dict)
        self.assertNotIn("error", fields_after_clear)
        self.assertEqual(fields_after_clear, {"_in_json": {"type": "json_list", "optional": True}})

        # Step 3: set new unseen fields as values only (no schema)
        res_new = comp.set_data(new_field=123)  # type: ignore[arg-type]
        res_new = cast(Dict[str, Any], res_new)
        self.assertIsInstance(res_new, dict)
        self.assertNotIn("error", res_new)

        self.assertEqual(res_new["new_field"], 123)


if __name__ == "__main__":
    unittest.main()
