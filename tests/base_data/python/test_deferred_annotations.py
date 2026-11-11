"""Regression for deferred BaseData field names on Python 3.14."""

import unittest

from nodejobs.dependencies.BaseData import BaseData


class DeferredAnnotationsTests(unittest.TestCase):
    def test_annotated_field_is_available_as_a_class_key(self):
        class ListGraphsArgs(BaseData):
            auth_email: str
            auth_token: str

        self.assertEqual(ListGraphsArgs.auth_email, "auth_email")
        self.assertEqual(ListGraphsArgs.auth_token, "auth_token")
        data = ListGraphsArgs({ListGraphsArgs.auth_email: "owner@example.com", ListGraphsArgs.auth_token: "token"})
        self.assertEqual(data.auth_email, "owner@example.com")
        self.assertEqual(dict(data)["auth_email"], "owner@example.com")


if __name__ == "__main__":
    unittest.main()
