import json
import inspect
import os
import shutil
import unittest
from unittest import mock
from typing import Tuple

from nodejobs.dependencies.BaseSession import DataSession


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_TMP = os.path.join(THIS_DIR, "base_session_path_replace_tests")


def fresh_root_and_session(test_name: str) -> Tuple[str, DataSession]:
    root = os.path.join(BASE_TMP, test_name)
    if os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root, exist_ok=True)
    session = DataSession({"root": root, DataSession.f_unlocked: True})
    return root, session


class DataSessionPathReplaceTests(unittest.TestCase):
    def _read_text(self, path: str) -> str:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def _read_bytes(self, path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    def _write_text(self, path: str, text: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

    def _norm(self, s: str) -> str:
        return str(s).replace("\\", "/")

    def _assert_not_error(self, res) -> None:
        self.assertIsInstance(res, dict)
        self.assertNotEqual(str(res.get("status", "")).lower(), "error", f"unexpected error payload: {res}")

    def _tokenize_like_boundary_map(self, out_obj, graph_boundary_map: dict, ignore_key_paths=None):
        """
        Utility-test analogue for node_mcp output tokenization:
        - build replacement pairs from boundary map (root -> <gid-{id}->)
        - run DataSession.path_replace_in_object
        """
        repl = []
        for gid, root in graph_boundary_map.items():
            if not isinstance(gid, str) or not gid.strip():
                continue
            if not isinstance(root, str) or not root.strip():
                continue
            src = os.path.abspath(root).replace("\\", "/")
            dst = f"<gid-{gid}->"
            repl.append((src, dst))
        return DataSession.path_replace_in_object(
            repl, out_obj, ignore_key_paths=ignore_key_paths
        )

    def test_path_string_replace_longest_prefix_wins(self):
        repl = [
            ("/a/b", "/x/y"),
            ("/a/b/c", "/x/y/z"),
        ]
        target = "/a/b/c/file.json"
        out = DataSession.path_string_replace(repl, target)
        self.assertEqual(self._norm(out), "/x/y/z/file.json")

    def test_path_string_replace_boundary_safe_no_partial_token_hits(self):
        repl = [("/root/graph", "/dst/graph")]
        target = "A=/root/graphical B=/root/graph/file C=/root/graph"
        out = DataSession.path_string_replace(repl, target)
        n = self._norm(out)
        self.assertIn("A=/root/graphical", n)
        self.assertIn("B=/dst/graph/file", n)
        self.assertIn("C=/dst/graph", n)

    def test_path_string_replace_preserves_trailing_separator_behavior(self):
        repl = [("/old/root", "/new/root")]
        target = "a=/old/root/ b=/old/root"
        out = DataSession.path_string_replace(repl, target)
        n = self._norm(out)
        self.assertIn("a=/new/root/", n)
        self.assertIn("b=/new/root", n)

    def test_path_string_replace_handles_mixed_slashes(self):
        repl = [
            (r"C:\Old\Root", "/new/root"),
            ("/old/root", "/new/root"),
        ]
        target = r"p1=C:\Old\Root\child.txt p2=/old/root/child.txt"
        out = DataSession.path_string_replace(repl, target)
        n = self._norm(out)
        self.assertIn("p1=/new/root/child.txt", n)
        self.assertIn("p2=/new/root/child.txt", n)

    def test_path_string_replace_case_insensitive_match(self):
        repl = [("/Root/Graph", "/dst/graph")]
        target = "/root/graph/file.json and /ROOT/GRAPH/sub.txt"
        out = DataSession.path_string_replace(repl, target)
        n = self._norm(out)
        self.assertIn("/dst/graph/file.json", n)
        self.assertIn("/dst/graph/sub.txt", n)
        self.assertNotIn("/root/graph/file.json", n.lower())

    def test_path_string_replace_multiple_pairs_single_target(self):
        repl = [
            ("/src/a", "/dst/a"),
            ("/src/b", "/dst/b"),
        ]
        target = "one:/src/a/file two:/src/b/file"
        out = DataSession.path_string_replace(repl, target)
        n = self._norm(out)
        self.assertIn("one:/dst/a/file", n)
        self.assertIn("two:/dst/b/file", n)

    def test_safe_path_file_replace_updates_txt_file(self):
        root, _ = fresh_root_and_session("test_safe_path_file_replace_updates_txt_file")
        p = os.path.join(root, "notes.txt")
        self._write_text(p, "root=/old/root\n")

        res = DataSession.safe_path_file_replace(
            p,
            [("/old/root", "/new/root")],
            strict_json=True,
        )
        self._assert_not_error(res)
        self.assertTrue(bool(res.get("updated")), f"expected updated=True, got: {res}")
        self.assertIn("/new/root", self._norm(self._read_text(p)))

    def test_safe_path_file_replace_updates_ndjson_file(self):
        root, _ = fresh_root_and_session("test_safe_path_file_replace_updates_ndjson_file")
        p = os.path.join(root, "events.ndjson")
        lines = [
            json.dumps({"path": "/old/root/a"}),
            json.dumps({"content": {"src": "/old/root/b"}}),
        ]
        self._write_text(p, "\n".join(lines) + "\n")

        res = DataSession.safe_path_file_replace(
            p,
            [("/old/root", "/new/root")],
            strict_json=True,
        )
        self._assert_not_error(res)
        self.assertTrue(bool(res.get("updated")), f"expected updated=True, got: {res}")

        out_lines = [ln for ln in self._read_text(p).splitlines() if ln.strip()]
        self.assertEqual(len(out_lines), 2)
        parsed = [json.loads(ln) for ln in out_lines]
        self.assertEqual(parsed[0]["path"], "/new/root/a")
        self.assertEqual(parsed[1]["content"]["src"], "/new/root/b")

    def test_safe_path_file_replace_noop_when_no_match(self):
        root, _ = fresh_root_and_session("test_safe_path_file_replace_noop_when_no_match")
        p = os.path.join(root, "noop.txt")
        self._write_text(p, "no matching path here\n")
        before = self._read_bytes(p)

        res = DataSession.safe_path_file_replace(
            p,
            [("/old/root", "/new/root")],
            strict_json=True,
        )
        self._assert_not_error(res)
        if "updated" in res:
            self.assertFalse(bool(res.get("updated")), f"expected updated=False, got: {res}")
        after = self._read_bytes(p)
        self.assertEqual(before, after)

    def test_safe_path_file_replace_json_validity_preserved_when_original_valid(self):
        root, _ = fresh_root_and_session("test_safe_path_file_replace_json_validity_preserved_when_original_valid")
        p = os.path.join(root, "config.json")
        original_obj = {
            "root": "/old/root",
            "nested": {"paths": ["/old/root/a", "/old/root/b"]},
        }
        self._write_text(p, json.dumps(original_obj, indent=2) + "\n")

        res = DataSession.safe_path_file_replace(
            p,
            [("/old/root", "/new/root")],
            strict_json=True,
        )
        self._assert_not_error(res)
        self.assertTrue(bool(res.get("updated")), f"expected updated=True, got: {res}")

        out_text = self._read_text(p)
        out_obj = json.loads(out_text)
        self.assertEqual(out_obj["root"], "/new/root")
        self.assertEqual(out_obj["nested"]["paths"][0], "/new/root/a")
        self.assertEqual(out_obj["nested"]["paths"][1], "/new/root/b")

    def test_safe_path_file_replace_invalid_candidate_restores_backup(self):
        root, _ = fresh_root_and_session("test_safe_path_file_replace_invalid_candidate_restores_backup")
        p = os.path.join(root, "data.json")
        original = json.dumps({"root": "/old/root", "ok": True}, indent=2) + "\n"
        self._write_text(p, original)

        with mock.patch.object(DataSession, "path_string_replace", return_value='{"root": "/new/root",'):
            res = DataSession.safe_path_file_replace(
                p,
                [("/old/root", "/new/root")],
                strict_json=True,
            )

        self.assertIsInstance(res, dict)
        self.assertEqual(str(res.get("status", "")).lower(), "error", f"expected error result, got: {res}")
        restored = self._read_text(p)
        self.assertEqual(restored, original)
        parsed = json.loads(restored)
        self.assertEqual(parsed["root"], "/old/root")
        self.assertEqual(parsed["ok"], True)

    def test_safe_path_file_replace_original_invalid_json_does_not_enforce_post_parse(self):
        root, _ = fresh_root_and_session("test_safe_path_file_replace_original_invalid_json_does_not_enforce_post_parse")
        p = os.path.join(root, "invalid.json")
        original = '{"root": "/old/root", "missing_end": true'
        self._write_text(p, original)

        res = DataSession.safe_path_file_replace(
            p,
            [("/old/root", "/new/root")],
            strict_json=True,
        )
        self._assert_not_error(res)
        self.assertTrue(bool(res.get("updated")), f"expected updated=True, got: {res}")
        out = self._read_text(p)
        self.assertIn("/new/root", self._norm(out))
        self.assertNotIn("/old/root", self._norm(out))
        with self.assertRaises(json.JSONDecodeError):
            json.loads(out)

    def test_safe_path_file_replace_multiple_files_recursive_loop_updates_all(self):
        root, _ = fresh_root_and_session("test_safe_path_file_replace_multiple_files_recursive_loop_updates_all")
        files = {
            os.path.join(root, "a.json"): json.dumps({"p": "/old/root/a"}),
            os.path.join(root, "sub", "b.txt"): "path=/old/root/b\n",
            os.path.join(root, "sub", "deep", "c.ndjson"): json.dumps({"p": "/old/root/c"}) + "\n",
            os.path.join(root, "sub", "deep", "ignore.bin"): "binary-ish /old/root/ignore",
        }
        for p, content in files.items():
            self._write_text(p, content)

        target_exts = {".json", ".ndjson", ".txt"}
        replaced_files = []
        for dirpath, _, names in os.walk(root):
            for name in names:
                ext = os.path.splitext(name)[1].lower()
                if ext not in target_exts:
                    continue
                p = os.path.join(dirpath, name)
                res = DataSession.safe_path_file_replace(
                    p,
                    [("/old/root", "/new/root")],
                    strict_json=True,
                )
                self._assert_not_error(res)
                replaced_files.append(p)

        self.assertEqual(len(replaced_files), 3)

        for p in replaced_files:
            text = self._read_text(p)
            self.assertIn("/new/root", self._norm(text), f"expected replacement in {p}")
            self.assertNotIn("/old/root", self._norm(text), f"unexpected old root in {p}")

        # Non-target extension untouched by recursive loop.
        ignored = self._read_text(os.path.join(root, "sub", "deep", "ignore.bin"))
        self.assertIn("/old/root/ignore", ignored)

    def test_safe_path_file_replace_success_cleans_candidate_and_backup_files(self):
        root, _ = fresh_root_and_session("test_safe_path_file_replace_success_cleans_candidate_and_backup_files")
        p = os.path.join(root, "cleanup.txt")
        self._write_text(p, "x=/old/root\n")

        base = os.path.basename(p)
        folder = os.path.dirname(p)
        candidate = os.path.join(folder, f".{base}.candidate")
        backup = os.path.join(folder, f".{base}.backup")

        res = DataSession.safe_path_file_replace(
            p,
            [("/old/root", "/new/root")],
            strict_json=True,
        )
        self._assert_not_error(res)
        self.assertTrue(bool(res.get("updated")))
        self.assertFalse(os.path.exists(candidate), "candidate temp file should be removed on success")
        self.assertFalse(os.path.exists(backup), "backup temp file should be removed on success")
        self.assertIn("/new/root", self._read_text(p))

    def test_safe_path_file_replace_swap_failure_restores_and_cleans_temp_files(self):
        root, _ = fresh_root_and_session("test_safe_path_file_replace_swap_failure_restores_and_cleans_temp_files")
        p = os.path.join(root, "rollback.txt")
        original = "x=/old/root\n"
        self._write_text(p, original)

        base = os.path.basename(p)
        folder = os.path.dirname(p)
        candidate = os.path.join(folder, f".{base}.candidate")
        backup = os.path.join(folder, f".{base}.backup")

        real_rename = os.rename
        calls = {"n": 0}

        def flaky_rename(src, dst):
            calls["n"] += 1
            # 1) original -> backup succeeds
            if calls["n"] == 1:
                return real_rename(src, dst)
            # 2) candidate -> original fails (exercise restore path)
            if calls["n"] == 2:
                raise OSError("simulated_swap_failure")
            # 3) backup -> original restore succeeds
            return real_rename(src, dst)

        with mock.patch("os.rename", side_effect=flaky_rename):
            res = DataSession.safe_path_file_replace(
                p,
                [("/old/root", "/new/root")],
                strict_json=True,
            )

        self.assertIsInstance(res, dict)
        self.assertEqual(str(res.get("status", "")).lower(), "error")
        self.assertTrue(bool(res.get("backup_restored")), f"expected backup restore, got: {res}")
        self.assertTrue(os.path.isfile(p), "original file should exist after restore")
        self.assertEqual(self._read_text(p), original, "original content should be restored")
        self.assertFalse(os.path.exists(candidate), "candidate temp file should be cleaned on failure")
        self.assertFalse(os.path.exists(backup), "backup temp file should be consumed by restore")

    def test_path_replace_in_object_boundary_processor_matrix_port(self):
        graph_id = "gid_matrix_a"
        graph_root = "/tmp/node_mcp_boundary/.node_mcp_server/graphs/gid_matrix_a"
        fallback_root = "/tmp/node_mcp_boundary"
        graph_token = f"<gid-{graph_id}->/"
        local_token = "<gid-local_python_scripts->/"

        graph_boundary_map = {
            graph_id: graph_root,
            "local_python_scripts": fallback_root,
            "ai_services_package": "/tmp/node_mcp_boundary_pkg",
        }

        # first-level only ignore policy (current utility contract)
        ignore_top_level = ["error", "details"]

        cases = [
            {"name": "exact_graph_root_bare", "input": {"v": graph_root}, "expected": {"v": f"<gid-{graph_id}->"}},
            {"name": "graph_root_child_slash", "input": {"v": graph_root + "/node/file.json"}, "expected": {"v": graph_token + "node/file.json"}},
            {"name": "graph_root_child_backslash", "input": {"v": graph_root + "\\node\\file.json"}, "expected": {"v": graph_token + "node\\file.json"}},
            {"name": "json_boundary_quote_comma", "input": {"v": f'path "{graph_root}", next'}, "expected": {"v": f'path "<gid-{graph_id}->", next'}},
            {"name": "multiple_occurrences_same_string", "input": {"v": f"{graph_root}/a and {graph_root}/b"}, "expected": {"v": f"{graph_token}a and {graph_token}b"}},
            {
                "name": "graph_root_beats_fallback_prefix",
                "input": {"v": graph_root + "/a"},
                "expected": {"v": graph_token + "a"},
                "not_contains": [local_token + ".node_mcp_server/graphs/"],
            },
            {
                "name": "prefix_collision_falls_back",
                "input": {"v": graph_root + "XYZ/file"},
                "expected": {"v": local_token + ".node_mcp_server/graphs/gid_matrix_aXYZ/file"},
            },
            {
                "name": "existing_token_kept_raw_root_replaced",
                "input": {"v": f"<gid-{graph_id}->/kept {graph_root}/rewritten"},
                "expected": {"v": f"<gid-{graph_id}->/kept {graph_token}rewritten"},
            },
            {
                "name": "case_insensitive_match",
                "input": {"v": graph_root.upper() + "/CASE.TXT"},
                "expected": {"v": graph_token + "CASE.TXT"},
            },
            {
                "name": "recursive_dict_and_list",
                "input": {"a": [graph_root, {"b": graph_root + "/x"}]},
                "expected": {"a": [f"<gid-{graph_id}->", {"b": graph_token + "x"}]},
            },
            {
                "name": "ignore_top_level_error_subtree",
                "input": {"error": {"v": graph_root + "/x"}, "safe": graph_root + "/ok"},
                "expected": {"error": {"v": graph_root + "/x"}, "safe": graph_token + "ok"},
            },
            {
                # Deliberate first-level-only behavior: nested details are NOT ignored.
                "name": "nested_details_tokenized_first_level_ignore_only",
                "input": {"a": {"details": {"v": graph_root + "/x"}}, "safe": graph_root + "/ok"},
                "expected": {"a": {"details": {"v": graph_token + "x"}}, "safe": graph_token + "ok"},
            },
            {
                "name": "detail_subtree_tokenized",
                "input": {"a": {"detail": {"v": graph_root + "/x"}}, "safe": graph_root + "/ok"},
                "expected": {"a": {"detail": {"v": graph_token + "x"}}, "safe": graph_token + "ok"},
            },
            {
                "name": "non_string_values_unchanged",
                "input": {"n": 1, "b": True, "x": None},
                "expected": {"n": 1, "b": True, "x": None},
            },
        ]

        for case in cases:
            with self.subTest(case=case["name"]):
                out = self._tokenize_like_boundary_map(
                    out_obj=case["input"],
                    graph_boundary_map=graph_boundary_map,
                    ignore_key_paths=ignore_top_level,
                )
                self.assertEqual(out, case["expected"])
                out_text = json.dumps(out, ensure_ascii=True)
                for needle in case.get("not_contains", []):
                    self.assertNotIn(needle, out_text)

    def test_path_replace_in_object_trailing_shape_preservation_matrix_port(self):
        graph_id = "gid_matrix_shape"
        graph_root = "/some/root"
        graph_token = f"<gid-{graph_id}->"

        graph_boundary_map = {
            graph_id: graph_root,
        }

        cases = [
            {"name": "exact_root_no_trailing_slash", "input": {"v": graph_root}, "expected": {"v": graph_token}},
            {"name": "root_quoted_before_comma", "input": {"v": f'path "{graph_root}", next'}, "expected": {"v": f'path "{graph_token}", next'}},
            {"name": "short_child_folder_no_trailing", "input": {"v": graph_root + "/nd_1233"}, "expected": {"v": graph_token + "/nd_1233"}},
            {"name": "short_child_folder_trailing", "input": {"v": graph_root + "/nd_1233/"}, "expected": {"v": graph_token + "/nd_1233/"}},
            {"name": "long_folder_no_trailing", "input": {"v": graph_root + "/leads/to/folder"}, "expected": {"v": graph_token + "/leads/to/folder"}},
            {"name": "long_folder_trailing", "input": {"v": graph_root + "/leads/to/folder/"}, "expected": {"v": graph_token + "/leads/to/folder/"}},
            {"name": "long_file_txt", "input": {"v": graph_root + "/leads/to/folder/file.txt"}, "expected": {"v": graph_token + "/leads/to/folder/file.txt"}},
            {"name": "root_with_trailing_slash", "input": {"v": graph_root + "/"}, "expected": {"v": graph_token + "/"}},
        ]

        for case in cases:
            with self.subTest(case=case["name"]):
                out = self._tokenize_like_boundary_map(
                    out_obj=case["input"],
                    graph_boundary_map=graph_boundary_map,
                    ignore_key_paths=[],
                )
                self.assertEqual(out, case["expected"])

    def test_path_string_replace_pattern_source_guard_port(self):
        """
        Ported source guard style from node_mcp tests:
        path_string_replace must not regress to parser-fragile rf-string regex style.
        """
        src = inspect.getsource(DataSession.path_string_replace)
        self.assertNotIn('rf"(?:', src)
        self.assertNotIn("\\{", src)

    def test_collect_paths_under_root_recursive_extension_filter(self):
        root, _ = fresh_root_and_session("test_collect_paths_under_root_recursive_extension_filter")
        self._write_text(os.path.join(root, "a.json"), "{}\n")
        self._write_text(os.path.join(root, "sub", "b.ndjson"), "{}\n")
        self._write_text(os.path.join(root, "sub", "deep", "c.txt"), "x\n")
        self._write_text(os.path.join(root, "sub", "deep", "d.bin"), "x\n")

        out = DataSession.collect_paths_under_root(
            root_path=root,
            include_exts=[".json", ".ndjson", ".txt"],
        )
        norm = [self._norm(p) for p in out]
        self.assertEqual(len(norm), 3)
        self.assertTrue(any(p.endswith("/a.json") for p in norm))
        self.assertTrue(any(p.endswith("/sub/b.ndjson") for p in norm))
        self.assertTrue(any(p.endswith("/sub/deep/c.txt") for p in norm))
        self.assertFalse(any(p.endswith("/d.bin") for p in norm))

    def test_safe_path_file_replace_many_aggregate_counts(self):
        root, _ = fresh_root_and_session("test_safe_path_file_replace_many_aggregate_counts")
        p1 = os.path.join(root, "a.txt")
        p2 = os.path.join(root, "b.txt")
        p3 = os.path.join(root, "c.txt")
        self._write_text(p1, "p=/old/root/a\n")
        self._write_text(p2, "p=/old/root/b\n")
        self._write_text(p3, "no-hit\n")

        out = DataSession.safe_path_file_replace_many(
            target_paths=[p1, p2, p3],
            replacements=[("/old/root", "/new/root")],
            ignore_key_paths=None,
            stop_on_error=False,
        )
        self.assertEqual(out.get("status"), "ok")
        self.assertEqual(out.get("files_scanned"), 3)
        self.assertEqual(out.get("files_updated"), 2)
        self.assertEqual(out.get("files_noop"), 1)
        self.assertEqual(out.get("files_failed"), 0)
        self.assertIn("/new/root", self._norm(self._read_text(p1)))
        self.assertIn("/new/root", self._norm(self._read_text(p2)))
        self.assertIn("no-hit", self._read_text(p3))

    def test_safe_path_file_move_overwrite_existing_dst_with_backup_cleanup(self):
        root, _ = fresh_root_and_session("test_safe_path_file_move_overwrite_existing_dst_with_backup_cleanup")
        src = os.path.join(root, "src.txt")
        dst = os.path.join(root, "sub", "dst.txt")
        self._write_text(src, "new-value\n")
        self._write_text(dst, "old-value\n")

        out = DataSession.safe_path_file_move(src_path=src, dst_path=dst)
        self.assertEqual(out.get("status"), "ok")
        self.assertTrue(bool(out.get("moved")))
        self.assertFalse(os.path.exists(src))
        self.assertEqual(self._read_text(dst), "new-value\n")
        self.assertFalse(os.path.exists(os.path.join(root, "sub", ".dst.txt.backup")))

    def test_safe_path_file_move_restores_dst_when_move_fails(self):
        root, _ = fresh_root_and_session("test_safe_path_file_move_restores_dst_when_move_fails")
        src = os.path.join(root, "src.txt")
        dst = os.path.join(root, "dst.txt")
        self._write_text(src, "new-value\n")
        self._write_text(dst, "old-value\n")

        real_rename = os.rename
        src_abs = os.path.abspath(src)
        dst_abs = os.path.abspath(dst)

        def _rename_with_forced_fail(a, b):
            if os.path.abspath(a) == src_abs and os.path.abspath(b) == dst_abs:
                raise Exception("forced_move_failure")
            return real_rename(a, b)

        with mock.patch("nodejobs.dependencies.BaseSession.os.rename", side_effect=_rename_with_forced_fail):
            out = DataSession.safe_path_file_move(src_path=src, dst_path=dst)

        self.assertEqual(out.get("status"), "error")
        self.assertTrue(bool(out.get("backup_restored")))
        self.assertEqual(self._read_text(src), "new-value\n")
        self.assertEqual(self._read_text(dst), "old-value\n")

    def test_safe_path_file_move_missing_src_returns_error(self):
        root, _ = fresh_root_and_session("test_safe_path_file_move_missing_src_returns_error")
        src = os.path.join(root, "missing.txt")
        dst = os.path.join(root, "dst.txt")
        out = DataSession.safe_path_file_move(src_path=src, dst_path=dst)
        self.assertEqual(out.get("status"), "error")
        self.assertEqual(out.get("reason"), "src_not_file")
        self.assertFalse(bool(out.get("moved")))

    def test_safe_path_replace_under_root_recursive_updates_and_reports(self):
        root, _ = fresh_root_and_session("test_safe_path_replace_under_root_recursive_updates_and_reports")
        self._write_text(os.path.join(root, "a.json"), json.dumps({"p": "/old/root/a"}))
        self._write_text(os.path.join(root, "sub", "b.txt"), "p=/old/root/b\n")
        self._write_text(os.path.join(root, "sub", "deep", "c.ndjson"), json.dumps({"p": "/old/root/c"}) + "\n")
        self._write_text(os.path.join(root, "sub", "deep", "ignore.bin"), "p=/old/root/d\n")

        out = DataSession.safe_path_replace_under_root(
            root_path=root,
            replacements=[("/old/root", "/new/root")],
            include_exts=[".json", ".ndjson", ".txt"],
            ignore_key_paths=None,
            stop_on_error=False,
            exclude_dirs=None,
        )
        self.assertEqual(out.get("status"), "ok")
        self.assertEqual(out.get("matched_count"), 3)
        self.assertEqual(out.get("files_failed"), 0)
        self.assertEqual(out.get("files_updated"), 3)

        for p in [
            os.path.join(root, "a.json"),
            os.path.join(root, "sub", "b.txt"),
            os.path.join(root, "sub", "deep", "c.ndjson"),
        ]:
            text = self._read_text(p)
            self.assertIn("/new/root", self._norm(text))
            self.assertNotIn("/old/root", self._norm(text))

        self.assertIn("/old/root/d", self._read_text(os.path.join(root, "sub", "deep", "ignore.bin")))

    def test_safe_path_replace_under_root_invalid_root(self):
        out = DataSession.safe_path_replace_under_root(
            root_path="/tmp/this/path/should/not/exist",
            replacements=[("/old/root", "/new/root")],
        )
        self.assertEqual(out.get("status"), "error")
        self.assertEqual(out.get("error"), "root_not_dir")


if __name__ == "__main__":
    unittest.main()
