import os
import shutil
import unittest
from nodejobs.events.data_session_append import DataSessionAppend

from nodejobs.events.ndjson_events import (
    NDJSONWriter,
    NDJSONReader,
    InvalidEventError,
    StreamReadError,
    _data_path,
)

from nodejobs.events.models import (
    EventData,
)

from nodejobs.events.watcher import (
    WatchConfig,
    WatchFilter,
    NDJSONWatcher,
)
import threading
import time


class TestEvents(unittest.TestCase):
    def _mk_session(self, root: str):
        # Clean and prepare a fresh root per test
        if os.path.exists(root):
            shutil.rmtree(root)
        os.makedirs(root, exist_ok=True)
        return DataSessionAppend(
            {DataSessionAppend.f_root: root, DataSessionAppend.f_verbose: False}
        )

    def _key_paths(self) -> list[str]:
        # Single, stable composite key path list for tests; missing contributes ""
        return ["labels.k"]

    def _count_nonempty_lines(self, session, path: str) -> int:
        with session.open(path, "rb") as f:
            return sum(1 for raw in f if raw and raw.strip())

    def test_basic_write_read_defaults(self):
        session = self._mk_session("./test_events_defaults")
        path = "logs/test.ndjson"
        writer = NDJSONWriter(session, path)
        reader = NDJSONReader(session, path)

        off = writer.append(
            {
                EventData.content: {},
                EventData.labels: {},
            },
            self._key_paths(),
        )
        self.assertIsInstance(off, int)
        self.assertGreater(off, 0)

        evs = reader.read_all()
        self.assertEqual(len(evs), 1)
        e = evs[0]
        # datetime Z format and parseable
        self.assertTrue(isinstance(e.datetime, str) and e.datetime.endswith("Z"))
        import datetime as _dt

        _ = _dt.datetime.fromisoformat(e.datetime.replace("Z", "+00:00"))

        self.assertEqual(e.labels, {})
        self.assertEqual(e.sources, [])
        self.assertEqual(e.destinations, [])
        self.assertEqual(e.content, {})
        data = e.to_safe_dict_LLMS_DONT_USE()
        self.assertNotIn("ttl", data)

    def test_multi_append_order_and_search(self):
        session = self._mk_session("./test_events_multi")
        path = "logs/multi.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)

        # A: explicit datetime
        a_dt = "2020-01-01T00:00:00Z"
        w.append(
            {
                EventData.datetime: a_dt,
                EventData.content: {"a": 1},
                EventData.labels: {},
            },
            self._key_paths(),
        )
        # B: labels + searchable text
        w.append(
            {
                EventData.content: {"msg": "hello world"},
                EventData.labels: {"job": "alpha", "ok": True},
            },
            self._key_paths(),
        )
        # C: sources/destinations (single-string normalize)
        w.append(
            {
                EventData.content: {"x": "y"},
                EventData.labels: {},
                EventData.sources: ["svc1"],
                EventData.destinations: "svc2",
            },
            self._key_paths(),
        )

        evs = r.read_all()
        self.assertEqual(len(evs), 3)
        self.assertEqual(evs[0].datetime, a_dt)
        self.assertEqual(evs[1].labels.get("job"), "alpha")
        self.assertEqual(evs[1].labels.get("ok"), True)
        self.assertEqual(evs[2].sources, ["svc1"])
        self.assertEqual(evs[2].destinations, ["svc2"])

        # search: labels
        hits = r.search(labels={"job": "alpha"})
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0].labels.get("job"), "alpha")

        # search: text
        hits2 = r.search(text="hello")
        self.assertEqual(len(hits2), 1)
        self.assertIn("hello", hits2[0].content.get("msg", ""))

    def test_incremental_read_offsets(self):
        session = self._mk_session("./test_events_incr")
        path = "logs/incr.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)

        # write 3
        for i in range(3):
            w.append(
                {EventData.content: {"i": i}, EventData.labels: {"k": i}},
                self._key_paths(),
            )

        evs1, off1 = r.read_from_offset(0)
        self.assertEqual(len(evs1), 3)
        self.assertGreater(off1, 0)

        # write 2 more
        w.append(
            {EventData.content: {"i": 3}, EventData.labels: {"k": 3}}, self._key_paths()
        )
        w.append(
            {EventData.content: {"i": 4}, EventData.labels: {"k": 4}}, self._key_paths()
        )

        evs2, off2 = r.read_from_offset(off1)
        self.assertEqual(len(evs2), 2)
        self.assertGreater(off2, off1)

        evs3, off3 = r.read_from_offset(off2)
        self.assertEqual(len(evs3), 0)
        self.assertEqual(off3, off2)

        # Ensure order continuity
        all_is = [*[_e.content["i"] for _e in evs1], *[_e.content["i"] for _e in evs2]]
        self.assertEqual(all_is, [0, 1, 2, 3, 4])

    def test_invalid_event_rejected(self):
        session = self._mk_session("./test_events_invalid")
        path = "logs/invalid.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)

        # write a valid event first
        w.append(
            {EventData.content: {"ok": 1}, EventData.labels: {"a": 1}},
            self._key_paths(),
        )

        with self.assertRaises(InvalidEventError):
            w.append(
                {EventData.content: {}, EventData.labels: {"bad": [1, 2]}},
                self._key_paths(),
            )

        # Still only 1 event present, no partial writes
        evs = r.read_all()
        self.assertEqual(len(evs), 1)
        self.assertIn("ok", evs[0].content)

    def test_watcher_tail_from_end_only_new(self):
        session = self._mk_session("./test_events_watch_tail_end")
        path = "logs/tail_end.ndjson"
        w = NDJSONWriter(session, path)

        # baseline not to be re-emitted
        w.append({EventData.content: {"i": 0}, EventData.labels: {}}, self._key_paths())
        w.append({EventData.content: {"i": 1}, EventData.labels: {}}, self._key_paths())

        cfg = WatchConfig(
            {
                WatchConfig.path: path,
                WatchConfig.poll_ms: 20,
                WatchConfig.from_end: True,
                WatchConfig.wait_for_create: True,
                WatchConfig.on_truncate: "end",
                WatchConfig.idle_stop_ms: 300,
            }
        )
        watcher = NDJSONWatcher(session, cfg)

        out = []

        def run():
            for ev in watcher.tail():
                out.append(ev)

        t = threading.Thread(target=run, daemon=True)
        t.start()

        # Give watcher a moment to start polling
        time.sleep(0.05)
        # Append new events; only these should be captured
        w.append({EventData.content: {"i": 2}, EventData.labels: {}}, self._key_paths())
        w.append({EventData.content: {"i": 3}, EventData.labels: {}}, self._key_paths())

        t.join(timeout=2.0)
        self.assertGreaterEqual(len(out), 2)
        got = [e.content.get("i") for e in out]
        self.assertTrue(2 in got and 3 in got)
        self.assertFalse(0 in got or 1 in got)

    def test_watcher_from_start_reads_existing_and_live(self):
        session = self._mk_session("./test_events_watch_tail_start")
        path = "logs/tail_start.ndjson"
        w = NDJSONWriter(session, path)

        w.append(
            {EventData.content: {"m": "A"}, EventData.labels: {}}, self._key_paths()
        )
        w.append(
            {EventData.content: {"m": "B"}, EventData.labels: {}}, self._key_paths()
        )

        cfg = WatchConfig(
            {
                WatchConfig.path: path,
                WatchConfig.poll_ms: 20,
                WatchConfig.from_end: False,
                WatchConfig.wait_for_create: True,
                WatchConfig.on_truncate: "end",
                WatchConfig.idle_stop_ms: 300,
            }
        )
        watcher = NDJSONWatcher(session, cfg)

        out = []

        def run():
            for ev in watcher.tail():
                out.append(ev)

        t = threading.Thread(target=run, daemon=True)
        t.start()

        time.sleep(0.05)
        w.append(
            {EventData.content: {"m": "C"}, EventData.labels: {}}, self._key_paths()
        )

        t.join(timeout=2.0)
        got = [e.content.get("m") for e in out]
        self.assertEqual(got[:2], ["A", "B"])
        self.assertIn("C", got)

    def test_watcher_wait_for_create(self):
        session = self._mk_session("./test_events_watch_wait_create")
        path = "logs/wait_create.ndjson"

        cfg = WatchConfig(
            {
                WatchConfig.path: path,
                WatchConfig.poll_ms: 20,
                WatchConfig.from_end: False,
                WatchConfig.wait_for_create: True,
                WatchConfig.on_truncate: "end",
                WatchConfig.idle_stop_ms: 300,
            }
        )
        watcher = NDJSONWatcher(session, cfg)

        out = []

        def run():
            for ev in watcher.tail():
                out.append(ev)

        t = threading.Thread(target=run, daemon=True)
        t.start()

        # Create file and write after watcher is already waiting
        time.sleep(0.05)
        w = NDJSONWriter(session, path)
        w.append({EventData.content: {"x": 1}, EventData.labels: {}}, self._key_paths())
        w.append({EventData.content: {"x": 2}, EventData.labels: {}}, self._key_paths())

        t.join(timeout=2.0)
        self.assertEqual([e.content.get("x") for e in out], [1, 2])

    def test_watcher_on_truncate_policies(self):
        def run_policy(root_dir, policy):
            session = self._mk_session(root_dir)
            path = "logs/trunc.ndjson"
            w = NDJSONWriter(session, path)
            w.append(
                {EventData.content: {"i": 0}, EventData.labels: {}}, self._key_paths()
            )
            w.append(
                {EventData.content: {"i": 1}, EventData.labels: {}}, self._key_paths()
            )

            cfg = WatchConfig(
                {
                    WatchConfig.path: path,
                    WatchConfig.poll_ms: 20,
                    WatchConfig.from_end: False,
                    WatchConfig.wait_for_create: True,
                    WatchConfig.on_truncate: policy,
                    WatchConfig.idle_stop_ms: 300,
                }
            )
            watcher = NDJSONWatcher(session, cfg)
            out = []
            err = []

            def run():
                try:
                    it = watcher.tail()
                    first = next(it)
                    out.append(first)
                    with session.open(path, "wb") as f:
                        f.write(b"")
                    time.sleep(0.05)
                    w2 = NDJSONWriter(session, path)
                    w2.append(
                        {EventData.content: {"i": 99}, EventData.labels: {}},
                        self._key_paths(),
                    )
                    for ev in it:
                        out.append(ev)
                except Exception as e:
                    err.append(e)

            t = threading.Thread(target=run, daemon=True)
            t.start()
            t.join(timeout=2.0)
            return [e.content.get("i") for e in out], err

        got_end, err_end = run_policy("./test_events_watch_trunc_end", "end")
        self.assertTrue(len(err_end) == 0)
        self.assertIn(0, got_end)
        self.assertFalse(99 in got_end)

        got_start, err_start = run_policy("./test_events_watch_trunc_start", "start")
        self.assertTrue(len(err_start) == 0)
        self.assertIn(0, got_start)
        self.assertIn(99, got_start)

        got_err, err_err = run_policy("./test_events_watch_trunc_error", "error")
        self.assertTrue(any(isinstance(e, Exception) for e in err_err))

    def test_watcher_filter_labels_text_time(self):
        session = self._mk_session("./test_events_watch_filter")
        path = "logs/filter.ndjson"
        w = NDJSONWriter(session, path)

        w.append(
            {
                EventData.datetime: "2020-01-01T00:00:00Z",
                EventData.content: {"msg": "hello world"},
                EventData.labels: {"job": "alpha", "ok": True},
            },
            self._key_paths(),
        )
        w.append(
            {
                EventData.datetime: "2020-01-01T00:00:05Z",
                EventData.content: {"msg": "bye world"},
                EventData.labels: {"job": "beta"},
            },
            self._key_paths(),
        )

        cfg = WatchConfig(
            {
                WatchConfig.path: path,
                WatchConfig.poll_ms: 20,
                WatchConfig.from_end: False,
                WatchConfig.wait_for_create: True,
                WatchConfig.on_truncate: "end",
                WatchConfig.idle_stop_ms: 300,
            }
        )
        flt = WatchFilter(
            {
                WatchFilter.labels: {"job": "alpha", "ok": True},
                WatchFilter.labels_mode: "all",
                WatchFilter.text: "hello",
                WatchFilter.since: "2020-01-01T00:00:00Z",
                WatchFilter.until: "2020-01-01T00:00:10Z",
            }
        )
        watcher = NDJSONWatcher(session, cfg, flt)

        out = []
        for ev in watcher.tail():
            out.append(ev)

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].labels.get("job"), "alpha")
        self.assertIn("hello", out[0].content.get("msg", ""))

    def test_watcher_callback_exception_propagates(self):
        session = self._mk_session("./test_events_watch_callback_exc")
        path = "logs/cb.ndjson"
        w = NDJSONWriter(session, path)
        w.append(
            {EventData.content: {"boom": 1}, EventData.labels: {}}, self._key_paths()
        )

        cfg = WatchConfig(
            {
                WatchConfig.path: path,
                WatchConfig.poll_ms: 10,
                WatchConfig.from_end: False,
                WatchConfig.wait_for_create: True,
                WatchConfig.on_truncate: "end",
            }
        )
        watcher = NDJSONWatcher(session, cfg)

        class MyErr(Exception):
            pass

        def cb(ev):
            raise MyErr("fail in callback")

        with self.assertRaises(MyErr):
            watcher.watch(cb)

    def test_search_modes_and_time_bounds(self):
        session = self._mk_session("./test_events_search_modes_bounds")
        path = "logs/search.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)

        a_dt = "2020-01-01T00:00:00Z"
        b_dt = "2020-01-01T00:00:05Z"
        c_dt = "2020-01-01T00:00:10Z"

        w.append(
            {
                EventData.datetime: a_dt,
                EventData.content: {"m": "A"},
                EventData.labels: {"job": "alpha"},
            },
            self._key_paths(),
        )
        w.append(
            {
                EventData.datetime: b_dt,
                EventData.content: {"m": "B"},
                EventData.labels: {"job": "beta", "team": "x"},
            },
            self._key_paths(),
        )
        w.append(
            {
                EventData.datetime: c_dt,
                EventData.content: {"m": "C"},
                EventData.labels: {"job": "gamma"},
            },
            self._key_paths(),
        )

        hits_all = r.search(labels={"job": "beta", "team": "x"}, labels_mode="all")
        self.assertEqual(len(hits_all), 1)

        hits_any = r.search(labels={"job": "beta", "team": "nope"}, labels_mode="any")
        self.assertEqual(len(hits_any), 1)

        with self.assertRaises(ValueError):
            _ = r.search(labels={"job": "alpha"}, labels_mode="bogus")

        # since/until inclusive with strings
        hits_bounds_str = r.search(since=a_dt, until=b_dt)
        self.assertEqual([e.content.get("m") for e in hits_bounds_str], ["A", "B"])

        # since/until inclusive with datetime objects
        import datetime as _dt

        since_dt = _dt.datetime.fromisoformat(a_dt.replace("Z", "+00:00"))
        until_dt = _dt.datetime.fromisoformat(c_dt.replace("Z", "+00:00"))
        hits_bounds_dt = r.search(since=since_dt, until=until_dt)
        self.assertEqual(len(hits_bounds_dt), 3)

    def test_offset_validation_and_alignment(self):
        session = self._mk_session("./test_events_offsets")
        path = "logs/off.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)

        with self.assertRaises(ValueError):
            r.read_from_offset(-1)

        w.append({EventData.content: {"a": 1}, EventData.labels: {}}, self._key_paths())
        w.append({EventData.content: {"b": 2}, EventData.labels: {}}, self._key_paths())

        # beyond EOF
        _, end = r.read_from_offset(0)
        self.assertGreater(end, 0)
        with self.assertRaises(StreamReadError):
            r.read_from_offset(end + 1000)

        # mid-line: start slightly inside the first record; expect only the second record
        evs_mid, _ = r.read_from_offset(1)
        self.assertEqual(len(evs_mid), 1)
        self.assertEqual(evs_mid[0].content.get("b"), 2)

    def test_datetime_normalization_and_ttl(self):
        session = self._mk_session("./test_events_datetime_ttl")
        path = "logs/dt_ttl.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)

        import datetime as _dt

        naive = _dt.datetime(2021, 1, 1, 12, 0, 0)
        aware = _dt.datetime(2021, 1, 1, 12, 0, 0, tzinfo=_dt.timezone.utc)
        iso_no_z = "2021-01-01T12:00:00+00:00"

        w.append(
            {EventData.datetime: naive, EventData.content: {}, EventData.labels: {}},
            self._key_paths(),
        )
        w.append(
            {EventData.datetime: aware, EventData.content: {}, EventData.labels: {}},
            self._key_paths(),
        )
        w.append(
            {EventData.datetime: iso_no_z, EventData.content: {}, EventData.labels: {}},
            self._key_paths(),
        )

        evs = r.read_all()
        self.assertTrue(all(e.datetime.endswith("Z") for e in evs[:3]))

        # ttl accept int/str, reject float
        w.append(
            {EventData.content: {"ok": 1}, EventData.labels: {}, EventData.ttl: 3600},
            self._key_paths(),
        )
        w.append(
            {EventData.content: {"ok": 2}, EventData.labels: {}, EventData.ttl: "1h"},
            self._key_paths(),
        )
        evs2 = r.read_all()
        self.assertIn("ttl", evs2[-1].to_safe_dict_LLMS_DONT_USE())
        with self.assertRaises(InvalidEventError):
            w.append(
                {EventData.content: {}, EventData.labels: {}, EventData.ttl: 3.14},
                self._key_paths(),
            )

    def test_labels_and_routes_validation(self):
        session = self._mk_session("./test_events_labels_routes")
        path = "logs/lr.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)

        with self.assertRaises(InvalidEventError):
            w.append(
                {EventData.content: {}, EventData.labels: {1: "x"}}, self._key_paths()
            )  # non-str label key
        with self.assertRaises(InvalidEventError):
            w.append(
                {EventData.content: {}, EventData.labels: {"k": {"v": 1}}},
                self._key_paths(),
            )  # non-primitive label value

        # None allowed as labels value
        w.append(
            {EventData.content: {}, EventData.labels: {"maybe": None}},
            self._key_paths(),
        )
        self.assertIsNone(r.read_all()[-1].labels.get("maybe"))

        # sources single-string normalization
        w.append(
            {EventData.content: {}, EventData.labels: {}, EventData.sources: "svcA"},
            self._key_paths(),
        )
        self.assertEqual(r.read_all()[-1].sources, ["svcA"])

        # reject non-str in sources list
        with self.assertRaises(InvalidEventError):
            w.append(
                {
                    EventData.content: {},
                    EventData.labels: {},
                    EventData.sources: ["ok", 123],
                },
                self._key_paths(),
            )

    def test_stream_parsing_and_file_presence(self):
        # non-existent file
        session = self._mk_session("./test_events_presence")
        r_missing = NDJSONReader(session, "logs/missing.ndjson")
        with self.assertRaises(FileNotFoundError):
            r_missing.read_all()

        # malformed JSON line
        path = "logs/bad.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)
        w.append(
            {EventData.content: {"ok": 1}, EventData.labels: {}}, self._key_paths()
        )
        with session.open(path, "ab") as f:
            f.write(b'{"bad": [}\n')  # deliberately malformed
        with self.assertRaises(StreamReadError):
            r.read_all()

        # empty file returns []
        empty_path = "logs/empty.ndjson"
        with session.open(empty_path, "wb") as f:
            f.write(b"")
        r_empty = NDJSONReader(session, empty_path)
        self.assertEqual(r_empty.read_all(), [])

    def test_unicode_text_search(self):
        session = self._mk_session("./test_events_unicode")
        path = "logs/unicode.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)

        txt = "こんにちは世界 hello 🌍"
        w.append(
            {EventData.content: {"msg": txt}, EventData.labels: {}}, self._key_paths()
        )
        hits = r.search(text="こんにちは")
        self.assertEqual(len(hits), 1)
        self.assertIn("世界", hits[0].content.get("msg", ""))

    def test_append_upsert_require_keys(self):
        session = self._mk_session("./test_events_require_keys")
        path = "logs/require_keys.ndjson"
        w = NDJSONWriter(session, path)

        # append requires key_paths positional argument
        with self.assertRaises(TypeError):
            w.append({EventData.content: {"x": 1}, EventData.labels: {}})  # type: ignore[call-arg]

        # upsert requires key_paths positional argument
        with self.assertRaises(TypeError):
            w.upsert({EventData.content: {"x": 2}, EventData.labels: {}})  # type: ignore[call-arg]

        # wrong type for key_paths -> ValueError
        with self.assertRaises(ValueError):
            w.append({EventData.content: {"x": 3}, EventData.labels: {}}, "bad-type")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            w.upsert({EventData.content: {"x": 4}, EventData.labels: {}}, 123)  # type: ignore[arg-type]

    def test_keyed_append_and_upsert_semantics(self):
        session = self._mk_session("./test_events_keyed_semantics")

        # (a) Duplicate appends with same composite key are allowed
        path_a = "logs/keyed_append_a.ndjson"
        w_a = NDJSONWriter(session, path_a)
        r_a = NDJSONReader(session, path_a)

        w_a.append(
            {EventData.content: {"a": 1}, EventData.labels: {"k": "dup"}},
            self._key_paths(),
        )
        w_a.append(
            {EventData.content: {"a": 2}, EventData.labels: {"k": "dup"}},
            self._key_paths(),
        )

        evs_a = r_a.read_all()
        self.assertEqual(len(evs_a), 2)
        self.assertEqual(evs_a[0].labels.get("k"), "dup")
        self.assertEqual(evs_a[1].labels.get("k"), "dup")

        # (b) Two upserts with the same composite key do not add a row; latest content replaces prior
        path_b = "logs/keyed_upsert_b.ndjson"
        w_b = NDJSONWriter(session, path_b)
        r_b = NDJSONReader(session, path_b)

        # First upsert inserts
        w_b.upsert(
            {EventData.content: {"x": 1}, EventData.labels: {"k": "merge"}},
            self._key_paths(),
        )
        evs_b1 = r_b.read_all()
        self.assertEqual(len(evs_b1), 1)
        nonce1 = evs_b1[0]._writerNonce
        self.assertEqual(evs_b1[0].content.get("x"), 1)

        # Second upsert: union/overwrite provided by caller; writer replaces
        w_b.upsert(
            {EventData.content: {"x": 2, "y": 3}, EventData.labels: {"k": "merge"}},
            self._key_paths(),
        )
        evs_b2 = r_b.read_all()
        self.assertEqual(len(evs_b2), 1)
        self.assertEqual(evs_b2[0].content.get("x"), 2)
        self.assertEqual(evs_b2[0].content.get("y"), 3)
        # Nonce preserved across replace
        self.assertEqual(evs_b2[0]._writerNonce, nonce1)

        # (c) Append, append, then upsert on same key replaces only the nearest-to-EOF match
        path_c = "logs/keyed_last_wins_c.ndjson"
        w_c = NDJSONWriter(session, path_c)
        r_c = NDJSONReader(session, path_c)

        w_c.append(
            {EventData.content: {"v": 1}, EventData.labels: {"k": "lastwin"}},
            self._key_paths(),
        )
        w_c.append(
            {EventData.content: {"v": 2}, EventData.labels: {"k": "lastwin"}},
            self._key_paths(),
        )
        evs_c_before = r_c.read_all()
        self.assertEqual(len(evs_c_before), 2)

        # Upsert updates only the second row
        w_c.upsert(
            {EventData.content: {"v": 9}, EventData.labels: {"k": "lastwin"}},
            self._key_paths(),
        )
        evs_c_after = r_c.read_all()
        self.assertEqual(len(evs_c_after), 2)
        self.assertEqual(evs_c_after[0].content.get("v"), 1)
        self.assertEqual(evs_c_after[1].content.get("v"), 9)

    def test_upsert_keeps_single_row_in_event_and_sidecar(self):
        session = self._mk_session("./test_events_upsert_singleton")
        path = "logs/upsert_singleton.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)
        content_key = str(EventData.content)
        labels_key = str(EventData.labels)
        k_type = "type"
        k_customization = "customization"
        k_full_name = "full_name"
        k_full_email = "full_email"
        k_company = "company"
        key_paths = [f"{content_key}.{k_full_email}"]

        payloads = [
            {
                k_type: "espresso",
                k_customization: ["oat"],
                k_full_name: "jg",
                k_full_email: "a@b.com",
                k_company: "c1",
            },
            {
                k_type: "espresso",
                k_customization: ["oat", "skim"],
                k_full_name: "jg",
                k_full_email: "a@b.com",
                k_company: "c2",
            },
            {
                k_type: "latte",
                k_customization: ["soy"],
                k_full_name: "jg",
                k_full_email: "a@b.com",
                k_company: "c3",
            },
        ]

        for payload in payloads:
            w.upsert({content_key: payload, labels_key: {}}, key_paths)

            evs = r.read_all()
            self.assertEqual(len(evs), 1)
            self.assertEqual(evs[0].content.get(k_company), payload[k_company])

            event_rows = self._count_nonempty_lines(session, path)
            data_rows = self._count_nonempty_lines(session, _data_path(path))
            self.assertEqual(event_rows, 1)
            self.assertEqual(data_rows, 1)

    def test_upsert_updates_sidecar_when_event_line_unchanged(self):
        session = self._mk_session("./test_events_upsert_sidecar_update")
        path = "logs/upsert_sidecar_update.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)

        content_key = str(EventData.content)
        labels_key = str(EventData.labels)
        k_full_email = "full_email"
        k_company = "company"
        k_type = "type"
        key_paths = [f"{content_key}.{k_full_email}"]

        a = {
            k_full_email: "a@b.com",
            k_company: "c1",
            k_type: "espresso",
        }
        b = {
            k_full_email: "a@b.com",
            k_company: "c2",
            k_type: "latte",
        }

        w.upsert({content_key: a, labels_key: {}}, key_paths)
        w.upsert({content_key: b, labels_key: {}}, key_paths)

        evs = r.read_all()
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0].content.get(k_company), "c2")

        event_rows = self._count_nonempty_lines(session, path)
        data_rows = self._count_nonempty_lines(session, _data_path(path))
        self.assertEqual(event_rows, 1)
        self.assertEqual(data_rows, 1)

    def test_delete_removes_event_and_sidecar_counts(self):
        session = self._mk_session("./test_events_delete_counts")
        path = "logs/delete_counts.ndjson"
        w = NDJSONWriter(session, path)
        r = NDJSONReader(session, path)

        content_key = str(EventData.content)
        labels_key = str(EventData.labels)
        k_full_email = "full_email"
        key_paths = [f"{content_key}.{k_full_email}"]

        a = {k_full_email: "a@b.com", "type": "espresso"}
        b = {k_full_email: "b@b.com", "type": "latte"}

        w.upsert({content_key: a, labels_key: {}}, key_paths)
        w.upsert({content_key: b, labels_key: {}}, key_paths)

        event_rows_before = self._count_nonempty_lines(session, path)
        data_rows_before = self._count_nonempty_lines(session, _data_path(path))

        w.delete_by_key({content_key: a, labels_key: {}}, key_paths)

        event_rows_after = self._count_nonempty_lines(session, path)
        data_rows_after = self._count_nonempty_lines(session, _data_path(path))

        self.assertEqual(event_rows_before - 1, event_rows_after)
        self.assertEqual(data_rows_before - 1, data_rows_after)

        evs = r.read_all()
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0].content.get(k_full_email), "b@b.com")

    def test_delete_missing_key_no_effect(self):
        session = self._mk_session("./test_events_delete_missing")
        path = "logs/delete_missing.ndjson"
        w = NDJSONWriter(session, path)

        content_key = str(EventData.content)
        labels_key = str(EventData.labels)
        k_full_email = "full_email"
        key_paths = [f"{content_key}.{k_full_email}"]

        w.upsert({content_key: {k_full_email: "a@b.com"}, labels_key: {}}, key_paths)

        event_rows_before = self._count_nonempty_lines(session, path)
        data_rows_before = self._count_nonempty_lines(session, _data_path(path))

        res = w.delete_by_key({content_key: {k_full_email: "nope@b.com"}, labels_key: {}}, key_paths)
        self.assertEqual(res, -1)

        event_rows_after = self._count_nonempty_lines(session, path)
        data_rows_after = self._count_nonempty_lines(session, _data_path(path))

        self.assertEqual(event_rows_before, event_rows_after)
        self.assertEqual(data_rows_before, data_rows_after)

    def test_delete_null_key_no_effect(self):
        session = self._mk_session("./test_events_delete_null")
        path = "logs/delete_null.ndjson"
        w = NDJSONWriter(session, path)

        content_key = str(EventData.content)
        labels_key = str(EventData.labels)
        k_full_email = "full_email"
        key_paths = [f"{content_key}.{k_full_email}"]

        w.upsert({content_key: {k_full_email: "a@b.com"}, labels_key: {}}, key_paths)

        event_rows_before = self._count_nonempty_lines(session, path)
        data_rows_before = self._count_nonempty_lines(session, _data_path(path))

        res = w.delete_by_key({content_key: {k_full_email: None}, labels_key: {}}, key_paths)
        self.assertEqual(res, -1)

        event_rows_after = self._count_nonempty_lines(session, path)
        data_rows_after = self._count_nonempty_lines(session, _data_path(path))

        self.assertEqual(event_rows_before, event_rows_after)
        self.assertEqual(data_rows_before, data_rows_after)


if __name__ == "__main__":
    unittest.main()
