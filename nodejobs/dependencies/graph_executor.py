# GraphExecutionNode

from typing import Any, Dict, Union
import os, json, hashlib, uuid, time, threading, shutil
import re
from nodejobs.dependencies.BaseSession import DataSession
from nodejobs.dependencies.BaseService import BaseService
from nodejobs.dependencies.field_set import FieldSet

# GraphExecutionNode.set_context({"ReplicateCachingComponent":ReplicateCachingComponent})


def _collect_graph_class_names(graph: Dict[str, Any]) -> list[str]:
    if not isinstance(graph, dict):
        raise Exception("Graph must be a dict to resolve class names.")
    names = []
    for node_id, node in graph.items():
        if node_id == "__outputs" or not isinstance(node, dict):
            continue
        clas = node.get("clas")
        if isinstance(clas, str) and clas:
            names.append(clas)
    return list(dict.fromkeys(names))


def _resolve_class_from_nodes(class_name: str):
    import importlib
    import pkgutil
    import nodejobs.dependencies as nodes_pkg

    class_key = class_name.replace("_", "").lower()
    candidates = []
    for mod in pkgutil.iter_modules(nodes_pkg.__path__):
        mod_name = mod.name
        if (
            mod_name.startswith("_")
            or mod_name.endswith("_test")
            or mod_name.endswith("_tests")
        ):
            continue
        mod_key = mod_name.replace("_", "").lower()
        if class_key.startswith(mod_key) or mod_key.startswith(class_key):
            candidates.append(mod_name)

    for mod_name in candidates:
        module = importlib.import_module(f"nodejobs.dependencies.{mod_name}")
        obj = getattr(module, class_name, None)
        if isinstance(obj, type):
            return obj
    return None


def _auto_context_from_graph(
    graph: Dict[str, Any], ctx: Dict[str, Any]
) -> Dict[str, Any]:
    names = _collect_graph_class_names(graph)
    if not names:
        return {}
    resolved: Dict[str, Any] = {}
    missing = []
    for name in names:
        if name in ctx:
            continue
        cls = _resolve_class_from_nodes(name)
        if cls is None:
            missing.append(name)
        else:
            resolved[name] = cls
    if missing:
        raise Exception(f"Missing class bindings for: {', '.join(missing)}")
    return resolved


_DOTSLASH_TOKEN_RE = re.compile(r"(?P<p>\\./[^\\s\\\"\\'\\)\\]\\}\\>]+)")


def _norm_posix_rel(p: str) -> str:
    # Root-relative convention: always "./" + posix separators
    if p is None:
        return "./"
    p = p.replace("\\\\", "/")
    if not p.startswith("./"):
        p = "./" + p.lstrip("/")
    return p


def _is_under_root(abs_path: str, abs_root: str) -> bool:
    if not abs_path or not abs_root:
        return False
    try:
        abs_path = os.path.abspath(abs_path)
        abs_root = os.path.abspath(abs_root)
        return os.path.commonpath([abs_path, abs_root]) == abs_root
    except Exception:
        return False


def _to_outer_root_relative(abs_path: str, outer_root: str) -> str:
    rel = os.path.relpath(abs_path, outer_root)
    rel = rel.replace(os.sep, "/")
    return outer_root.rstrip("/") + "/" + rel.lstrip("/")


def _rewrite_one_path_string(s: str, run_root: str, outer_root: str) -> str:
    if not isinstance(s, str) or not s:
        return s

    # 0) Root-relative URL-ish paths "/...": if it's not a real FS path,
    # treat it as a URL and lift it under the outer root's basename.
    if (s.startswith("/") or s.startswith("\\")) and not os.path.exists(s):
        base = os.path.basename(os.path.normpath(outer_root or ""))
        if base:
            u = s.replace("\\", "/")
            if u != f"/{base}" and not u.startswith(f"/{base}/"):
                return f"/{base}{u}"
        return s

    # 1) Absolute paths: convert if they live under run_root or outer_root.
    if os.path.isabs(s):
        abs_s = os.path.abspath(s)
        if _is_under_root(abs_s, outer_root):
            return _to_outer_root_relative(abs_s, outer_root)
        if _is_under_root(abs_s, run_root):
            return _to_outer_root_relative(abs_s, outer_root)
        return s


    # 2) Root-relative tokens "./...": treat as relative to run_root, then convert to outer root-relative.
    if s.startswith("./"):
        run_rel = os.path.relpath(run_root, outer_root).replace(os.sep, "/").strip("/")
        rest = s[2:].lstrip("/")
        if run_rel in ("", "."):
            out = outer_root + "/" + rest
            if (out.startswith("/") or out.startswith("\\")) and not os.path.exists(out):
                raise Exception(f"GraphExecution path rewrite produced missing absolute path: {out} (outer_root={outer_root})")
            return out
        if rest.startswith(run_rel + "/"):
            out = outer_root + "/" + rest
            if (out.startswith("/") or out.startswith("\\")) and not os.path.exists(out):
                raise Exception(f"GraphExecution path rewrite produced missing absolute path: {out} (outer_root={outer_root})")
            return out
        out = "./" + rest
        out = out.replace("./", outer_root + "/")
        if (out.startswith("/") or out.startswith("\\")) and not os.path.exists(out):
            raise Exception(f"GraphExecution path rewrite produced missing absolute path: {out} (outer_root={outer_root})")
        return out

    return s


def _rewrite_dot_slash_tokens_in_text(text: str, run_root: str, outer_root: str) -> str:
    if not isinstance(text, str) or "./" not in text:
        return text

    def repl(m: re.Match) -> str:
        tok = m.group("p")
        tok2 = _rewrite_one_path_string(tok, run_root, outer_root)
        if isinstance(tok2, str) and (tok2.startswith("/") or tok2.startswith("\\")) and not os.path.exists(tok2):
            raise Exception(f"GraphExecution token rewrite produced missing absolute path: {tok2} (outer_root={outer_root})")
        return tok2

    return _DOTSLASH_TOKEN_RE.sub(repl, text)


def _normalize_graph_exec_outputs(obj: Any, run_root: str, outer_root: str) -> Any:
    # Walk dict/list/strings; rewrite any "./..." tokens inside blobs of text too.
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: _normalize_graph_exec_outputs(v, run_root, outer_root) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize_graph_exec_outputs(v, run_root, outer_root) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_normalize_graph_exec_outputs(v, run_root, outer_root) for v in obj)
    if isinstance(obj, str):
        s2 = _rewrite_one_path_string(obj, run_root, outer_root)
        return _rewrite_dot_slash_tokens_in_text(s2, run_root, outer_root)
    return obj


def _prune_undefined_nodes_and_seed_feature(graph: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Skip UNDEFINED nodes during execution.
    Use their embedded node['output'] dict as the already-computed METHOD output.
    """
    if not isinstance(graph, dict):
        raise Exception("Graph must be a dict")

    seed_feature: Dict[str, Any] = {}
    pruned: Dict[str, Any] = {}

    for node_id, node in graph.items():
        if node_id == "__outputs":
            pruned[node_id] = node
            continue
        if not isinstance(node, dict):
            pruned[node_id] = node
            continue

        if node.get("clas") != "UNDEFINED":
            pruned[node_id] = node
            continue

        out = node.get("output")
        if not isinstance(out, dict):
            raise Exception(f"UNDEFINED node has no dict output: {node_id}")

        # Match exported graphs that reference UNDEFINED outputs via __ref/<node>/METHOD/<field>
        seed_feature[node_id] = {"METHOD": out}

    return pruned, seed_feature


class GraphExecutionNode(FieldSet):
    """
    Executes a ProcessingGraph and caches the result.
    Args:
      - graph: dict (inline graph)
      - overwrite: bool = False
    """

    p_query = "query"
    ctx = None

    def _get_default_fields(self):
        return {
            "execution_graph_path": {"type": "file"},
            "overwrite": {"type": "bool", "optional": True, "default": True},
            "overwrite_sub": {"type": "bool", "optional": True, "default": False},
            "cache_key": {"type": "str", "optional": True},
        }

    def _normalize_outputs_map_arg(self, outputs_map):
        if outputs_map is None:
            return None
        if isinstance(outputs_map, list) and outputs_map:
            return outputs_map[0]
        return outputs_map

    def _inject_outputs_map(self, result, outputs_map, label="outputs_map", protected_keys=None):
        outputs_map = self._normalize_outputs_map_arg(outputs_map)
        if outputs_map is None:
            return
        if not isinstance(outputs_map, dict):
            raise Exception(f"GraphExecutionNode {label} must be a dict.")

        if protected_keys is None:
            protected_keys = set()
        elif not isinstance(protected_keys, set):
            protected_keys = set(protected_keys)

        for alias, ref in outputs_map.items():
            if not isinstance(ref, list) or len(ref) < 4 or ref[0] != "__ref":
                raise Exception(f"GraphExecutionNode {label} entry must be a __ref list.")
            node_id, method, slot = ref[1], ref[2], ref[3]
            if not isinstance(node_id, str) or not isinstance(method, str) or not isinstance(slot, str):
                raise Exception(f"GraphExecutionNode {label} __ref missing node/method/slot.")
            if alias in protected_keys:
                raise Exception(f"GraphExecutionNode {label} alias collides with protected key: {alias}")
            try:
                value = result[node_id][method][slot]
            except Exception as exc:
                raise Exception(
                    f"GraphExecutionNode {label} failed to resolve {alias} -> {node_id}.{method}.{slot}"
                ) from exc
            # Allow re-binding aliases (idempotent on cache-hit); do not allow clobbering protected keys.
            result[alias] = value

    def _build_graph_fields(self, graph: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        if not isinstance(graph, dict):
            return {}
        reserved = set(self._get_default_fields().keys()) | {"io_mode"}
        fields: Dict[str, Dict[str, str]] = {}
        counts: Dict[str, int] = {}

        def add_field(
            full_path: str, alias: str = None, field_type: str = "str"
        ) -> None:
            if not isinstance(full_path, str) or "." not in full_path:
                return
            base = (
                alias
                if isinstance(alias, str) and alias
                else full_path.rsplit(".", 1)[-1]
            )
            count = counts.get(base, 0) + 1
            counts[base] = count
            name = base if count == 1 else f"{base}_{count}"
            while name in reserved or name in fields:
                count += 1
                counts[base] = count
                name = f"{base}_{count}"
            fields[name] = {"type": field_type, "path": full_path}

        graph_keys = set(graph.keys())

        def is_external_ref(val) -> bool:
            if not isinstance(val, list) or len(val) < 2:
                return False
            if val[0] != "__ref":
                return False
            ref_target = val[1]
            return ref_target not in graph_keys

        def add_field_from_segments(node_id: str, segments: list[str]) -> None:
            full_path = node_id + "." + ".".join(segments)
            alias = segments[-1]
            if alias.isdigit() and len(segments) > 1:
                alias = f"{segments[-2]}_{segments[-1]}"
            add_field(full_path, alias)

        def walk(value, segments, node_id):
            if is_external_ref(value):
                add_field_from_segments(node_id, segments)
                return
            if isinstance(value, dict):
                for k, v in value.items():
                    walk(v, segments + [k], node_id)
                return
            if isinstance(value, list):
                for i, v in enumerate(value):
                    walk(v, segments + [str(i)], node_id)
                return

        for node_id, node in graph.items():
            if node_id == "__outputs":
                continue
            if not isinstance(node, dict):
                continue
            deps = node.get("dependencies")
            if not isinstance(deps, dict):
                continue
            for command, dep in deps.items():
                walk(dep, [command], node_id)

        return fields

    def _apply_field_values_to_graph(
        self, graph: Dict[str, Any], values, alias_fields
    ) -> Dict[str, Any]:
        """
        Apply FieldSet values into the graph using simple JSON-path style keys.

        Any key containing a '.' is treated as a dot-separated path inside `graph`,
        with 'dependencies' automatically inserted after the first segment, e.g.:
            "nd_first.run_prediction.query.prompt"
        becomes:
            "nd_first.dependencies.run_prediction.query.prompt"
        """
        # values = self.get_data()
        if not isinstance(values, dict):
            return graph
        if not isinstance(alias_fields, dict):
            raise Exception("Field map missing; call get_fields before execute.")

        for key, value in values.items():

            if not isinstance(key, str):
                continue
            if key.startswith("_"):
                continue
            if key in ("execution_graph_path", "io_mode", "overwrite", "overwrite_sub", "cache_key"):
                continue
            if "." not in key:
                spec = alias_fields.get(key)
                if not isinstance(spec, dict) or not isinstance(spec.get("path"), str):
                    raise Exception(
                        f"Unknown field alias: {key}. Call get_fields to refresh."
                    )
                key = spec["path"]

            first_dot = key.find(".")
            path_key = key[:first_dot] + ".dependencies." + key[first_dot + 1 :]
            parts = path_key.split(".")

            cur: Any = graph
            traversed = []

            for seg in parts[:-1]:
                traversed.append(seg)
                if isinstance(cur, list):
                    if not seg.isdigit():
                        raise Exception(
                            f"Graph field mapping failed for key '{key}' (expanded '{path_key}'): "
                            f"list segment '{seg}' is not an index."
                        )
                    idx = int(seg)
                    if idx < 0 or idx >= len(cur):
                        raise Exception(
                            f"Graph field mapping failed for key '{key}' (expanded '{path_key}'): "
                            f"list index {idx} out of range."
                        )
                    cur = cur[idx]
                    continue

                if not isinstance(cur, dict) or seg not in cur:
                    partial_path = ".".join(traversed)
                    available = (
                        list(cur.keys())
                        if isinstance(cur, dict)
                        else f"<not a dict: {type(cur).__name__}>"
                    )
                    raise Exception(
                        f"Graph field mapping failed for key '{key}' (expanded '{path_key}'): "
                        f"mismatch at segment '{seg}' (partial path '{partial_path}'). "
                        f"Available keys at this level: {available}"
                    )
                cur = cur[seg]

            last = parts[-1]
            if isinstance(cur, list):
                if not last.isdigit():
                    raise Exception(
                        f"Graph field mapping failed for key '{key}' (expanded '{path_key}'): "
                        f"list segment '{last}' is not an index."
                    )
                idx = int(last)
                if idx < 0 or idx >= len(cur):
                    raise Exception(
                        f"Graph field mapping failed for key '{key}' (expanded '{path_key}'): "
                        f"list index {idx} out of range."
                    )
                cur[idx] = value
            elif isinstance(cur, dict):
                cur[last] = value
            else:
                partial_path = ".".join(traversed)
                raise Exception(
                    f"Graph field mapping failed for key '{key}' (expanded '{path_key}'): "
                    f"cannot assign into non-dict at '{partial_path}' (type={type(cur).__name__})."
                )

        return graph

    def load_graph(self, graph_path):
        abs_path = graph_path
        if not os.path.isabs(abs_path):
            abs_path = os.path.join(self.data_session.get_root(), abs_path)

        if not os.path.isfile(abs_path):
            return None

        with open(abs_path, "r", encoding="utf-8") as f:
            graph = json.load(f)
        return graph

    def get_fields(self, query=None):
        # Let FieldSet ensure default schema exists (and cached)
        try:
            fields = super().get_fields()
        except Exception:
            fields = None

        if not isinstance(fields, dict) or not fields:
            res = self.set_fields(self._get_default_fields())
            if isinstance(res, dict) and "error" in res:
                return res
            fields = super().get_fields()

        base_fields = fields
        if isinstance(base_fields, dict) and "error" in base_fields:
            return base_fields

        # if query == None:
        #     return base_fields
        # query = dict(query)
        if query is None:
            query = self.get_data()
            if not isinstance(query, dict):
                return base_fields
        else:
            query = dict(query)

        graph_path = query.get("execution_graph_path")
        if not isinstance(graph_path, str) or len(graph_path) == 0:
            return base_fields
        graph = self.load_graph(graph_path)
        if graph == None:
            return base_fields

        if not isinstance(graph, dict):
            return base_fields

        dynamic_fields: Dict[str, Dict[str, str]] = self._build_graph_fields(graph)

        merged = dict(base_fields) if isinstance(base_fields, dict) else {}
        for k, v in dynamic_fields.items():
            if k not in merged:
                merged[k] = v

        # Persist full schema (user-defined + dynamic + defaults); do not overwrite with only dynamic fields.
        self.set_fields(merged)
        return super().get_fields()

    @classmethod
    def set_context(cls, ctx):
        cls.ctx = ctx

    @classmethod
    def add_context(cls, ctx):
        if cls.ctx == None:
            cls.ctx = {}
        cls.ctx.update(ctx)
        # cls.ctx = ctx

    def get_current_cache_key(self, overwrite, cache_key):
        if cache_key is not None:
            return cache_key
        # print("searching ..")
        root = self.get_rel_cache_path()
        # print(root)
        best = None
        for name in os.listdir(root):
            if not name.endswith(".json"):
                continue
            stem = name[:-5]  # strip ".json"
            try:
                int(stem, 16)  # hex key
            except ValueError:
                continue
            # print(stem)
            full_path = os.path.join(root, name)
            if not os.path.isfile(full_path):
                continue
            if best is None or stem < best:  # lexicographic smallest
                best = stem
        cache_key = best
        return cache_key

    def execute(
        self, graph, values=None, cache_key=None, overwrite=False, overwrite_sub=False
    ) -> Dict[str, Any]:
        graph_abs_path = None
        if isinstance(graph, str):
            graph_abs_path = graph
            if not os.path.isabs(graph_abs_path):
                graph_abs_path = os.path.join(
                    self.data_session.get_root(), graph_abs_path
                )

            # Safety: if the export graph path is outside this node's cache namespace,
            # copy it into cache and execute the local copy. This prevents run/artifact
            # collisions when multiple nodes reuse the same exported subgraph path.
            cache_root = os.path.abspath(self.get_cache_data_session().get_root())
            graph_abs = os.path.abspath(graph_abs_path)
            try:
                in_cache = os.path.commonpath([cache_root, graph_abs]) == cache_root
            except Exception:
                in_cache = False

            if not in_cache:
                exec_dir = os.path.join(cache_root, "executions")
                os.makedirs(exec_dir, exist_ok=True)
                stem = os.path.splitext(os.path.basename(graph_abs))[0]
                digest = hashlib.sha1(graph_abs.encode("utf-8")).hexdigest()[:12]
                local_graph_abs_path = os.path.join(exec_dir, f"{stem}.{digest}.json")

                # Always overwrite. Use an atomic replace so concurrent callers don't
                # leave partial files behind.
                tmp_path = os.path.join(
                    exec_dir,
                    f".{os.path.basename(local_graph_abs_path)}.{uuid.uuid4().hex}.tmp",
                )
                shutil.copy2(graph_abs_path, tmp_path)
                os.replace(tmp_path, local_graph_abs_path)
                graph_abs_path = local_graph_abs_path
            graph = self.load_graph(graph_abs_path)
            if graph is None:
                raise Exception(f"Could not load graph from path: {graph_abs_path}")
        assert isinstance(graph, dict), "Could not load the graph"

        graph, seed_feature = _prune_undefined_nodes_and_seed_feature(graph)

        wrapper_outputs_map = None
        if isinstance(values, dict) and "outputs_map" in values:
            values = dict(values)
            wrapper_outputs_map = values.pop("outputs_map", None)

        if values is not None and len(values) > 0:
            if graph_abs_path:
                fields = self.get_fields({"execution_graph_path": graph_abs_path})
            else:
                fields = self.get_fields()
            if isinstance(fields, dict) and "error" in fields:
                raise Exception(f"get_fields failed: {fields['error']}")
            alias_fields = self.load_cache("fields")
            if not isinstance(alias_fields, dict):
                raise Exception("Field map missing after get_fields.")
            # Input boundary: resolve URL-ish root paths like "/nd_xxx/..." under current DataSession root.
            values = self.resolve_session_paths(values)
            #values = _normalize_graph_exec_outputs(values, self.data_session.get_root(), self.data_session.get_root())
            graph = self._apply_field_values_to_graph(graph, values, alias_fields)

        if overwrite_sub:
            for node_id, n in graph.items():
                if node_id == "__outputs" or not isinstance(n, dict):
                    continue
                deps = n.get("dependencies")
                if not isinstance(deps, dict):
                    continue
                for _, dep in deps.items():
                    if not isinstance(dep, dict):
                        continue
                    cp = dep.get("call_params")
                    if isinstance(cp, dict):
                        cp["overwrite"] = True
        # print(json.dumps(graph,indent=2))

        bundle_root = None
        if graph_abs_path:
            base = os.path.splitext(os.path.basename(graph_abs_path))[0]
            bundle_root = os.path.join(os.path.dirname(graph_abs_path), base)
            os.makedirs(bundle_root, exist_ok=True)

        run_root = (
            bundle_root if bundle_root else self.get_cache_data_session().get_root()
        )
        for node_id, n in graph.items():
            if node_id == "__outputs":
                continue
            n["settings"]["data_session"]["root"] = run_root

        # print(f"RUNNING {cache_key}")
        # print("-"+cache_key)
        if cache_key == None:
            cache_key = "unversioned_output"

        def _guard_return_shape(guard: dict) -> Dict[str, Any]:
            # Only return the run-guard marker. Never overwrite output aliases
            # with the guard payload; that poisons downstream refs and the UI.
            return {"__run_guard": guard}

        guard_prefix = "executions/runs"
        guard = self.load_run_guard(cache_key, prefix=guard_prefix)
        if isinstance(guard, dict):
            return _guard_return_shape(guard)
        # print(f"RUNNING2 {cache_key}")
        cached = self.load_processed_cache(
            {"overwrite": overwrite}, cache_key, prefix="executions/runs"
        )
        if not cached:
            cached = self.load_processed_cache(
                {"overwrite": overwrite}, cache_key, prefix="execution"
            )
        if cached:
            # print("----> CACHE HIT")
            res = cached["result"]
            self._inject_outputs_map(res, wrapper_outputs_map, label="outputs_map", protected_keys=set(graph.keys()))
            # Output boundary: normalize paths to outer-root-relative (including "./" tokens inside text blobs).
            outer_root = self.data_session.get_root()
            res = _normalize_graph_exec_outputs(res, run_root, outer_root)
            return res
        # print("----> CACHE MISS")

        owner_uuid = uuid.uuid4().hex
        acquired, guard = self.try_acquire_run_guard(
            cache_key, prefix=guard_prefix, owner_uuid=owner_uuid, ttl_sec=600
        )
        if not acquired:
            return _guard_return_shape(guard)

        stop_evt = threading.Event()

        def _hb():
            while not stop_evt.wait(2.0):
                try:
                    self.update_run_guard(cache_key, guard_prefix, owner_uuid, extend_ttl_sec=600)
                except Exception:
                    pass

        threading.Thread(target=_hb, daemon=True).start()

        # Execute once, cache result
        if self.ctx == None:
            # Never allow stdout issues (BrokenPipeError) to crash graph execution.
            try:
                self.log("Default Context!")
            except Exception:
                pass
            from processing_graph.BaseProcessor import BaseProcessor

            ctx = BaseProcessor.build_context()
        else:
            # print("Loaded Context!")
            ctx = self.ctx

        if ctx is None:
            ctx = {}
        ctx = dict(ctx)
        ctx.update(_auto_context_from_graph(graph, ctx))

        prev_cwd = os.getcwd()
        os.chdir(run_root)
        try:
            from processing_graph.BaseProcessor import BaseProcessor

            proc = BaseProcessor(graph, context=ctx)
            proc.log = self.log
            result = proc.process(seed_feature)
        except Exception as exc:
            try:
                self.update_run_guard(
                    cache_key,
                    guard_prefix,
                    owner_uuid,
                    status="error",
                    message=str(exc),
                    extend_ttl_sec=60,
                )
                # Ensure retries are not blocked by an error guard.
                try:
                    self.release_run_guard(cache_key, guard_prefix, owner_uuid, keep=False)
                except Exception:
                    pass
            finally:
                raise
        finally:
            stop_evt.set()
            os.chdir(prev_cwd)

        outputs_map = graph.get("__outputs")
        if isinstance(outputs_map, dict):
            for alias, ref in outputs_map.items():
                if not isinstance(ref, list) or len(ref) < 4 or ref[0] != "__ref":
                    raise Exception(
                        "GraphExecutionNode __outputs entry must be a __ref list."
                    )
                node_id, method, slot = ref[1], ref[2], ref[3]
                if (
                    not isinstance(node_id, str)
                    or not isinstance(method, str)
                    or not isinstance(slot, str)
                ):
                    raise Exception(
                        "GraphExecutionNode __outputs __ref missing node/method/slot."
                    )
                if alias in result:
                    raise Exception(
                        f"GraphExecutionNode __outputs alias collides with node key: {alias}"
                    )
                try:
                    value = result[node_id][method][slot]
                except Exception as exc:
                    raise Exception(
                        f"GraphExecutionNode __outputs failed to resolve {alias} -> {node_id}.{method}.{slot}"
                    ) from exc
                result[alias] = value

        self._inject_outputs_map(result, wrapper_outputs_map, label="outputs_map", protected_keys=set(graph.keys()))

        payload = {"ok": True, "cache_key": cache_key, "result": result}
        # print(f"SAVING {cache_key}: {payload}")
        self.save_cache(cache_key, payload, prefix="executions/runs")
        self.release_run_guard(cache_key, guard_prefix, owner_uuid, keep=False)

        # Runtime filter: normalize only on return, never in saved cache.
        outer_root = self.data_session.get_root()
        return _normalize_graph_exec_outputs(payload["result"], run_root, outer_root)


class GraphExecutionService(BaseService):
    ctx = None

    @classmethod
    def set_context(cls, ctx):
        cls.ctx = ctx

    @classmethod
    def get_command_map(cls):
        return {
            "execute": {"required_args": ["graph"], "method": cls.execute},
            "execute_batch": {
                "required_args": ["graph", "cache_path", "count"],
                "method": cls.execute_batch,
            },
        }

    @classmethod
    def flatten_graph_results(cls, result_dict):
        """
        Flatten node/function results of the form:
        { node_id: { func_name: value_or_dict } }
        into:
        { "node.func[.inner]": value }
        """
        flat = {}

        for node_id, node_body in result_dict.items():
            # node_body is expected to be { func_name: value }
            if not isinstance(node_body, dict):
                flat[node_id] = node_body
                continue

            for func_name, func_val in node_body.items():
                base_key = f"{node_id}.{func_name}"

                if isinstance(func_val, dict):
                    # One more level: { inner_key: inner_val }
                    for inner_key, inner_val in func_val.items():
                        flat[f"{base_key}.{inner_key}"] = inner_val
                else:
                    # Primitive / list: use just node.func
                    flat[base_key] = func_val

        return flat

    @classmethod
    def execute_batch(
        cls,
        graph: dict,
        count: int = None,
        overwrite: bool = False,
        root: str = None,
        cache_path: str = "execution",
    ):
        """
        Run `execute` repeatedly, assigning incrementing cache_path suffixes.
        - `count` is required; use `execute` for single runs.
        - Suffix is always 4 digits (e.g., ..._0025), regardless of previous digit width.
        - Retries each iteration up to 3 times on failure before raising.
        Returns: list of results from each successful `execute`.
        """
        if count is None:
            raise ValueError(
                "`count` is required for execute_batch; use `execute` for single runs."
            )

        if root is None:
            root = os.getcwd()

        # Determine search directory and prefix for suffix matching.
        is_abs = os.path.isabs(cache_path)
        base_dir = os.path.dirname(cache_path.rstrip(os.sep)) if is_abs else root
        prefix = os.path.basename(cache_path.rstrip(os.sep)) if is_abs else cache_path

        # Find the largest existing numeric suffix.
        try:
            entries = os.listdir(base_dir)
        except FileNotFoundError:
            entries = []

        import re

        pat = re.compile(r"^" + re.escape(prefix) + r"(\d+)$")
        max_idx = -1
        for name in entries:
            m = pat.match(name)
            if m:
                try:
                    idx = int(m.group(1))
                    if idx > max_idx:
                        max_idx = idx
                except ValueError:
                    pass

        start_idx = max_idx + 1
        results = []
        count = int(count)
        for i in range(count):
            idx = start_idx + i
            next_name = f"{prefix}{idx:04d}"
            next_cache_path = os.path.join(base_dir, next_name) if is_abs else next_name

            last_exc = None
            for attempt in range(3):
                try:
                    # print(f"executing... {next_cache_path}")
                    res = cls.execute(
                        graph=graph,
                        overwrite=overwrite,
                        root=root,
                        cache_path=next_cache_path,
                    )
                    #
                    #
                    #
                    """
                    "result": 
                                {
                                    "nd_first": 
                                    {
                                        "run_prediction": 
                                        {
                                            "Output": [
                                                "/Users/computercomputer/justinops/art_lab/doc_workflows/graph/Resources/GraphDataRoot/defaultSave/nd_a65ff606/nd_first/output_001.jpg"
                                                ]
                                        }

                                    }
                    ,
                                    "nd_second": 
                                    {
                                        "run_prediction": 
                                        {
                                            "Output": [
                                                "/Users/computercomputer/justinops/art_lab/doc_workflows/graph/Resources/GraphDataRoot/defaultSave/nd_a65ff606/nd_second/output_001.jpg"
                                                ],
                                            "Output3": 17,                                               
                                            "Output2": {"data":"example"}                                              
                                        }

                                    },
                                    "nd_third": 
                                    {
                                        "run_prediction": 25

                                    }                                    

                                }                    
                    INSTEAD should be in this format 
                    {
                    "nd_first.run_prediction.Output":  [
                                                "/Users/computercomputer/justinops/art_lab/doc_workflows/graph/Resources/GraphDataRoot/defaultSave/nd_a65ff606/nd_first/output_001.jpg"
                                                ],
                    "nd_second.run_prediction.Output":  [
                                                "/Users/computercomputer/justinops/art_lab/doc_workflows/graph/Resources/GraphDataRoot/defaultSave/nd_a65ff606/nd_second/output_001.jpg"
                                                ],
                    "nd_second.run_prediction.Output3":  17,
                    "nd_second.run_prediction.Output2":  {"data":"example"}  ,
                    "nd_third.run_prediction": 25  ,

                    }
                    """
                    res = cls.flatten_graph_results(res)
                    results.append(res)
                    break
                except Exception as e:
                    last_exc = e
                    if attempt == 2:
                        raise last_exc
                    else:
                        pass
                        # print(f"retrying..{attempt},  {next_cache_path}")

        return results

    @classmethod
    def execute(
        cls,
        graph: dict,
        overwrite: bool = False,
        root: str = None,
        cache_key: str = None,
        cache_path: str = "execution",
    ):
        if root == None:
            root = os.getcwd()

        GraphExecutionNode.set_context(cls.ctx)

        node = GraphExecutionNode(
            {
                "data_session": DataSession(
                    {DataSession.f_root: root, DataSession.f_unlocked: True}
                ),
                "cache_path": cache_path,
            }
        )
        return node.execute(
            {"graph": graph, "overwrite": overwrite}, cache_key=cache_key
        )


if __name__ == "__main__":
    GraphExecutionService.run_cli()

    # python3 GraphExecutionService.py execute graph=[[469dd46ee9da4b47a963bfdc97fad232_naming.json]] cache_dir="469dd46ee9da4b47a963bfdc97fad232_exec_001"  overwrite=true
    # or choose a dir:
    # python3 GraphExecutionService.py execute graph=[[469dd46ee9da4b47a963bfdc97fad232_naming.json]] root="$(pwd)/executions"
