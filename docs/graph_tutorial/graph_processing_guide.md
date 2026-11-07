# Graph Processing Guide (v1)

This guide explains how graph processing works across two related codebases:

1. `processing_graph`: the core typed graph engine and executor (`BaseProcessor`).
2. `nodejobs`: utilities used by nodes (typed data, caching, job logging).

The intent is to document:

1. How a JSON graph is structured.
2. How graphs are executed and cached.
3. How nodes are resolved and overridden (dependency injection).
4. How results and artifacts are versioned and consolidated.

## Repos And Roles

### `processing_graph` (core engine)

Primary files:

- `/Users/computercomputer/justinops/processing_graph/processing_graph/BaseProcessor.py`
- `/Users/computercomputer/justinops/processing_graph/processing_graph/ProcessingNode.py`

What it provides:

- A declarative graph model where nodes have settings + dependencies.
- Runtime validation and dependency resolution.
- `__ref` semantics: dependencies can reference outputs from earlier nodes.

### `nodejobs` (utilities)

Primary files:

- `/Users/computercomputer/justinops/nodejobs/nodejobs/dependencies/CachingComponent.py`
- `/Users/computercomputer/justinops/nodejobs/nodejobs/jobs.py`

What it provides:

- `CachingComponent`: a structured cache namespace under a `DataSession` root (`save_cache_path` / `load_cache_path`).
- `Jobs`: job execution and logging (stdout/stderr + job record JSON).

## Graph JSON Structure

In the `nodejobs` ecosystem, a graph is typically a JSON object (dict) with:

- Keys: node ids (`"nd_first"`, `"nd_second"`, etc).
- Values: node specs.

Common node spec fields:

- `"name"`: a human-readable label.
- `"clas"`: a string class name to instantiate/execute.
- `"settings"`: configuration (commonly includes `data_session` and `cache_path`).
- `"dependencies"`: nested object describing inputs per method.

### Dependency References (`__ref`)

Dependencies can reference upstream node outputs using a list form:

```json
["__ref", "<src_node_id>", "<method>", "<slot>"]
```

Conceptually:

- `src_node_id`: which node produced the value.
- `method`: which method on that node produced the output.
- `slot`: which output field to read.

This encoding is used by the executor and engine to connect edges without relying on runtime pointers.

## Practical Examples (BaseProcessor + CachingComponent Nodes)

This section is intentionally practical. Every example node:

1. Subclasses `CachingComponent`
2. Uses `load_cache_path` / `save_cache_path`
3. Supports `overwrite` as a **primary bool parameter** on **every function**
4. Supports `cache_key` as a **primary parameter** on **every function**
5. Uses **per-function** cache keys passed from the graph (no implicit `state/<fn>.json` paths)

### Node Pattern (Repeatable)

Node rules:

- Each callable graph function MUST accept: `overwrite: bool = False, cache_key: str | None = None`.
- `cache_key` is used DIRECTLY as the argument to `load_cache_path(cache_key)` / `save_cache_path(cache_key, ...)`.
- Cache is per-function because the graph provides a different cache_key per function.

Required pattern inside each function:

```py
if cache_key is None:
    raise Exception("cache_key is required (must be unique per function call)")
if not overwrite:
    # Default load mode is binary-safe: bytes are rehydrated automatically.
    # (raw_mode=True bypasses the object hooks and should be avoided unless you want raw JSON.)
    cached = self.load_cache_path(cache_key)
    if cached is not None:
        return cached
out = {...}
self.save_cache_path(cache_key, out)
return out
```

### Example Nodes

```py
import json
import random

from nodejobs.dependencies.CachingComponent import CachingComponent


class RandomNode(CachingComponent):
    """Emit a random value; cached per function call."""

    def draw(self, seed: int | None = None, overwrite: bool = False, cache_key: str | None = None):
        if cache_key is None:
            raise Exception("cache_key is required for RandomNode.draw")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("value"), (int, float)):
                return {"value": float(cached["value"])}

        if seed is None:
            v = random.random()
        else:
            v = random.Random(int(seed)).random()

        out = {"value": float(v)}
        self.save_cache_path(cache_key, out)
        return out


class MovingAverageNode(CachingComponent):
    """Stateful node: caches per function (one file)."""

    def calc(self, point: float, overwrite: bool = False, cache_key: str | None = None):
        if cache_key is None:
            raise Exception("cache_key is required for MovingAverageNode.calc")

        state = None if overwrite else self.load_cache_path(cache_key)
        past = state.get("past_number") if isinstance(state, dict) else None
        if past is None:
            past = float(point)
        past = 0.9 * float(past) + 0.1 * float(point)
        # Store both state + returned slot to simplify downstream tooling.
        self.save_cache_path(cache_key, {"past_number": past, "value": past})
        return {"value": past}

    def buffer_average(self, buffer: list[float], overwrite: bool = False, cache_key: str | None = None):
        if cache_key is None:
            raise Exception("cache_key is required for MovingAverageNode.buffer_average")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and "value" in cached:
                return cached

        buf = list(buffer or [])
        v = (sum(buf) / len(buf)) if buf else 0.0
        out = {"value": v}
        self.save_cache_path(cache_key, out)
        return out
```

### Example Graph Definition (dict)

This is a BaseProcessor-compatible graph dict. Notes:

- `clas` is a **string**, resolved via the `context` dict you pass to BaseProcessor.
- Each node must have `settings.data_session` and `settings.cache_path` so `CachingComponent` can write.
- Critically: `data_session` is **per node** (declared in each node's `settings`).
- In this guide, the `graph_data_root` is common across all nodes.
- Specify `data_session` inline as a fully specified dict (a BaseData instance), e.g.
  - `"data_session": {"root": "/tmp/some_run", "unlocked": True}`
- Every function includes `overwrite` and `cache_key` dependencies as primary parameters.

```py
def make_graph(*, graph_data_root: str) -> dict:
    # data_session is consumed by CachingComponent during node instancing.
    # BaseProcessor passes settings into node constructors; it does not manage the session itself.
    assert isinstance(graph_data_root, str) and graph_data_root

    return {
        "Random": {
            "name": "Random",
            "clas": "RandomNode",
            "settings": {
                "data_session": {"root": graph_data_root, "unlocked": True},
                "cache_path": "Random",
            },
            "dependencies": {
                "draw": {
                    "seed": ["__ref", "rng_seed"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "random_cache_key"],
                }
            },
        },
        "MovingAverage": {
            "name": "MovingAverage",
            "clas": "MovingAverageNode",
            "settings": {
                "data_session": {"root": graph_data_root, "unlocked": True},
                "cache_path": "MovingAverage",
            },
            "dependencies": {
                "calc": {
                    "point": ["__ref", "Random", "draw", "value"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "calc_cache_key"],
                },
                "buffer_average": {
                    "buffer": ["__ref", "buffer_in"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "avg_cache_key"],
                },
            },
        },
    }
```

### Execute With BaseProcessor

```py
from processing_graph.BaseProcessor import BaseProcessor

CTX = {
    "RandomNode": RandomNode,
    "MovingAverageNode": MovingAverageNode,
}

run_root = "/tmp/graph_run_demo"  # choose a real directory

# Per-node sessions are specified inline as dicts (common root across nodes).
GRAPH = make_graph(graph_data_root=run_root)

bp = BaseProcessor(GRAPH, context=CTX)

feature = {
    "buffer_in": [0.1, 0.2, 0.3],
    "rng_seed": 1337,
    "overwrite": False,
    # IMPORTANT: cache_key is used directly as a cache path. Make these unique per function call.
    "random_cache_key": "state/Random.draw.sessionA.json",
    "calc_cache_key": "state/MovingAverage.calc.sessionA.json",
    "avg_cache_key": "state/MovingAverage.buffer_average.sessionA.json",
}
out = bp.process(feature)
random_value = out["Random"]["draw"]["value"]
ema_value = out["MovingAverage"]["calc"]["value"]
avg_value = out["MovingAverage"]["buffer_average"]["value"]
print(random_value, ema_value, avg_value)

# Force all functions to recompute
feature2 = dict(feature)
feature2["overwrite"] = True
feature2["calc_cache_key"] = "state/MovingAverage.calc.sessionA.overwrite.json"
out2 = bp.process(feature2)
ema_value_2 = out2["MovingAverage"]["calc"]["value"]
avg_value_2 = out2["MovingAverage"]["buffer_average"]["value"]
print(ema_value_2, avg_value_2)
```

### Overriding A Node Implementation (DI)

Because `clas` is a string, swapping implementations is just swapping the `context` binding:

```py
class MovingAverageNodeV2(MovingAverageNode):
    def calc(self, point: float, overwrite: bool = False, cache_key: str | None = None):
        if cache_key is None:
            raise Exception("cache_key is required for MovingAverageNodeV2.calc")
        cached = None if overwrite else self.load_cache_path(cache_key)
        if isinstance(cached, dict) and "value" in cached:
            return cached
        out = {"value": float(point)}
        self.save_cache_path(cache_key, out)
        return out

CTX2 = {"RandomNode": RandomNode, "MovingAverageNode": MovingAverageNodeV2}
bp2 = BaseProcessor(GRAPH, context=CTX2)
```

## Tutorial: Dataset Processing Graph (Load -> Stats -> Scatter)

This tutorial is a dataset processor example:

1. The unit of work is a `list[dict]` dataset loaded from disk.
2. Each node consumes and produces dataset-level structures.
3. Caching applies at the dataset level (per function call + `cache_key`).

Nodes:

1. `LoadRecordFromFile`: loads a JSON file containing `list[dict]`, validates required keys, caches result.
2. `PandasStats`: consumes the dataset from an ascendant node, computes mean/std for selected fields, caches derived stats.
3. `GraphScatterImage`: consumes the dataset plus the mean/std output (fan-in), renders scatter + mean/std ellipses, caches PNG bytes.

### Binary-Safe Cache I/O (Important)

`CachingComponent.save_cache_path(...)` and `CachingComponent.load_cache_path(...)` are binary-safe.
If the cached payload contains `bytes` (like a PNG), the cache layer stores the binary as a blob reference under the cache
namespace and rehydrates it automatically on load.

This is intended to streamline development: you can cache and pass rich payloads without inventing ad-hoc serialization.

Do not use `raw_mode=True` when you want binary rehydration; it bypasses the object hooks.

### Example Dataset File (10 Records)

`points_10.json`:

```json
[
  {"x": 0.0, "y": 0.0},
  {"x": 1.0, "y": 1.2},
  {"x": 2.0, "y": 1.9},
  {"x": 3.0, "y": 3.1},
  {"x": 4.0, "y": 3.9},
  {"x": 5.0, "y": 5.2},
  {"x": 6.0, "y": 6.1},
  {"x": 7.0, "y": 6.8},
  {"x": 8.0, "y": 8.2},
  {"x": 9.0, "y": 9.1}
]
```

### Graph JSON File

This graph demonstrates automatic dependency resolution with fan-in via `__ref`:

1. `Stats.describe.records` reads `Load.load.records`.
2. `Scatter.plot.records` reads `Load.load.records`.
3. `Scatter.plot.stats` reads `Stats.describe.stats`.

`data_processing_graph.json`:

```json
{
  "Load": {
    "name": "Load",
    "clas": "LoadRecordFromFile",
    "settings": {
      "data_session": {"root": "<<GRAPH_DATA_ROOT>>", "unlocked": true},
      "cache_path": "Load"
    },
    "dependencies": {
      "load": {
        "path": ["__ref", "records_path"],
        "required_keys": ["__ref", "required_keys"],
        "overwrite": ["__ref", "overwrite"],
        "cache_key": ["__ref", "load_cache_key"]
      }
    }
  },
  "Stats": {
    "name": "Stats",
    "clas": "PandasStats",
    "settings": {
      "data_session": {"root": "<<GRAPH_DATA_ROOT>>", "unlocked": true},
      "cache_path": "Stats"
    },
    "dependencies": {
      "describe": {
        "records": ["__ref", "Load", "load", "records"],
        "fields": ["__ref", "stat_fields"],
        "overwrite": ["__ref", "overwrite"],
        "cache_key": ["__ref", "stats_cache_key"]
      }
    }
  },
  "Scatter": {
    "name": "Scatter",
    "clas": "GraphScatterImage",
    "settings": {
      "data_session": {"root": "<<GRAPH_DATA_ROOT>>", "unlocked": true},
      "cache_path": "Scatter"
    },
    "dependencies": {
      "plot": {
        "records": ["__ref", "Load", "load", "records"],
        "stats": ["__ref", "Stats", "describe", "stats"],
        "x_key": ["__ref", "x_key"],
        "y_key": ["__ref", "y_key"],
        "title": ["__ref", "plot_title"],
        "sigma": ["__ref", "sigma"],
        "overwrite": ["__ref", "overwrite"],
        "cache_key": ["__ref", "scatter_cache_key"]
      }
    }
  }
}
```

### Driver App (loads graph, runs it, writes a PNG)

This driver is intentionally self-contained: it defines the node implementations, loads the graph JSON, runs it, and
writes out `scatter.png`.

`run_data_processing_graph.py`:

```py
import io
import json
from pathlib import Path
from typing import Any

from nodejobs.dependencies.CachingComponent import CachingComponent
from processing_graph.BaseProcessor import BaseProcessor


class LoadRecordFromFile(CachingComponent):
    """Load JSON list[dict], validate required keys, cache results."""

    def load(
        self,
        path: str,
        required_keys: list[str],
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for LoadRecordFromFile.load")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("records"), list):
                return cached

        p = Path(path)
        with p.open("r", encoding="utf-8") as f:
            raw = json.load(f)

        if not isinstance(raw, list):
            raise TypeError("expected JSON file containing list[dict]")

        req = list(required_keys or [])
        records: list[dict[str, Any]] = []
        for i, rec in enumerate(raw):
            if not isinstance(rec, dict):
                raise TypeError(f"record[{i}] must be a dict")
            for k in req:
                if k not in rec:
                    raise KeyError(f"record[{i}] missing required key: {k}")
            records.append(rec)

        out = {
            "records": records,
            "count": len(records),
            "required_keys": req,
            "source_path": str(p),
        }
        self.save_cache_path(cache_key, out)
        return out


class PandasStats(CachingComponent):
    """Compute mean/std for selected fields over list[dict], cache results."""

    def describe(
        self,
        records: list[dict],
        fields: list[str],
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for PandasStats.describe")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("stats"), dict):
                return cached

        import pandas as pd

        df = pd.DataFrame(list(records or []))
        stats: dict[str, dict[str, float]] = {}
        for field in list(fields or []):
            if field not in df.columns:
                stats[field] = {"mean": 0.0, "std": 0.0, "count": 0.0}
                continue
            s = pd.to_numeric(df[field], errors="coerce").dropna()
            if len(s) == 0:
                stats[field] = {"mean": 0.0, "std": 0.0, "count": 0.0}
                continue
            stats[field] = {
                "mean": float(s.mean()),
                "std": float(s.std(ddof=0)) if len(s) > 1 else 0.0,
                "count": float(len(s)),
            }

        out = {
            "records": list(records or []),
            "fields": list(fields or []),
            "stats": stats,
        }
        self.save_cache_path(cache_key, out)
        return out


class GraphScatterImage(CachingComponent):
    """Render scatter plot to PNG bytes; cache result (binary-safe)."""

    def plot(
        self,
        records: list[dict],
        stats: dict,
        x_key: str,
        y_key: str,
        title: str | None = None,
        sigma: float = 1.0,
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for GraphScatterImage.plot")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("png_bytes"), (bytes, bytearray)):
                return cached

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Ellipse

        xs: list[float] = []
        ys: list[float] = []
        for i, rec in enumerate(records or []):
            if not isinstance(rec, dict):
                raise TypeError(f"records[{i}] must be a dict")
            xs.append(float(rec[x_key]))
            ys.append(float(rec[y_key]))

        if not isinstance(stats, dict):
            raise TypeError("stats must be a dict")
        mx = float(stats[x_key]["mean"])
        my = float(stats[y_key]["mean"])
        sx = float(stats[x_key]["std"])
        sy = float(stats[y_key]["std"])
        s = float(sigma)
        if s <= 0.0:
            raise ValueError("sigma must be > 0")

        fig, ax = plt.subplots(figsize=(5, 4), dpi=160)
        ax.scatter(xs, ys, alpha=0.85)

        # Mean + std ellipses (axis-aligned).
        # Matplotlib Ellipse width/height are full diameters.
        e1 = Ellipse(
            (mx, my),
            width=2.0 * s * sx,
            height=2.0 * s * sy,
            fill=False,
            linewidth=2.0,
            edgecolor="C1",
        )
        e2 = Ellipse(
            (mx, my),
            width=4.0 * s * sx,
            height=4.0 * s * sy,
            fill=False,
            linewidth=1.5,
            edgecolor="C1",
            alpha=0.6,
        )
        ax.add_patch(e1)
        ax.add_patch(e2)
        ax.scatter([mx], [my], s=30, c="C1")
        ax.set_xlabel(str(x_key))
        ax.set_ylabel(str(y_key))
        if isinstance(title, str) and title:
            ax.set_title(title)
        ax.grid(True, alpha=0.2)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)

        out = {
            "png_bytes": buf.getvalue(),
            "mime": "image/png",
            "x_key": str(x_key),
            "y_key": str(y_key),
            "n": float(len(xs)),
            "sigma": float(s),
        }
        self.save_cache_path(cache_key, out)
        return out


def main() -> None:
    here = Path(__file__).resolve().parent

    graph_path = here / "data_processing_graph.json"
    dataset_path = here / "points_10.json"

    run_root = here / "_data_processing_run"
    run_root.mkdir(parents=True, exist_ok=True)

    graph = json.loads(graph_path.read_text(encoding="utf-8"))
    if not isinstance(graph, dict):
        raise TypeError("graph JSON must be an object")

    # If you are not relying on env mapping for <<GRAPH_DATA_ROOT>>, patch it here.
    for node_def in graph.values():
        if not isinstance(node_def, dict):
            continue
        settings = node_def.get("settings")
        if not isinstance(settings, dict):
            continue
        ds = settings.get("data_session")
        if not isinstance(ds, dict):
            continue
        ds["root"] = str(run_root)

    ctx = {
        "LoadRecordFromFile": LoadRecordFromFile,
        "PandasStats": PandasStats,
        "GraphScatterImage": GraphScatterImage,
    }

    bp = BaseProcessor(graph, context=ctx)
    out = bp.process(
        {
            "records_path": str(dataset_path),
            "required_keys": ["x", "y"],
            "stat_fields": ["x", "y"],
            "x_key": "x",
            "y_key": "y",
            "plot_title": "points_10",
            "sigma": 1.0,
            "overwrite": False,
            "load_cache_key": "state/LoadRecordFromFile.load.points_10.json",
            "stats_cache_key": "state/PandasStats.describe.points_10.json",
            "scatter_cache_key": "state/GraphScatterImage.plot.points_10.png.json"
        }
    )

    png_bytes = out["Scatter"]["plot"]["png_bytes"]
    out_path = run_root / "scatter.png"
    out_path.write_bytes(png_bytes)
    print(f"wrote: {out_path}")


if __name__ == "__main__":
    main()
```

## BaseData And Python Standards

This codebase relies on `nodejobs.dependencies.BaseData.BaseData` as the core typed data container. It is a dict,
but with strict typing, validation, env-mapping, and key-constant conventions.

Source references:

- `/Users/computercomputer/justinops/nodejobs/nodejobs/dependencies/BaseData.py`

### BaseData (What It Is)

`BaseData` is a typed dict with:

1. Annotation-driven required fields.
2. Optional fields expressed as `(type, default)` tuples in annotations.
3. Automatic validation and conversion at construction time (including nested BaseData).
4. Automatic env mapping for `<<ENV_VAR>>` placeholders during initialization.
5. Attribute-style read access for annotated keys (missing keys read as `None`).

### Field Constants vs Instance Values (Magic Field Discovery)

BaseData gives you two related shorthands that work together:

1. `SomeBaseData.key` resolves to the string `"key"` (a developer-time constant).
2. `some_base_data_instance.key` resolves to `some_base_data_instance["key"]` (a runtime value read).

This is why BaseData can behave like a dict while also giving:

1. IDE discoverability (autocomplete on typed fields).
2. Reduced typo risk (you rarely write raw string keys).
3. Earlier and clearer runtime errors (required fields, type mismatches, unknown keys).

Example:

```py
from nodejobs.dependencies.BaseData import BaseData


class ApiConfig(BaseData):
    base_url: str
    timeout_sec: (int, 30)


# Class attribute access: constants for dict construction
assert ApiConfig.base_url == "base_url"

cfg = ApiConfig({
    ApiConfig.base_url: "https://example.internal",
})

# Instance attribute access: reads values (equivalent to dict indexing)
assert cfg.base_url == cfg[ApiConfig.base_url]
assert cfg.timeout_sec == 30
```

### BaseData Rules (Non-Negotiable)

1. No hard-coded string keys in Python.
2. Wrap inputs immediately into the exact BaseData type at every method boundary.
3. Pass BaseData instances between methods; do not rebuild dicts.
4. Do not call `to_safe_dict()` or any “safe dict” conversion helpers. BaseData already is a dict.
5. Do not use `os.environ` / `os.getenv` directly in app code. Use `<<ENV_VAR>>` env mapping.
6. Avoid `from __future__ import annotations` in modules that define BaseData annotations.
   - BaseData asserts annotation types are not strings; future-annotations turns types into strings.

### Structure-By-Example

```py
from nodejobs.dependencies.BaseData import BaseData


class ApiConfig(BaseData):
    base_url: str
    timeout_sec: (int, 30)  # optional with default


class DogCreate(BaseData):
    name: str
    age: (int, None)
    breed: (str, None)


def create_dog(cfg_in, body_in) -> str:
    cfg = ApiConfig(cfg_in)       # wrap immediately
    body = DogCreate(body_in)     # wrap immediately

    # NO string keys:
    # - banned: cfg["base_url"], cfg.get("base_url")
    # - preferred: cfg.base_url
    return f"{cfg.base_url.rstrip('/')}/dogs?name={body.name}"
```

### Typed Lists Must Link Types

If you want recursive validation/construction, list fields must be typed with their BaseData element type:

```py
from nodejobs.dependencies.BaseData import BaseData


class Child(BaseData):
    src: str
    dest: str


class Parent(BaseData):
    overrides: list[Child]  # good
    # overrides: list       # bad (disables useful recursive checks)
```

### Env Mapping Standard (`<<ENV_VAR>>`)

BaseData replaces `<<ENV_VAR>>` placeholders during `BaseData.__init__` before validation and defaults.
If the env var is missing, it raises.

You can pass env placeholders externally at instancing time (not only via `do_pre_process`):

```py
from nodejobs.dependencies.BaseData import BaseData


class OpenAIConfig(BaseData):
    api_key: str
    base_url: (str, "https://api.openai.com/v1")


cfg = OpenAIConfig({
    OpenAIConfig.api_key: "<<OPENAI_API_KEY>>",
})

# The env is resolved during BaseData.__init__ (no direct os.getenv usage at call sites).
# Pass `cfg` everywhere as a BaseData instance; do not serialize/rebuild it.
def make_auth_header(cfg_in) -> dict[str, str]:
    cfg_in = OpenAIConfig(cfg_in)  # wrap immediately at method boundary
    return {"Authorization": f"Bearer {cfg_in.api_key}"}
```

Preferred default-from-env pattern is to inject the placeholder in `do_pre_process`:

```py
from nodejobs.dependencies.BaseData import BaseData


class Query(BaseData):
    openai_key: str

    def do_pre_process(self, in_dict):
        if Query.openai_key not in in_dict or in_dict[Query.openai_key] is None:
            in_dict[Query.openai_key] = "<<OPEN_AI_KEY>>"
        return in_dict
```

## Versioning: Graphs, Runs, And Results

There are three version axes you can use consistently:

1. Graph definition version
   - Store/commit the graph JSON as the versioned source-of-truth.
   - Add an explicit `graph_version` string in the graph root or in a designated node’s settings (project convention).

2. Execution cache key version
   - In this guide’s standard, caching is implemented in each node function using `CachingComponent`.
   - Cache versioning is therefore a node-level concern:
     - use different cache_key paths
     - and/or change node cache behavior explicitly when semantics change

Practical guideline:

- Make the graph JSON the only editable artifact.
- Everything else (runs, caches, logs) is derived.

## Consolidating Results

To consolidate results cleanly:

1. Normalize paths in nodes if needed (BaseProcessor does not rewrite outputs).
2. Store the consolidated result JSON under a run directory keyed by:
   - graph version
   - cache key
   - input parameter hash

If you need flattened outputs, implement a deterministic flattening rule:

- `node_id.method.field` (or `alias.field`) conventions.
- Preserve the full nested result as well.

## Practical Conventions (Recommended)

1. Treat graphs as code.
   - Commit graph JSON exports.
   - Version them.

2. Use context binding for DI.
   - Keep class names stable in graphs.
   - Swap implementations through context.

3. Separate concerns:
   - Graph JSON defines topology.
   - Nodes define behavior.
   - BaseProcessor resolves dependencies and executes node functions.
   - CachingComponent provides cache I/O (and related conventions) used by node implementations.

## Appendix: Architecture

### How Graph Execution Works (BaseProcessor)

Main executor:

- `processing_graph/BaseProcessor.py::BaseProcessor`
- `processing_graph/ProcessingNode.py::ProcessingNode`

#### Lifecycle Overview

1. **Initialization (`BaseProcessor.__init__`)**
   - Iterates over every node in the graph.
   - Forces `node["name"] = node_id`.
   - Calls `createNodeRecursive(...)` to instantiate node runtime wrappers.

2. **Node instancing (`createNodeRecursive`)**
   - Wraps `settings` in `ProcessingNode.NodeSettings`.
   - Builds two maps:
     - `dependency_list`: the node’s `dependencies` dict (method -> kwargs)
     - `upstream_dependency_list`: optional upstream deps
   - Resolves `clas`:
     - If `clas` is a string, it **must** exist in the `context` dict you passed into `BaseProcessor`.
   - Creates a `ProcessingNode` runtime wrapper (or `type` if provided).
   - Scans the node dependency values for `__ref` tuples to pre-register dependency instances (runtime edge wiring).

3. **Execution (`BaseProcessor.process`)**
   - Executes either:
     - all nodes (when no root is specified), or
     - a specific root node (when `rootIn` is set).
   - Uses `process_node(...)` to recursively compute dependencies and then run the node’s functions.

4. **Dependency resolution (`ProcessingNode.get_dependency_value` / `getValueForSetting`)**
   - A value like `["__ref", "OtherNode", "method", "slot"]` is interpreted as “read `feature["OtherNode"]["method"]["slot"]`”.
   - A value like `["__ref", "some_input"]` is interpreted as “read `feature["some_input"]`”.

5. **Function execution (`ProcessingNode.do_process`)**
   - For each function name in the per-node feature map, if `inner_instance` has that method, it is called with kwargs.
   - Return shape per node is:
     - `feature[node_name] = { "<method>": <return_value>, ... }`

#### Output Shape (What You Actually Get Back)

BaseProcessor returns the full `feature` dict after execution. The standard shape is:

```py
{
  "<input_keys>": ...,
  "NodeA": { "method1": {...}, "method2": {...} },
  "NodeB": { "invoke": {...} },
}
```

#### Debugging: Executed Trace

BaseProcessor maintains `self.executed_nodes` and prints a succinct `GraphError:` trace on failure
showing where in the chain execution stopped.

### How Node Classes Resolve And How To Override Them (BaseProcessor)

Graphs refer to node implementations by string name (`node["clas"]`).

Resolution path in `BaseProcessor.createNodeRecursive`:

1. If `node["clas"]` is a class/type, it is used directly.
2. If `node["clas"]` is a string, it must exist in the `context` dict passed to `BaseProcessor(graph, context=CTX)`.
3. If missing, BaseProcessor raises an assertion:
   - `If you are passing string as a class: (<name>), you must have it in the context`

#### Overriding Nodes (Dependency Injection)

There are two practical strategies:

1. Provide a context binding map.
   - Bind `"SomeClassName"` to your custom implementation.
   - The graph JSON stays stable; the implementation swaps.
