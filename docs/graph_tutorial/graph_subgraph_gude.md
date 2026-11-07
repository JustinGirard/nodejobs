# Graph Subgraph Guide (v1)

This is guide #2 in the series. It assumes you already read `graph_processing_guide.md` and the runnable tutorial `guide_tutoial_01_scatter.py`.

Guide #1 showed how to run a graph directly via `BaseProcessor` and how nodes cache their own work via `CachingComponent`.

This guide shows how to take that same scatter pipeline and run it as a subgraph using a single wrapper component: `GraphExecutionNode`.

Core goals:

1. Use one node to encapsulate data roots, runs, and graph-level caching, so "main" becomes very small.
2. Introduce subgraph execution (graph-by-path, published outputs, and wrapper-level input binding).
3. Explain overwrite/cache invalidation at multiple levels and how to wire it cleanly.

Canonical implementation reference:

- `nodejobs/nodejobs/dependencies/graph_executor.py::GraphExecutionNode`

## What Is GraphExecutionNode

`GraphExecutionNode` is a `FieldSet`-based component that wraps `BaseProcessor` execution:

1. Loads a graph from an exported JSON path (or accepts an inline dict).
2. Applies wrapper-provided values into graph dependencies (external refs).
3. Manages a run root ("bundle root") for artifacts.
4. Caches a graph run result under a `cache_key`.
5. Provides a run guard for in-progress execution.
6. Materializes published outputs via `__outputs`.
7. Optionally injects extra published outputs via wrapper `outputs_map`.
8. Normalizes output paths at the boundary.

In short: it is a graph runner packaged as a single node that is itself cacheable and addressable.

## Key Concept: Graph-Level Cache vs Node-Level Cache

There are two caching layers when you use `GraphExecutionNode`:

1. Graph-level result cache (GraphExecutionNode-managed)
2. Node-level caches (your nodes' `CachingComponent` logic)

A common development pattern:

1. Keep node caches stable and only invalidate selectively.
2. Occasionally invalidate the graph-level envelope when the published outputs contract changes.

## Running The Scatter Pipeline As A Subgraph

We reuse the exact logical pipeline from `guide_tutoial_01_scatter.py`:

1. `LoadRecordFromFile.load` reads a `list[dict]` dataset from disk and validates required keys.
2. `PandasStats.describe` computes mean/std and emits `stats`.
3. `GraphScatterImage.plot` draws points + mean/std ellipses and returns `png_bytes`.

The wiring is fan-in:

1. `Load -> Scatter` (records go directly to plot)
2. `Load -> Stats -> Scatter` (stats also go to plot)

### Why Subgraphs Use `__outputs`

In guide #1 we removed `__outputs` because `BaseProcessor` does not use it.

In subgraph execution, `__outputs` is normative and intended:

1. `GraphExecutionNode` reads `__outputs` and injects the resolved values into the returned result dict as top-level keys.
2. Callers do not need to know internal node ids.

## The Exported Subgraph (`scatter_subgraph.json`)

This is an example export graph JSON for the scatter pipeline. It includes `__outputs` so the subgraph publishes a small stable API.

Notes:

1. `data_session` is inline as a dict (BaseData).
2. `<<GRAPH_DATA_ROOT>>` is an env placeholder; you can also patch it at runtime.
3. `cache_path` uses the "organized by root/*_node/DATA" convention.

```json
{
  "__outputs": {
    "final_png_bytes": ["__ref", "Scatter", "plot", "png_bytes"],
    "final_mime": ["__ref", "Scatter", "plot", "mime"]
  },
  "Load": {
    "name": "Load",
    "clas": "LoadRecordFromFile",
    "settings": {
      "data_session": {"root": "<<GRAPH_DATA_ROOT>>", "unlocked": true},
      "cache_path": "Load_node/DATA"
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
      "cache_path": "Stats_node/DATA"
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
      "cache_path": "Scatter_node/DATA"
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

### What `__outputs` Means Here

After running, `GraphExecutionNode` returns a result dict that includes the full nested run outputs, plus:

1. `result["final_png_bytes"]` (bytes)
2. `result["final_mime"]` (`"image/png"`)

Those are the subgraph's public contract.

## Runs vs Exports (Data Management and Versioning)

This section disambiguates two artifacts that are easy to conflate:

1. A graph export (the JSON that defines what to run).
2. A graph run (the on-disk outputs produced when you execute an export).

Reference implementation notes live in:

- `nodejobs/nodejobs/dependencies/graph_executor_tests.py` (comment blocks: "Graph Export vs Graph Run" and root/cache terminology)

### Graph Export (Source Artifact)

A graph export is a JSON file that defines the processing graph to execute:

1. Node ids
2. `clas` bindings (class names)
3. node `settings` (including `data_session` and `cache_path`)
4. `dependencies` wiring, including `__ref` edges
5. `__outputs` (published subgraph API surface)

Key property:

1. An export is "source input". It should be treated like code.
2. GraphExecutionNode reads export JSON; it does not rewrite the export back to disk.

### Graph Run (Derived Artifact)

A graph run is the on-disk outputs produced by executing a graph export.

It typically includes:

1. GraphExecutionNode-managed cached run envelope JSON (graph-level result cache).
2. Run guard / run status records (executor-managed state).
3. Node-produced artifacts (images, videos, derived files).
4. Node-level caches written by individual nodes (via CachingComponent).

Key property:

1. Runs are derived and can be deleted/recomputed.
2. Runs are versioned by cache namespaces and cache keys, not by editing the export.

### Why GraphExecutionNode Sometimes Copies the Export

When you execute with `graph=<path_to_export_json>`, GraphExecutionNode may copy the export into its own cache namespace if the export path is outside the executor cache root.

Why:

1. Prevent run/artifact collisions when multiple executor instances reuse a shared export path.
2. Make each executor's "run bundle" self-contained under its own cache namespace.

Implication:

1. Treat the original export as immutable source.
2. Treat the executor copy as a safety implementation detail for run isolation.

### Roots You Must Keep Straight

There are three different roots involved in typical usage:

1. Outer root (executor DataSession root)
   - The `DataSession.root` used to construct GraphExecutionNode.
   - This is the caller-controlled data root.
2. Executor cache namespace (executor `cache_path`)
   - GraphExecutionNode's own persisted state under `<outer_root>/<cache_path>/...`
   - Includes `executions/runs`, run guards, and FieldSet schema/value caches.
3. Run root (bundle root)
   - The directory used as `settings.data_session.root` for nodes during the subgraph execution.
   - Node artifacts tend to land here.

### Versioning Axes (Practical)

You typically need three version axes:

1. Export version
   - Commit/tag the export JSON (or store it with an explicit version in the filename).
   - Example: `scatter_subgraph.v1.json`, `scatter_subgraph.v2.json`
2. Run cache_key version
   - `cache_key` is the graph-level run identity for GraphExecutionNode.
   - Recommended: include a semantic version suffix and a stable input signature.
   - Example: `scatter.points_10.v1` or `scatter.dataset_<hash>.v3`
3. Node cache_key versioning
   - Each node function uses its own cache_key paths.
   - Changing those keys is the simplest way to invalidate one node's work without invalidating the entire subgraph envelope.

### Collision Avoidance Rules

GraphExecutionNode caches graph-level results under its cache namespace.
Collisions happen if you reuse all of:

1. the same outer root
2. the same executor `cache_path`
3. the same graph-level `cache_key`

Guidance:

1. Make `cache_path` unique per executor instance (namespace by container node id).
2. Always supply an explicit `cache_key` for real usage.
3. Avoid relying on the default cache_key `unversioned_output` except in throwaway experiments.

### Recommended On-Disk Layout

A clean, human-debuggable layout under a single outer root:

1. `INPUT/`
   - input datasets and files (source data)
2. `EXPORTS/`
   - committed/exported graph JSON files (source artifacts)
3. `OUTPUT/`
   - final "public" artifacts you want humans/CI to grab (derived)
4. `<Executor>_node/DATA/`
   - GraphExecutionNode cache namespace (run envelopes, guards, executor schema/value state)
5. `<SubgraphNode>_node/DATA/`
   - subgraph node caches and blobs (internal derived state)

This keeps the mental model clean:

1. Exports are source.
2. Runs are derived.
3. Cache keys and overwrite flags are your invalidation controls.

## The Nice Wrapped Container (Tiny Main)

This wrapper replaces the "manual BaseProcessor wiring" approach.

The wrapper's job is only:

1. Choose an outer root for runs.
2. Instantiate `GraphExecutionNode` under that root and a cache namespace.
3. Call `execute(...)` with graph path, values, cache key, and overwrite flags.
4. Write the published output artifact to a stable output path.

```py
from nodejobs.dependencies.BaseSession import DataSession
from nodejobs.dependencies.graph_executor import GraphExecutionNode


def run_scatter_subgraph(*, outer_root: str, export_graph_path: str) -> str:
    exec_node = GraphExecutionNode({
        "data_session": DataSession({"root": outer_root, "unlocked": True}),
        "cache_path": "GraphExec_node/DATA"
    })

    values = {
        "records_path": f"{outer_root}/INPUT/points_10.json",
        "required_keys": ["x", "y"],
        "stat_fields": ["x", "y"],
        "x_key": "x",
        "y_key": "y",
        "plot_title": "points_10 (subgraph)",
        "sigma": 1.0,

        # Node-level cache invalidation (wired into the subgraph node function args).
        "overwrite": False,

        # Per-function cache keys (node-level caching).
        "load_cache_key": "state/LoadRecordFromFile.load.points_10.json",
        "stats_cache_key": "state/PandasStats.describe.points_10.json",
        "scatter_cache_key": "state/GraphScatterImage.plot.points_10.png.json"
    }

    result = exec_node.execute(
        graph=export_graph_path,
        values=values,

        # Graph-level caching: stable key for the whole run output envelope.
        cache_key="scatter.points_10.v1",

        # Overwrite the graph-level envelope cache.
        overwrite=False,

        # Only affects graphs that use dependencies.<method>.call_params.overwrite.
        overwrite_sub=False
    )

    png = result["final_png_bytes"]
    out_path = f"{outer_root}/OUTPUT/scatter.png"
    with open(out_path, "wb") as f:
        f.write(png)

    return out_path
```

## Published Outputs: `__outputs` vs wrapper `outputs_map`

Normative approach: publish from the subgraph itself via `__outputs`.

Wrapper override: provide `values["outputs_map"]`.

```py
values = {
  # normal inputs...
  "outputs_map": {
    "final_png_bytes": ["__ref", "Scatter", "plot", "png_bytes"]
  }
}
```

Use `outputs_map` when:

1. You cannot edit the exported graph JSON.
2. You want different published outputs per caller without changing the export.

## Overwrite: Practical Taxonomy (and how to wire it)

There are multiple overwrite concepts. It helps to name them.

### 1. GraphExecutionNode `overwrite` (graph-level invalidation)

This controls the executor's cached run envelope.

1. If `overwrite=True`, GraphExecutionNode recomputes and overwrites the cached run result for `cache_key`.
2. This does not automatically force sub-nodes to recompute if their own caches are hit.

### 2. Subgraph-wired `overwrite` (node-level invalidation)

In the scatter export graph, every node function wires:

- `overwrite`: `["__ref", "overwrite"]`

So the wrapper can flip one boolean:

- `values["overwrite"] = True`

And every node bypasses its cache.

This is the most explicit and reliable cache invalidation approach for graphs authored to the guide #1 conventions.

### 3. GraphExecutionNode `overwrite_sub` (call_params convention only)

GraphExecutionNode also has `overwrite_sub=True`.

It walks dependencies and, if a dependency dict contains `call_params`, it sets:

- `call_params["overwrite"] = True`

This matters only for graphs exported by tooling that uses `call_params`.

The scatter graph in this guide does not use `call_params`, so `overwrite_sub` is a compatibility lever, not the recommended primary mechanism.

## Recommended Cache Control Wiring for Development

A pragmatic dev loop:

1. Keep a stable graph-level `cache_key` for the run envelope.
2. Use node-level `values["overwrite"]` to force recompute when changing node code.
3. Use graph-level `overwrite=True` when changing the published `__outputs` contract.

Examples:

1. Re-run everything (node work + envelope)
   - `values.overwrite=True`
   - `execute(overwrite=True)`
2. Re-run nodes but keep envelope stable
   - `values.overwrite=True`
   - `execute(overwrite=False)`
3. Keep nodes stable, just repackage output
   - `values.overwrite=False`
   - `execute(overwrite=True)`

## Subgraph Inside A Parent Graph (Conceptual)

Once you understand "GraphExecutionNode runs a graph export", you can embed it as a node in a larger parent graph.

The parent graph contains a node whose `clas` is `GraphExecutionNode` and whose dependency call is `execute`.
Downstream nodes then reference published outputs from that subgraph run.

```json
{
  "SubScatter": {
    "name": "SubScatter",
    "clas": "GraphExecutionNode",
    "settings": {
      "data_session": {"root": "<<PARENT_ROOT>>", "unlocked": true},
      "cache_path": "SubScatter_node/DATA"
    },
    "dependencies": {
      "execute": {
        "graph": ["__ref", "scatter_export_path"],
        "values": ["__ref", "scatter_values"],
        "cache_key": ["__ref", "scatter_cache_key"],
        "overwrite": ["__ref", "overwrite"],
        "overwrite_sub": ["__ref", "overwrite_sub"]
      }
    }
  },
  "Downstream": {
    "name": "Downstream",
    "clas": "SomeNode",
    "settings": {
      "data_session": {"root": "<<PARENT_ROOT>>", "unlocked": true},
      "cache_path": "Downstream_node/DATA"
    },
    "dependencies": {
      "run": {
        "png_bytes": ["__ref", "SubScatter", "execute", "final_png_bytes"]
      }
    }
  }
}
```

The key outcome: the parent graph declares refs and the executor resolves and wires them.

## Important Operational Notes

### Cache path must be unique per executor instance

GraphExecutionNode stores run envelopes under its own cache namespace (`cache_path`). If multiple executor nodes share:

1. same outer root
2. same `cache_path`
3. same `cache_key`

they collide.

In practice:

1. Make `cache_path` unique per executor instance.
2. Always supply an explicit `cache_key`.

### Export graph path safety copy

If you execute by graph path and it is outside the executor's cache namespace, GraphExecutionNode copies it into its own `executions/` directory before running.
This prevents collisions when multiple runs reuse the same export.

### Binary-safe caching

GraphExecutionNode stores its run envelope as JSON under `executions/runs`. Node outputs that include bytes are safe because `CachingComponent` supports binary references and rehydration.

## Meta-Graph Example: One Generator -> Three Subgraph Executions

This is a perfunctory but realistic example showing why organized subgraphs are useful.

Goal:

1. `PointGenerator` produces 3 datasets (each `list[dict]` with keys `x` and `y`), sized 10.
2. The outer graph fans out into 3 `GraphExecutionNode` executions (same subgraph export, different dataset index).
3. The driver writes three scatter plots to `OUTPUT/`.

Key concept: we pump `records` directly into the subgraph. The subgraph does not load from disk; it accepts a dataset directly as an external ref.

### Subgraph Export Variant: Inline Records (`scatter_subgraph_inline_records.json`)

This subgraph is the same scatter pipeline, but it takes `records` as a direct external input.
There is no `LoadRecordFromFile` node. Both `Stats` and `Scatter` consume the external `records` field.

```json
{
  "__outputs": {
    "final_png_bytes": ["__ref", "Scatter", "plot", "png_bytes"],
    "final_mime": ["__ref", "Scatter", "plot", "mime"]
  },
  "Stats": {
    "name": "Stats",
    "clas": "PandasStats",
    "settings": {
      "data_session": {"root": "<<GRAPH_DATA_ROOT>>", "unlocked": true},
      "cache_path": "Stats_node/DATA"
    },
    "dependencies": {
      "describe": {
        "records": ["__ref", "records"],
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
      "cache_path": "Scatter_node/DATA"
    },
    "dependencies": {
      "plot": {
        "records": ["__ref", "records"],
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

### Node: PointGenerator (3 distributions, n=10)

This generator returns three record lists in one call, plus three pre-built `values_*` dicts suitable for passing directly into `GraphExecutionNode.execute(values=...)`.

Why return `values_*`:

1. It avoids any deep resolution issues (we pass a plain dict into GraphExecutionNode).
2. It keeps the meta-graph simple: Generator -> Executor nodes.

```py
import random
from nodejobs.dependencies.CachingComponent import CachingComponent


class PointGenerator(CachingComponent):
    def generate(self, n: int = 10, overwrite: bool = False, cache_key: str | None = None) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for PointGenerator.generate")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and all(
                isinstance(cached.get(k), dict) for k in ("values_1", "values_2", "values_3")
            ):
                return cached

        rng = random.Random(1337)

        def sample(sx: float, sy: float) -> list[dict]:
            out = []
            for _ in range(int(n)):
                x = rng.gauss(0.0, sx)
                y = rng.gauss(0.0, sy)
                out.append({"x": float(x), "y": float(y)})
            return out

        # Three different distributions.
        records_1 = sample(1.0, 1.0)  # equal variance
        records_2 = sample(3.0, 1.0)  # more x variance
        records_3 = sample(1.0, 3.0)  # more y variance

        def make_values(records: list[dict], label: str, suffix: str) -> dict:
            return {
                "records": records,
                "stat_fields": ["x", "y"],
                "x_key": "x",
                "y_key": "y",
                "plot_title": label,
                "sigma": 1.0,

                # Node-level overwrite inside the subgraph (wired to node functions).
                "overwrite": False,

                # Per-function cache keys inside the subgraph.
                "stats_cache_key": f"state/PandasStats.describe.{suffix}.json",
                "scatter_cache_key": f"state/GraphScatterImage.plot.{suffix}.png.json",
            }

        out = {
            "records_1": records_1,
            "records_2": records_2,
            "records_3": records_3,
            "values_1": make_values(records_1, "dist1: equal variance", "dist1"),
            "values_2": make_values(records_2, "dist2: more x variance", "dist2"),
            "values_3": make_values(records_3, "dist3: more y variance", "dist3"),
            "n": int(n),
        }

        self.save_cache_path(cache_key, out)
        return out
```

### Meta-Graph: BaseProcessor Outer Graph (3 GraphExecutionNodes)

Outer graph shape:

1. `PointGenerator.generate` runs once.
2. `Exec1/Exec2/Exec3` each runs `GraphExecutionNode.execute` against the same exported subgraph, using `values_1/2/3`.

Important organization rule:

1. Each executor gets its own `cache_path` namespace (`Exec1_node/DATA`, etc.) to avoid collisions.
2. Each executor gets its own graph-level `cache_key` (`scatter.dist1.v1`, etc.).

```py
META_GRAPH = {
    "PointGenerator": {
        "name": "PointGenerator",
        "clas": "PointGenerator",
        "settings": {
            "data_session": {"root": "./", "unlocked": True},
            "cache_path": "PointGenerator_node/DATA",
        },
        "dependencies": {
            "generate": {
                "n": ["__ref", "n_points"],
                "overwrite": ["__ref", "overwrite_parent"],
                "cache_key": ["__ref", "gen_cache_key"],
            }
        },
    },
    "Exec1": {
        "name": "Exec1",
        "clas": "GraphExecutionNode",
        "settings": {
            "data_session": {"root": "./", "unlocked": True},
            "cache_path": "Exec1_node/DATA",
        },
        "dependencies": {
            "execute": {
                "graph": ["__ref", "subgraph_export_path"],
                "values": ["__ref", "PointGenerator", "generate", "values_1"],
                "cache_key": "scatter.dist1.v1",
                "overwrite": ["__ref", "overwrite_graph_envelope"],
                "overwrite_sub": ["__ref", "overwrite_sub"],
            }
        },
    },
    "Exec2": {
        "name": "Exec2",
        "clas": "GraphExecutionNode",
        "settings": {
            "data_session": {"root": "./", "unlocked": True},
            "cache_path": "Exec2_node/DATA",
        },
        "dependencies": {
            "execute": {
                "graph": ["__ref", "subgraph_export_path"],
                "values": ["__ref", "PointGenerator", "generate", "values_2"],
                "cache_key": "scatter.dist2.v1",
                "overwrite": ["__ref", "overwrite_graph_envelope"],
                "overwrite_sub": ["__ref", "overwrite_sub"],
            }
        },
    },
    "Exec3": {
        "name": "Exec3",
        "clas": "GraphExecutionNode",
        "settings": {
            "data_session": {"root": "./", "unlocked": True},
            "cache_path": "Exec3_node/DATA",
        },
        "dependencies": {
            "execute": {
                "graph": ["__ref", "subgraph_export_path"],
                "values": ["__ref", "PointGenerator", "generate", "values_3"],
                "cache_key": "scatter.dist3.v1",
                "overwrite": ["__ref", "overwrite_graph_envelope"],
                "overwrite_sub": ["__ref", "overwrite_sub"],
            }
        },
    },
}
```

### Driver: Run Meta-Graph and Write Three PNGs

We intentionally keep file writing out of the graph to avoid introducing a fourth concept node. The graph produces bytes; the driver persists them.

```py
from pathlib import Path

from processing_graph.BaseProcessor import BaseProcessor
from nodejobs.dependencies.graph_executor import GraphExecutionNode


def run_meta_graph(*, root: str, subgraph_export_path: str) -> None:
    root_path = Path(root)
    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "OUTPUT").mkdir(parents=True, exist_ok=True)

    # In normal hosting, GraphExecutionNode has its context set by the host.
    # For a standalone example, GraphExecutionNode must be able to resolve the subgraph node classes.
    # The normative way is to set it explicitly:
    #   GraphExecutionNode.set_context({
    #     "PandasStats": PandasStats,
    #     "GraphScatterImage": GraphScatterImage,
    #   })
    # (Use the same PandasStats and GraphScatterImage implementations as guide_tutoial_01_scatter.py.)
    ctx = {
        "PointGenerator": PointGenerator,
        "GraphExecutionNode": GraphExecutionNode,
    }

    bp = BaseProcessor(META_GRAPH, context=ctx)
    out = bp.process(
        {
            "n_points": 10,
            "overwrite_parent": False,
            "overwrite_graph_envelope": False,
            "overwrite_sub": False,
            "gen_cache_key": "state/PointGenerator.generate.n10.v1.json",
            "subgraph_export_path": str(subgraph_export_path),
        }
    )

    png1 = out["Exec1"]["execute"]["final_png_bytes"]
    png2 = out["Exec2"]["execute"]["final_png_bytes"]
    png3 = out["Exec3"]["execute"]["final_png_bytes"]

    (root_path / "OUTPUT" / "scatter_1.png").write_bytes(png1)
    (root_path / "OUTPUT" / "scatter_2.png").write_bytes(png2)
    (root_path / "OUTPUT" / "scatter_3.png").write_bytes(png3)
```

This is the organizing payoff:

1. One generator node.
2. One reusable subgraph export.
3. Three clean executor namespaces (`Exec1_node/DATA`, `Exec2_node/DATA`, `Exec3_node/DATA`).
4. Three cached graph-level envelopes (`cache_key` per execution).

## Dynamic Graphs: Execution Templates vs Transactional Exports

All examples above use a static graph topology written out ahead of time. For many real workflows, that is not expressive enough.

This section introduces dynamic execution planning in one simple move: make the graph topology a function of one input parameter.

### Terminology

1. Transactional export graph (static)
   - What you commit and version.
   - Treat it as source.
   - Stable node ids, wiring, and published `__outputs`.
2. Execution template (code)
   - A Python function that generates a graph dict from parameters.
   - Lets you expand "N copies of a pattern" without hand-writing nodes.
3. Execution graph (runtime materialization)
   - The actual graph dict produced by the template for a specific parameterization.
   - Optional but recommended: persist it for reproducibility/audits when doing serious work.

### Dynamic Meta-Graph Example: Generate 10 Subgraph Executions in a Loop

We build directly on the prior "inline records" subgraph idea.

1. One `PointGenerator.generate` produces `values_0` through `values_9`.
2. A loop generates `Exec0` through `Exec9`.
3. Each `Exec{i}` runs the same scatter subgraph export using `GraphExecutionNode.execute`.
4. The driver writes 10 images to `OUTPUT/`.

The key point is that the number of executor nodes (and therefore the topology) is a function of `dataset_count`.

#### PointGenerator: produce `values_{i}` for i in [0..dataset_count-1]

The distributions vary by index:

1. `i % 3 == 0`: equal xy variance
2. `i % 3 == 1`: more x variance
3. `i % 3 == 2`: more y variance

```py
import random

from nodejobs.dependencies.CachingComponent import CachingComponent


class PointGenerator(CachingComponent):
    def generate(
        self,
        dataset_count: int = 10,
        n_points: int = 10,
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for PointGenerator.generate")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("values_0"), dict):
                return cached

        rng = random.Random(1337)

        def sample(sx: float, sy: float) -> list[dict]:
            out = []
            for _ in range(int(n_points)):
                out.append(
                    {
                        "x": float(rng.gauss(0.0, sx)),
                        "y": float(rng.gauss(0.0, sy)),
                    }
                )
            return out

        out: dict = {
            "dataset_count": int(dataset_count),
            "n_points": int(n_points),
        }

        for i in range(int(dataset_count)):
            if i % 3 == 0:
                sx, sy, label = 1.0, 1.0, f"idx{i}: equal variance"
            elif i % 3 == 1:
                sx, sy, label = 3.0, 1.0, f"idx{i}: more x variance"
            else:
                sx, sy, label = 1.0, 3.0, f"idx{i}: more y variance"

            records = sample(sx, sy)

            out[f"values_{i}"] = {
                "records": records,
                "stat_fields": ["x", "y"],
                "x_key": "x",
                "y_key": "y",
                "plot_title": label,
                "sigma": 1.0,

                # Node-level overwrite inside the subgraph.
                "overwrite": False,

                # Per-function cache keys inside the subgraph (unique per index).
                "stats_cache_key": f"state/PandasStats.describe.idx{i}.json",
                "scatter_cache_key": f"state/GraphScatterImage.plot.idx{i}.png.json",
            }

        self.save_cache_path(cache_key, out)
        return out
```

#### Execution Template: build a meta-graph whose topology depends on `dataset_count`

This is the dynamic planning step: node registrations are generated in a loop.

```py
def build_meta_graph(*, dataset_count: int) -> dict:
    g: dict = {
        "PointGenerator": {
            "name": "PointGenerator",
            "clas": "PointGenerator",
            "settings": {
                "data_session": {"root": "./", "unlocked": True},
                "cache_path": "PointGenerator_node/DATA",
            },
            "dependencies": {
                "generate": {
                    "dataset_count": ["__ref", "dataset_count"],
                    "n_points": ["__ref", "n_points"],
                    "overwrite": ["__ref", "overwrite_parent"],
                    "cache_key": ["__ref", "gen_cache_key"],
                }
            },
        }
    }

    for i in range(int(dataset_count)):
        exec_id = f"Exec{i}"
        g[exec_id] = {
            "name": exec_id,
            "clas": "GraphExecutionNode",
            "settings": {
                "data_session": {"root": "./", "unlocked": True},
                # Unique namespace per executor instance (prevents collisions).
                "cache_path": f"{exec_id}_node/DATA",
            },
            "dependencies": {
                "execute": {
                    "graph": ["__ref", "subgraph_export_path"],
                    "values": ["__ref", "PointGenerator", "generate", f"values_{i}"],
                    "cache_key": f"scatter.idx{i}.v1",
                    "overwrite": ["__ref", "overwrite_graph_envelope"],
                    "overwrite_sub": ["__ref", "overwrite_sub"],
                }
            },
        }

    return g
```

#### Driver: run the execution graph and write 10 PNGs

We keep artifact writing out of the graph to avoid adding another node type. The graph produces bytes; the driver persists them.

```py
from pathlib import Path

from processing_graph.BaseProcessor import BaseProcessor
from nodejobs.dependencies.graph_executor import GraphExecutionNode


def run_dynamic_meta_graph(*, root: str, subgraph_export_path: str, dataset_count: int = 10) -> None:
    root_path = Path(root)
    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "OUTPUT").mkdir(parents=True, exist_ok=True)

    graph = build_meta_graph(dataset_count=dataset_count)

    # Hosting note:
    # GraphExecutionNode must be able to resolve the classes used by the subgraph export.
    # In real hosting, the host sets this once.
    ctx = {
        "PointGenerator": PointGenerator,
        "GraphExecutionNode": GraphExecutionNode,
    }

    bp = BaseProcessor(graph, context=ctx)
    out = bp.process(
        {
            "dataset_count": int(dataset_count),
            "n_points": 10,
            "overwrite_parent": False,
            "overwrite_graph_envelope": False,
            "overwrite_sub": False,
            "gen_cache_key": f"state/PointGenerator.generate.count{dataset_count}.n10.v1.json",
            "subgraph_export_path": str(subgraph_export_path),
        }
    )

    for i in range(int(dataset_count)):
        png = out[f"Exec{i}"]["execute"]["final_png_bytes"]
        (root_path / "OUTPUT" / f"scatter_{i:02d}.png").write_bytes(png)
```

### Why This Is Dynamic Execution Planning

This is not just "a loop calling execute". The loop generates the graph topology itself.

1. The planner produces a concrete execution graph for a given `dataset_count`.
2. That execution graph is cacheable, inspectable, and can be persisted for reproducibility.
3. Organized subgraphs stay organized at scale because each executor instance gets its own namespace and cache key.

## Discussion: Why Transactional + Dynamic Graphs Matter

Dynamic graphs turn a workflow into an explicit, inspectable execution artifact. Combined with the graph-as-transaction mindset (exports are source, runs are derived), this becomes a flexible, version-control-compatible, and easy-to-debug intelligent actor layer.

### Forensics (Before, During, After)

Graph exports make it possible to pre-compute and reason about workflows:

1. Before execution: inspect topology, dependencies, and published outputs (`__outputs`) without running anything.
2. During execution: observe intermediate node/subgraph outputs, run guards, and partial artifacts.
3. After execution: replay the exact run from the same export plus the same cache keys and inputs, and compare changes across versions.

This is practical for debugging because what ran is a concrete object: the export plus the resolved execution graph plus the run artifacts.

### Decentralization and Orchestration

If nodes are external services (microservices, batch jobs, remote APIs), the same structure can orchestrate long-running workflows:

1. Each node/subgraph acts like a stable contract boundary (inputs, outputs, cache keys).
2. Hierarchical caching enables partial recomputation on failure rather than restarting the entire workflow.
3. Subgraphs allow you to treat a multi-step service pipeline as a single step at the parent level, while still keeping internal detail available for debugging and reuse.

### Complex Caching and Reusable Data Stores

The layered model supports complex caching systems:

1. GraphExecutionNode caches graph-level envelopes keyed by `cache_key`.
2. Nodes cache function-level work keyed by per-function cache keys.
3. Data roots and cache namespaces isolate runs and prevent collisions.
4. Reusable stores emerge naturally when cache keys and root layout are treated as part of the workflow design.

### LLM Collaboration and Flat Files

This style is unusually LLM-friendly:

1. Graphs are plain dict/JSON structures (flat files) with explicit wiring (`__ref`).
2. Runs are structured outputs on disk under predictable namespaces.
3. An assistant can explore, explain, or modify a workflow by editing a small export JSON or template function without reverse engineering hidden control flow.

### Reconfiguration and Node Reuse

Workflows become easy to reconfigure because the program is the graph:

1. Swap wiring by changing refs.
2. Swap behavior by changing `clas` bindings (or context) while keeping the graph stable.
3. Promote frequently used chains into subgraphs with published outputs.

This encourages node reuse and reduces copy-paste pipelines.

### Auditing, Compliance, and Logging

You can make debugging and auditing much more concrete:

1. Per-node and per-subgraph intermediate results can be retained.
2. Run envelopes are persisted and keyed.
3. Logs can be scoped by node cache namespace and executor cache namespace.
4. The export itself provides a reviewable spec of what was executed.

### Experimentation: Swap Types, Test Variants

Data science and iterative engineering benefit directly:

1. Try a new node version by swapping `clas` (or context binding) while keeping inputs/wiring stable.
2. Keep caches isolated by changing cache keys or namespaces.
3. Compare outputs across versions using identical exports and inputs.

### Optimization and Grid Search by Generating Many Graph Topologies

Dynamic execution planning enables systematic search:

1. Generate many execution graphs from a template (parameter sweep, grid search, ablations).
2. Execute each with unique namespaces and cache keys.
3. Inspect resulting datasets and artifacts under structured run roots.

This is the same mechanism as the generate 10 Exec{i} nodes example, generalized to more complex workflows.

### Summary

Transactional exports plus dynamic execution templates give you:

1. A reproducible workflow spec (graph export).
2. A flexible planner layer (template-generated execution graphs).
3. Structured, inspectable run artifacts (forensics and auditing).
4. Hierarchical caching for resilient orchestration and rapid iteration.
5. A flat-file representation that is easy for both engineers and LLMs to work with.
