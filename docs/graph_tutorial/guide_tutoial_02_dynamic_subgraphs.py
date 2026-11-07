"""
guide_tutoial_02_dynamic_subgraphs.py

Tutorial 02: Dynamic meta-graphs + organized subgraph execution.

This tutorial builds on guide_tutoial_01_scatter.py by reusing its node implementations:

1. PandasStats
2. GraphScatterImage

What this tutorial demonstrates:

1. Transactional export (subgraph JSON) vs execution graph (runtime materialization).
2. Pumping datasets directly into a subgraph:
   - The exported subgraph accepts external "records" (list[dict]) via GraphExecutionNode values.
   - No intermediate file writer nodes are needed; the driver persists final artifacts.
3. Dynamic execution planning:
   - A single parameter (dataset_count=10) determines the meta-graph topology.
   - We generate Exec0..Exec9 nodes in a loop.
4. Organized runs:
   - Each executor instance has a unique cache namespace: Exec{i}_node/DATA
   - Each executor run has a unique graph-level cache_key: scatter.idx{i}.v1

Run:
  python3 guide_tutoial_02_dynamic_subgraphs.py

Outputs (under a dedicated root directory):
  ./_graph_tutorial_run_02_dynamic/
    EXPORTS/scatter_subgraph_inline_records.json
    OUTPUT/scatter_00.png .. scatter_09.png
    PointGenerator_node/DATA/...
    Exec0_node/DATA/...
    ...

Dependencies:
  - nodejobs
  - processing_graph
  - pandas
  - matplotlib

Important note about GraphExecutionNode context:

GraphExecutionNode executes an exported graph whose node 'clas' values are strings (e.g. "PandasStats").
That means GraphExecutionNode must be able to resolve these class names.

In normal hosting, the host sets GraphExecutionNode context once.
In this standalone tutorial, we set it explicitly:
  GraphExecutionNode.set_context({"PandasStats": PandasStats, "GraphScatterImage": GraphScatterImage})
"""

import json
import random
import sys
from pathlib import Path
from typing import Any

from nodejobs.dependencies.CachingComponent import CachingComponent
from nodejobs.dependencies.graph_executor import GraphExecutionNode
from processing_graph.BaseProcessor import BaseProcessor


# Ensure sibling imports work even when invoked by absolute path.
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

# Reuse the subgraph node implementations from tutorial 01 (keeps this tutorial focused).
from guide_tutoial_01_scatter import PandasStats, GraphScatterImage  # noqa: E402


ROOT_DIRNAME = "_graph_tutorial_run_02_dynamic"


# --------------------------------------------------------------------------------------
# Transactional export (subgraph JSON)
#
# This is the inline-records variant of the scatter subgraph:
# - external "records" is injected via GraphExecutionNode values
# - no file loader node exists inside the subgraph
#
# The export includes __outputs because GraphExecutionNode materializes them.
# --------------------------------------------------------------------------------------
def scatter_subgraph_inline_records_export() -> dict:
    return {
        "__outputs": {
            "final_png_bytes": ["__ref", "Scatter", "plot", "png_bytes"],
            "final_mime": ["__ref", "Scatter", "plot", "mime"],
        },
        "Stats": {
            "name": "Stats",
            "clas": "PandasStats",
            "settings": {
                "data_session": {"root": "<<GRAPH_DATA_ROOT>>", "unlocked": True},
                "cache_path": "Stats_node/DATA",
            },
            "dependencies": {
                "describe": {
                    "records": ["__ref", "records"],
                    "fields": ["__ref", "stat_fields"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "stats_cache_key"],
                }
            },
        },
        "Scatter": {
            "name": "Scatter",
            "clas": "GraphScatterImage",
            "settings": {
                "data_session": {"root": "<<GRAPH_DATA_ROOT>>", "unlocked": True},
                "cache_path": "Scatter_node/DATA",
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
                    "cache_key": ["__ref", "scatter_cache_key"],
                }
            },
        },
    }


def write_export_graph(*, root: Path) -> Path:
    exports_dir = root / "EXPORTS"
    exports_dir.mkdir(parents=True, exist_ok=True)

    export_path = exports_dir / "scatter_subgraph_inline_records.json"
    export_path.write_text(
        json.dumps(scatter_subgraph_inline_records_export(), indent=2),
        encoding="utf-8",
    )
    return export_path


# --------------------------------------------------------------------------------------
# Outer generator node: produces datasets and values payloads for subgraph execution.
#
# It emits values_0..values_{dataset_count-1}. Each values_i is passed directly into
# GraphExecutionNode.execute(values=...), which is the core concept: "pump points directly
# into the subgraph".
#
# Distributions:
# - i % 3 == 0: equal variance
# - i % 3 == 1: more x variance
# - i % 3 == 2: more y variance
# --------------------------------------------------------------------------------------
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
                out.append({"x": float(rng.gauss(0.0, sx)), "y": float(rng.gauss(0.0, sy))})
            return out

        out: dict[str, Any] = {"dataset_count": int(dataset_count), "n_points": int(n_points)}

        for i in range(int(dataset_count)):
            if i % 3 == 0:
                sx, sy, label = 1.0, 1.0, f"idx{i}: equal variance"
            elif i % 3 == 1:
                sx, sy, label = 3.0, 1.0, f"idx{i}: more x variance"
            else:
                sx, sy, label = 1.0, 3.0, f"idx{i}: more y variance"

            records = sample(sx, sy)
            out[f"values_{i}"] = {
                # IMPORTANT:
                # GraphExecutionNode supports dynamic field aliases, but aliases collide easily
                # (e.g. multiple "cache_key" args become cache_key + cache_key_2).
                # For programmatic execution, prefer dot-path keys which map deterministically
                # into graph dependencies:
                #   "<NodeId>.<method>.<arg>" -> "<NodeId>.dependencies.<method>.<arg>"

                # Stats inputs
                "Stats.describe.records": records,
                "Stats.describe.fields": ["x", "y"],
                "Stats.describe.overwrite": True,
                "Stats.describe.cache_key": f"state/PandasStats.describe.idx{i}.json",

                # Scatter inputs
                "Scatter.plot.records": records,
                "Scatter.plot.x_key": "x",
                "Scatter.plot.y_key": "y",
                "Scatter.plot.title": label,
                "Scatter.plot.sigma": 1.0,
                "Scatter.plot.overwrite": True,
                "Scatter.plot.cache_key": f"state/GraphScatterImage.plot.idx{i}.png.json",
            }

        self.save_cache_path(cache_key, out)
        return out


# --------------------------------------------------------------------------------------
# Dynamic execution planning: meta-graph topology = f(dataset_count)
#
# We generate Exec0..ExecN-1 nodes in a loop.
# Each Exec{i} is a GraphExecutionNode instance with its own cache namespace.
# --------------------------------------------------------------------------------------
def build_meta_graph(*, root: str, dataset_count: int) -> dict:
    def settings(cache_path: str) -> dict:
        return {"data_session": {"root": root, "unlocked": True}, "cache_path": cache_path}

    g: dict[str, Any] = {
        "PointGenerator": {
            "name": "PointGenerator",
            "clas": "PointGenerator",
            "settings": settings("PointGenerator_node/DATA"),
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
            "settings": settings(f"{exec_id}_node/DATA"),
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


def main() -> None:
    # Dedicated root run directory under the CWD.
    root = Path.cwd() / ROOT_DIRNAME
    root.mkdir(parents=True, exist_ok=True)
    (root / "OUTPUT").mkdir(parents=True, exist_ok=True)

    # 1) Export graph (transactional artifact).
    export_path = write_export_graph(root=root)

    # 2) Bind subgraph node class implementations for GraphExecutionNode.
    GraphExecutionNode.set_context({"PandasStats": PandasStats, "GraphScatterImage": GraphScatterImage})

    # 3) Generate execution graph dynamically from dataset_count.
    dataset_count = 10
    n_points = 10
    exec_graph = build_meta_graph(root=str(root), dataset_count=dataset_count)

    # 4) Run outer graph with BaseProcessor for clarity.
    ctx = {
        "PointGenerator": PointGenerator,
        "GraphExecutionNode": GraphExecutionNode,
    }
    bp = BaseProcessor(exec_graph, context=ctx)
    out = bp.process(
        {
            "dataset_count": dataset_count,
            "n_points": n_points,
            # Development default: force recompute at BOTH levels.
            # - overwrite_parent: regenerate dynamic values payloads (prevents stale schema caches)
            # - overwrite_graph_envelope: rerun each GraphExecutionNode run envelope
            "overwrite_parent": True,
            "overwrite_graph_envelope": True,
            "overwrite_sub": False,
            "gen_cache_key": f"state/PointGenerator.generate.count{dataset_count}.n{n_points}.v1.json",
            "subgraph_export_path": str(export_path),
        }
    )

    # 5) Persist final artifacts (outside the graph).
    out_paths = []
    for i in range(dataset_count):
        png = out[f"Exec{i}"]["execute"]["final_png_bytes"]
        out_path = root / "OUTPUT" / f"scatter_{i:02d}.png"
        out_path.write_bytes(png)
        out_paths.append(out_path)

    print(f"wrote {dataset_count} plots under: {root / 'OUTPUT'}")
    print("image_paths:")
    for p in out_paths:
        print(str(p))


if __name__ == "__main__":
    main()
