"""
guide_tutoial_01_scatter.py

A single-file, self-contained tutorial that mirrors the "Dataset Processing Graph (Load -> Stats -> Scatter)"
example from graph_processing_guide.md.

What this script demonstrates (end-to-end):
1. How to define node classes as CachingComponent nodes with the repo's standard function signature:
   - overwrite: bool = False
   - cache_key: str | None = None
2. How to assemble a small graph (as a Python dict) that uses "__ref" dependencies to auto-resolve values.
3. How to run the graph using BaseProcessor (processing_graph).
4. How binary-safe caching works when the node returns bytes (PNG image output).
5. How fan-in works:
   - Load.load.records -> Scatter.plot.records
   - Load.load.records -> Stats.describe.records -> Stats.describe.stats -> Scatter.plot.stats

Run:
  python3 guide_tutoial_01_scatter.py

Outputs:
- A PNG file is written to: ./OUTPUT/scatter.png
- Node caches are written under: ./*_node/DATA/...

Directory layout conventions used by this tutorial:
- ROOT is a dedicated run directory under the CWD (so runs do not pollute your working folder).
- Each node writes under:
    ROOT/<node_id>_node/DATA/<cache files and blobs>
  Where "<node_id>_node" is just an organizational convention for humans and docs.
  It is NOT a magic injection mechanism; it's simply how we choose cache_path strings.

Dependencies:
- nodejobs (for CachingComponent)
- processing_graph (for BaseProcessor)
- pandas (for dataset stats)
- matplotlib (for scatter rendering)

Notes:
- This is a dataset processor: records are handled as a list-of-dicts dataset.
- You will see loops inside nodes only because we must traverse the dataset to validate, compute stats, and render.
  The graph itself is not "per-point processing"; it is node-level dataset processing.
"""

import io
import json
from pathlib import Path
from typing import Any

from nodejobs.dependencies.CachingComponent import CachingComponent
from processing_graph.BaseProcessor import BaseProcessor


# --------------------------------------------------------------------------------------
# (1) Node: LoadRecordFromFile
#
# Responsibility:
# - Read a JSON file that contains a list of records (list[dict]).
# - Validate that every record is a dict and contains required keys.
# - Cache the parsed/validated output.
#
# Why this exists:
# - It makes validation a first-class, cached step.
# - Downstream nodes can trust their inputs are "shaped correctly".
# --------------------------------------------------------------------------------------
class LoadRecordFromFile(CachingComponent):
    """Load a dataset from JSON on disk."""

    def load(
        self,
        path: str,
        required_keys: list[str],
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        """Return a validated dataset envelope.

        Output shape (dict):
          {
            "records": list[dict],
            "count": int,
            "required_keys": list[str],
            "source_path": str
          }
        """

        # ---- Standard cache gate --------------------------------------------------
        if cache_key is None:
            raise Exception("cache_key is required for LoadRecordFromFile.load")

        # NOTE: load_cache_path() in default mode is binary-safe.
        # raw_mode=True bypasses the object hooks; avoid it unless you explicitly want raw JSON.
        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("records"), list):
                return cached

        # ---- Actual work: load + validate ----------------------------------------
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


# --------------------------------------------------------------------------------------
# (2) Node: PandasStats
#
# Responsibility:
# - Take a dataset (list[dict]) from an ascendant node.
# - Compute mean and standard deviation for selected fields.
# - Cache the derived result.
# --------------------------------------------------------------------------------------
class PandasStats(CachingComponent):
    """Compute mean/std over a dataset."""

    def describe(
        self,
        records: list[dict],
        fields: list[str],
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        """Return statistics for selected fields.

        Output shape (dict):
          {
            "records": list[dict],
            "fields": list[str],
            "stats": {
              "<field>": {"mean": float, "std": float, "count": float},
              ...
            }
          }
        """

        # ---- Standard cache gate --------------------------------------------------
        if cache_key is None:
            raise Exception("cache_key is required for PandasStats.describe")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("stats"), dict):
                return cached

        # ---- Actual work: compute mean/std ---------------------------------------
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

            # Population std (ddof=0) gives stable behavior on small datasets.
            stats[field] = {
                "mean": float(s.mean()),
                "std": float(s.std(ddof=0)) if len(s) > 1 else 0.0,
                "count": float(len(s)),
            }

        out = {
            # We keep records as pass-through to match the guide example output.
            "records": list(records or []),
            "fields": list(fields or []),
            "stats": stats,
        }

        self.save_cache_path(cache_key, out)
        return out


# --------------------------------------------------------------------------------------
# (3) Node: GraphScatterImage
#
# Responsibility:
# - Take dataset records + derived stats.
# - Render a scatter plot to PNG bytes.
# - Draw mean point + mean/std ellipses (ovals) using stats for the selected keys.
# - Cache the PNG bytes (binary-safe).
#
# Binary-safe caching note:
# - save_cache_path() writes JSON, but supports bytes by writing a blob and inserting a "$binary_ref".
# - load_cache_path() rehydrates that "$binary_ref" back into bytes automatically.
# --------------------------------------------------------------------------------------
class GraphScatterImage(CachingComponent):
    """Render scatter + mean/std ellipses to PNG bytes."""

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
        """Return a PNG artifact envelope.

        Output shape (dict):
          {
            "png_bytes": bytes,
            "mime": "image/png",
            "x_key": str,
            "y_key": str,
            "n": float,
            "sigma": float,
            "mean": {"x": float, "y": float},
            "std": {"x": float, "y": float}
          }
        """

        # ---- Standard cache gate --------------------------------------------------
        if cache_key is None:
            raise Exception("cache_key is required for GraphScatterImage.plot")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("png_bytes"), (bytes, bytearray)):
                return cached

        # ---- Validate and unpack stats --------------------------------------------
        if not isinstance(stats, dict):
            raise TypeError("stats must be a dict")

        mx = float(stats[x_key]["mean"])
        my = float(stats[y_key]["mean"])
        sx = float(stats[x_key]["std"])
        sy = float(stats[y_key]["std"])

        s = float(sigma)
        if s <= 0.0:
            raise ValueError("sigma must be > 0")

        # ---- Gather dataset points ------------------------------------------------
        xs: list[float] = []
        ys: list[float] = []
        for i, rec in enumerate(records or []):
            if not isinstance(rec, dict):
                raise TypeError(f"records[{i}] must be a dict")
            xs.append(float(rec[x_key]))
            ys.append(float(rec[y_key]))

        # ---- Render plot -----------------------------------------------------------
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Ellipse

        fig, ax = plt.subplots(figsize=(6, 5), dpi=160)

        # 1) Scatter points
        ax.scatter(xs, ys, alpha=0.85)

        # 2) Mean point
        ax.scatter([mx], [my], s=35, c="C1")

        # 3) Mean + std ellipses (axis-aligned)
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
            "mean": {"x": mx, "y": my},
            "std": {"x": sx, "y": sy},
        }

        self.save_cache_path(cache_key, out)
        return out


# --------------------------------------------------------------------------------------
# Graph assembly
#
# We build the graph dict inline to keep this tutorial self-contained.
#
# Node settings:
# - data_session is the common root chosen by the user.
# - cache_path is unique per node and encodes the requested organization rule:
#     ROOT/<node_id>_node/DATA/...
# --------------------------------------------------------------------------------------
def build_graph(*, root: str) -> dict:
    root = str(root)
    if not root:
        raise ValueError("root must be a non-empty path")

    def node_settings(node_id: str) -> dict:
        return {
            "data_session": {"root": root, "unlocked": True},
            "cache_path": f"{node_id}_node/DATA",
        }

    return {
        "Load": {
            "name": "Load",
            "clas": "LoadRecordFromFile",
            "settings": node_settings("Load"),
            "dependencies": {
                "load": {
                    "path": ["__ref", "records_path"],
                    "required_keys": ["__ref", "required_keys"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "load_cache_key"],
                }
            },
        },
        "Stats": {
            "name": "Stats",
            "clas": "PandasStats",
            "settings": node_settings("Stats"),
            "dependencies": {
                "describe": {
                    "records": ["__ref", "Load", "load", "records"],
                    "fields": ["__ref", "stat_fields"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "stats_cache_key"],
                }
            },
        },
        "Scatter": {
            "name": "Scatter",
            "clas": "GraphScatterImage",
            "settings": node_settings("Scatter"),
            "dependencies": {
                "plot": {
                    "records": ["__ref", "Load", "load", "records"],
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


# --------------------------------------------------------------------------------------
# Tutorial dataset provisioning
#
# To make this file runnable without any other assets, we write a small dataset file under:
#   ROOT/INPUT/points_10.json
# --------------------------------------------------------------------------------------
POINTS_10 = [
    {"x": 0.0, "y": 0.0},
    {"x": 1.0, "y": 1.2},
    {"x": 2.0, "y": 1.9},
    {"x": 3.0, "y": 3.1},
    {"x": 4.0, "y": 3.9},
    {"x": 5.0, "y": 5.2},
    {"x": 6.0, "y": 6.1},
    {"x": 7.0, "y": 6.8},
    {"x": 8.0, "y": 8.2},
    {"x": 9.0, "y": 9.1},
]


def ensure_dataset_file(*, root: str) -> Path:
    root_path = Path(root)
    # Input data lives under ROOT/INPUT (kept separate from caches and output artifacts).
    input_dir = root_path / "INPUT"
    input_dir.mkdir(parents=True, exist_ok=True)

    dataset_path = input_dir / "points_10.json"
    if dataset_path.exists():
        return dataset_path

    dataset_path.write_text(json.dumps(POINTS_10, indent=2), encoding="utf-8")
    return dataset_path


def main() -> None:
    # This is a tutorial: it should "just work" with no args, but must also stay organized.
    # We therefore use a dedicated root run directory under the CWD.
    #
    # All generated artifacts (INPUT, OUTPUT, per-node caches) live under this root.
    root_path = Path.cwd() / "_graph_tutorial_run"
    root = str(root_path)
    root_path.mkdir(parents=True, exist_ok=True)

    dataset_path = ensure_dataset_file(root=root)

    graph = build_graph(root=root)
    ctx = {
        "LoadRecordFromFile": LoadRecordFromFile,
        "PandasStats": PandasStats,
        "GraphScatterImage": GraphScatterImage,
    }

    # feature keys are "graph inputs" that __ref can point to.
    # cache_key values are relative paths used directly by CachingComponent.load/save_cache_path.
    bp = BaseProcessor(graph, context=ctx)
    out = bp.process(
        {
            "records_path": str(dataset_path),
            "required_keys": ["x", "y"],
            "stat_fields": ["x", "y"],
            "x_key": "x",
            "y_key": "y",
             "plot_title": "points_10 (mean/std ellipses)",
             # Keep the tutorial deterministic and simple.
             "sigma": 1.0,
            "overwrite": True,
             "load_cache_key": "state/LoadRecordFromFile.load.points_10.json",
             "stats_cache_key": "state/PandasStats.describe.points_10.json",
             "scatter_cache_key": "state/GraphScatterImage.plot.points_10.png.json",
         }
     )

    # Write a stable "final artifact" under ROOT/OUTPUT.
    output_dir = root_path / "OUTPUT"
    output_dir.mkdir(parents=True, exist_ok=True)

    png_bytes = out["Scatter"]["plot"]["png_bytes"]
    out_path = output_dir / "scatter.png"
    out_path.write_bytes(png_bytes)

    print(f"wrote: {out_path}")
    print(f"node caches rooted at: {root_path}")
    print("image_paths:")
    print(str(out_path))


if __name__ == "__main__":
    main()
