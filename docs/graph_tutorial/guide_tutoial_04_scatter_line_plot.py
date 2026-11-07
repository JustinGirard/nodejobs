"""
guide_tutoial_04_scatter_line_plot.py

Graph Optimization Tutorial 04: Visualization graph (scatter + y=mx+b line).

Goal:
- Generate a random splatter of Point(x,y) using a 2D Gaussian:
  - translation (mu_x, mu_y)
  - ellipse spread (std_major, std_minor)
  - rotation (theta_deg)
- Plot scatter points plus the model line y = m*x + b to PNG bytes.

What this script demonstrates:
1. Two-node graph: Gen.generate -> Plot.plot.
2. __ref wiring across nodes.
3. Standard node cache gating:
   - overwrite: bool
   - cache_key: str
4. Binary-safe caching of PNG bytes using CachingComponent.

Run:
  python3 guide_tutoial_04_scatter_line_plot.py

Outputs:
- Writes: ./_graph_tutorial_run_04_scatter_line/OUTPUT/scatter_line.png
- Node caches under:
    ./_graph_tutorial_run_04_scatter_line/Gen_node/DATA/...
    ./_graph_tutorial_run_04_scatter_line/Plot_node/DATA/...
"""

import io
import time
from pathlib import Path

from nodejobs.dependencies.CachingComponent import CachingComponent
from processing_graph.BaseProcessor import BaseProcessor

from guide_tutoial_common_gaussian import (
    BasicModel,
    Gaussian2DParams,
    Point,
    sample_gaussian2d_points,
)

class RandomPointsNode(CachingComponent):
    def generate(
        self,
        n: int,
        gauss: Gaussian2DParams,
        seed: int | None = None,
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for RandomPointsNode.generate")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("points"), list):
                cached = dict(cached)
                cached["points"] = [Point(p) for p in (cached.get("points") or [])]
                meta = cached.get("meta")
                if isinstance(meta, dict) and "gauss" in meta:
                    meta = dict(meta)
                    meta["gauss"] = Gaussian2DParams(meta.get("gauss") or {})
                    cached["meta"] = meta
                return cached

        gauss = Gaussian2DParams(gauss)
        points, seed_used = sample_gaussian2d_points(n=int(n), gauss=gauss, seed=seed)

        out = {
            "points": points,
            "meta": {
                "n": int(n),
                "seed": int(seed) if seed is not None else None,
                "seed_used": int(seed_used),
                "gauss": gauss,
            },
            "computed_at": time.time(),
        }
        self.save_cache_path(cache_key, out)
        return out


class ScatterLinePlotNode(CachingComponent):
    def plot(
        self,
        points: list[Point],
        model: BasicModel,
        title: str | None = None,
        n_line: int = 200,
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for ScatterLinePlotNode.plot")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("png_bytes"), (bytes, bytearray)):
                cached = dict(cached)
                cached["model"] = BasicModel(cached.get("model") or {})
                return cached

        if not isinstance(points, list):
            raise TypeError("points must be a list[Point]")
        points = [Point(p) for p in points]
        model = BasicModel(model)

        xs = [float(p.x) for p in points]
        ys = [float(p.y) for p in points if p.y is not None]
        if not xs:
            raise ValueError("points must be non-empty")

        x_min = min(xs)
        x_max = max(xs)
        if x_max == x_min:
            x_min -= 1.0
            x_max += 1.0

        m = float(model.m)
        b = float(model.b)

        n_line_i = int(n_line)
        if n_line_i < 2:
            raise ValueError("n_line must be >= 2")

        line_xs = [x_min + (x_max - x_min) * (i / (n_line_i - 1)) for i in range(n_line_i)]
        line_ys = [(m * x) + b for x in line_xs]

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(6, 5), dpi=160)
        ax.scatter(xs, ys, alpha=0.85, label="points")
        ax.plot(line_xs, line_ys, linewidth=2.0, label=f"y = {m:.3f}x + {b:.3f}")

        # Preserve geometry so the Gaussian reads as an ellipse.
        ax.set_aspect("equal", adjustable="box")

        if isinstance(title, str) and title:
            ax.set_title(title)
        ax.set_xlabel("x")
        ax.set_ylabel("y")
        ax.grid(True, alpha=0.2)
        ax.legend(loc="best")

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)

        out = {
            "png_bytes": buf.getvalue(),
            "mime": "image/png",
            "model": BasicModel({BasicModel.m: m, BasicModel.b: b}),
            "x_min": float(x_min),
            "x_max": float(x_max),
            "n": int(len(points)),
            "computed_at": time.time(),
        }
        self.save_cache_path(cache_key, out)
        return out


def build_graph(*, root: str) -> dict:
    root = str(root)
    if not root:
        raise ValueError("root must be a non-empty path")

    def settings(node_id: str) -> dict:
        return {"data_session": {"root": root, "unlocked": True}, "cache_path": f"{node_id}_node/DATA"}

    return {
        "Gen": {
            "name": "Gen",
            "clas": "RandomPointsNode",
            "settings": settings("Gen"),
            "dependencies": {
                "generate": {
                    "n": ["__ref", "n"],
                    "gauss": ["__ref", "gauss"],
                    "seed": ["__ref", "seed"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "gen_cache_key"],
                }
            },
        },
        "Plot": {
            "name": "Plot",
            "clas": "ScatterLinePlotNode",
            "settings": settings("Plot"),
            "dependencies": {
                "plot": {
                    "points": ["__ref", "Gen", "generate", "points"],
                    "model": ["__ref", "model"],
                    "title": ["__ref", "title"],
                    "n_line": ["__ref", "n_line"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "plot_cache_key"],
                }
            },
        },
    }


def main() -> None:
    root_path = Path.cwd() / "_graph_tutorial_run_04_scatter_line"
    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "OUTPUT").mkdir(parents=True, exist_ok=True)

    graph = build_graph(root=str(root_path))
    ctx = {
        "RandomPointsNode": RandomPointsNode,
        "ScatterLinePlotNode": ScatterLinePlotNode,
    }

    bp = BaseProcessor(graph, context=ctx)

    feature = {
        "n": 80,
        "gauss": Gaussian2DParams(
            {
                Gaussian2DParams.mu_x: 0.0,
                Gaussian2DParams.mu_y: 0.0,
                Gaussian2DParams.std_major: 2.0,
                Gaussian2DParams.std_minor: 1.0,
                Gaussian2DParams.theta_deg: 35.0,
            }
        ),
        # seed=None => time-based seed_used => new samples each run.
        "seed": None,
        "model": BasicModel({BasicModel.m: 0.8, BasicModel.b: -0.2}),
        "title": "2D Gaussian (rotated) + model line",
        "n_line": 200,
        "overwrite": True,
        "gen_cache_key": "state/RandomPointsNode.generate.demo.json",
        "plot_cache_key": "state/ScatterLinePlotNode.plot.demo.png.json",
    }

    out = bp.process(dict(feature))
    png = out["Plot"]["plot"]["png_bytes"]
    out_path = root_path / "OUTPUT" / "scatter_line.png"
    out_path.write_bytes(png)
    print(f"wrote: {out_path}")
    print(f"node caches rooted at: {root_path}")
    print("image_paths:")
    print(str(out_path))


if __name__ == "__main__":
    main()
