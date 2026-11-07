"""
guide_tutoial_05_loss_and_residual_plot.py

Graph Optimization Tutorial 05: Loss measurement + residual visualization.

Goal:
- Generate points from a 2D Gaussian.
- Compute L1 loss against y = m*x + b.
- Render a plot that shows:
  - scatter of points
  - the model line y = m*x + b
  - vertical gray residual lines from each dot to the line

Run:
  python3 guide_tutoial_05_loss_and_residual_plot.py

Outputs:
- Writes: ./_graph_tutorial_run_05_loss/OUTPUT/loss_plot.png
- Prints loss value.
- Prints image_paths for easy preview.
"""

import io
import time
from pathlib import Path

from nodejobs.dependencies.CachingComponent import CachingComponent
from processing_graph.BaseProcessor import BaseProcessor

from guide_tutoial_common_gaussian import BasicModel, Gaussian2DParams, Point
from guide_tutoial_04_scatter_line_plot import RandomPointsNode


class AbsLossNode(CachingComponent):
    """Mean absolute error against y = m*x + b."""

    def loss(
        self,
        points: list[Point],
        model: BasicModel,
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for AbsLossNode.loss")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("loss_l1"), (int, float)):
                return cached

        points = [Point(p) for p in (points or [])]
        model = BasicModel(model)
        m = float(model.m)
        b = float(model.b)

        abs_err_sum = 0.0
        n = 0
        for p in points:
            if p.y is None:
                continue
            y = float(p.y)
            y_pred = (m * float(p.x)) + b
            abs_err_sum += abs(y - y_pred)
            n += 1

        loss_l1 = (abs_err_sum / float(n)) if n > 0 else 0.0
        out = {"loss_l1": float(loss_l1), "n": int(n), "computed_at": time.time()}
        self.save_cache_path(cache_key, out)
        return out


class LossPlotNode(CachingComponent):
    """Scatter + model line + residual (vertical) lines."""

    def plot(
        self,
        points: list[Point],
        model: BasicModel,
        loss_l1: float | None = None,
        title: str | None = None,
        n_line: int = 200,
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for LossPlotNode.plot")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("png_bytes"), (bytes, bytearray)):
                return cached

        points = [Point(p) for p in (points or [])]
        model = BasicModel(model)
        m = float(model.m)
        b = float(model.b)

        xs = [float(p.x) for p in points]
        ys = [float(p.y) for p in points if p.y is not None]
        if not xs:
            raise ValueError("points must be non-empty")

        x_min = min(xs)
        x_max = max(xs)
        if x_max == x_min:
            x_min -= 1.0
            x_max += 1.0

        n_line_i = int(n_line)
        if n_line_i < 2:
            raise ValueError("n_line must be >= 2")

        line_xs = [x_min + (x_max - x_min) * (i / (n_line_i - 1)) for i in range(n_line_i)]
        line_ys = [(m * x) + b for x in line_xs]

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(6, 5), dpi=160)

        # Scatter
        ax.scatter(xs, ys, alpha=0.85, label="points")

        # Model line
        ax.plot(line_xs, line_ys, linewidth=2.0, label=f"y = {m:.3f}x + {b:.3f}")

        # Residuals: vertical lines from (x,y) to (x,y_pred)
        for p in points:
            if p.y is None:
                continue
            x = float(p.x)
            y = float(p.y)
            y_pred = (m * x) + b
            ax.plot([x, x], [y, y_pred], color="gray", alpha=0.35, linewidth=1.0)

        # Preserve geometry so the Gaussian reads as an ellipse.
        ax.set_aspect("equal", adjustable="box")

        t = str(title) if isinstance(title, str) else ""
        if loss_l1 is not None:
            try:
                t = (t + " | " if t else "") + f"L1={float(loss_l1):.4f}"
            except Exception:
                pass
        if t:
            ax.set_title(t)

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
            "computed_at": time.time(),
        }
        self.save_cache_path(cache_key, out)
        return out


def build_graph(*, root: str) -> dict:
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
        "Loss": {
            "name": "Loss",
            "clas": "AbsLossNode",
            "settings": settings("Loss"),
            "dependencies": {
                "loss": {
                    "points": ["__ref", "Gen", "generate", "points"],
                    "model": ["__ref", "model"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "loss_cache_key"],
                }
            },
        },
        "Plot": {
            "name": "Plot",
            "clas": "LossPlotNode",
            "settings": settings("Plot"),
            "dependencies": {
                "plot": {
                    "points": ["__ref", "Gen", "generate", "points"],
                    "model": ["__ref", "model"],
                    "loss_l1": ["__ref", "Loss", "loss", "loss_l1"],
                    "title": ["__ref", "title"],
                    "n_line": ["__ref", "n_line"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "plot_cache_key"],
                }
            },
        },
    }


def main() -> None:
    root_path = Path.cwd() / "_graph_tutorial_run_05_loss"
    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "OUTPUT").mkdir(parents=True, exist_ok=True)

    graph = build_graph(root=str(root_path))
    ctx = {
        "RandomPointsNode": RandomPointsNode,
        "AbsLossNode": AbsLossNode,
        "LossPlotNode": LossPlotNode,
    }

    bp = BaseProcessor(graph, context=ctx)

    out = bp.process(
        {
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
            "seed": None,
            "model": BasicModel({BasicModel.m: 0.8, BasicModel.b: -0.2}),
            "title": "loss residuals",
            "n_line": 200,
            "overwrite": True,
            "gen_cache_key": "state/RandomPointsNode.generate.demo.json",
            "loss_cache_key": "state/AbsLossNode.loss.demo.json",
            "plot_cache_key": "state/LossPlotNode.plot.demo.png.json",
        }
    )

    loss_l1 = out["Loss"]["loss"]["loss_l1"]
    png = out["Plot"]["plot"]["png_bytes"]

    out_path = root_path / "OUTPUT" / "loss_plot.png"
    out_path.write_bytes(png)

    print("loss_l1:", loss_l1)
    print(f"wrote: {out_path}")
    print(f"node caches rooted at: {root_path}")
    print("image_paths:")
    print(str(out_path))


if __name__ == "__main__":
    main()
