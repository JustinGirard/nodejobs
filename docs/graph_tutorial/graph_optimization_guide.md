# Graph Optimization Guide (Build-Along)

This guide is written in build-along style. It alternates between:

1. Task: what to do next
2. Code: the exact block to add
3. Discussion: what it teaches and why it exists

When you finish the steps, you will have these runnable tutorials:

1. `guide_tutoial_03_linear_predict_minimal.py` (Graph A: one-node model execution)
2. `guide_tutoial_04_scatter_line_plot.py` (Graph B: two-node visualization, generate -> plot)

Each tutorial writes artifacts under a dedicated run directory and prints paths at the end.

## Standard Graph Concepts (Quick)

Graphs are dict/JSON keyed by node id.

Dependencies use `__ref`:

- `['__ref', 'key']` reads from the input feature dict.
- `['__ref', 'NodeId', 'method', 'slot']` reads from an upstream node output.

Cacheable nodes follow the same call signature pattern:

- `overwrite: bool = False`
- `cache_key: str | None = None`

## Shared Building Blocks (Used by Multiple Tutorials)

Tutorial 03 and Tutorial 04 use the same payload types and the same "mature" Gaussian sampling math.
To avoid repeating code, we build a shared module once.

### Task: Create `guide_tutoial_common_gaussian.py`

Create a new file: `guide_tutoial_common_gaussian.py` in this same directory.

### Code

```py
"""
guide_tutoial_common_gaussian.py

Shared, tutorial-grade utilities for the graph optimization series.

This file exists so Tutorial 03 and Tutorial 04 do not need to repeat:

- BaseData payload types (Point, BasicModel, Gaussian2DParams)
- The mature 2D Gaussian (ellipse + rotation) sampling math

All tutorials in this series treat `seed` as optional:

- if seed is None, we derive a time-based seed_used so repeat runs produce new results
- we return/record seed_used so runs are reproducible when needed
"""

import math
import random
import time

from nodejobs.dependencies.BaseData import BaseData


class Point(BaseData):
    x: float
    y: (float, None)


class BasicModel(BaseData):
    m: float
    b: float


class Gaussian2DParams(BaseData):
    mu_x: float
    mu_y: float
    std_major: float
    std_minor: float
    theta_deg: float


def sample_gaussian2d_points(
    *,
    n: int,
    gauss: Gaussian2DParams,
    seed: int | None = None,
) -> tuple[list[Point], int]:
    """Sample a rotated-ellipse 2D Gaussian.

    Parameterization:
      - translation: mu_x, mu_y
      - ellipse spread: std_major, std_minor
      - rotation: theta_deg

    Returns:
      (points, seed_used)
    """
    seed_used = int(seed) if seed is not None else int(time.time_ns() % (2**31 - 1))
    rng = random.Random(seed_used)

    gauss = Gaussian2DParams(gauss)
    mu_x = float(gauss.mu_x)
    mu_y = float(gauss.mu_y)
    std_major = float(gauss.std_major)
    std_minor = float(gauss.std_minor)
    theta = math.radians(float(gauss.theta_deg))

    if std_major < 0.0 or std_minor < 0.0:
        raise ValueError("std_major and std_minor must be >= 0")

    c = math.cos(theta)
    s = math.sin(theta)

    points: list[Point] = []
    for _ in range(int(n)):
        # u,v are independent standard normals
        u = float(rng.gauss(0.0, 1.0))
        v = float(rng.gauss(0.0, 1.0))

        # Scale to ellipse
        a = std_major * u
        b = std_minor * v

        # Rotate + translate
        x = mu_x + (c * a) - (s * b)
        y = mu_y + (s * a) + (c * b)
        points.append(Point({Point.x: float(x), Point.y: float(y)}))

    return points, seed_used
```

### Discussion

This file defines the standard payload types for the optimization series:

- `Point`: the unit record type.
- `BasicModel`: linear model parameters.
- `Gaussian2DParams`: translation + ellipse spread + rotation.

The helper `sample_gaussian2d_points` is the "mature generator" we will use everywhere:

- `mu_x`, `mu_y` moves the cloud (movement).
- `std_major`, `std_minor` controls ellipse size (spread).
- `theta_deg` controls rotation.

We treat `seed` as optional:

- `seed=None` means: use a time-based seed so every run is different.
- we return the computed `seed_used` so you can reproduce a run later.

---

## Tutorial 03 (Graph A): Minimal Linear Model Execution

Goal:

- Build the smallest model graph: one node, one method.
- Input: `points: list[Point]`, `model: BasicModel`.
- Output: `points_pred: list[SimplePoint]`.

### Task: Create `guide_tutoial_03_linear_predict_minimal.py`

### Step 1: Imports and Output Shape

### Code

```py
"""
guide_tutoial_03_linear_predict_minimal.py

Graph Optimization Tutorial 03: Minimal linear prediction graph (points + model).
"""

import time
from pathlib import Path

from nodejobs.dependencies.BaseData import BaseData
from nodejobs.dependencies.CachingComponent import CachingComponent
from processing_graph.BaseProcessor import BaseProcessor

from guide_tutoial_common_gaussian import (
    BasicModel,
    Gaussian2DParams,
    Point,
    sample_gaussian2d_points,
)


class SimplePoint(BaseData):
    x: float
    y_pred: float
    y: (float, None)
```

### Discussion

`SimplePoint` is tutorial-local. It represents the model output per input point.
We import the shared input types (`Point`, `BasicModel`) and generator helper from the common module.

### Step 2: Implement the Model Node

### Code

```py
class LinearPredictNode(CachingComponent):
    """Minimal linear model: y = m*x + b (vectorized over points)."""

    def predict(
        self,
        points: list[Point],
        model: BasicModel,
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for LinearPredictNode.predict")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("points_pred"), list):
                # Rehydrate typed outputs so cache-hit returns the same shapes as cache-miss.
                cached = dict(cached)
                cached["model"] = BasicModel(cached.get("model") or {})
                cached["points_pred"] = [SimplePoint(p) for p in (cached.get("points_pred") or [])]
                return cached

        points = [Point(p) for p in (points or [])]
        model = BasicModel(model)

        m = float(model.m)
        b = float(model.b)

        points_pred: list[SimplePoint] = []
        for p in points:
            x = float(p.x)
            y_pred = (m * x) + b
            points_pred.append(
                SimplePoint(
                    {
                        SimplePoint.x: x,
                        SimplePoint.y_pred: float(y_pred),
                        # Pass-through y when present (used later for loss tutorials).
                        SimplePoint.y: p.y,
                    }
                )
            )

        out = {
            "model": BasicModel({BasicModel.m: m, BasicModel.b: b}),
            "points_pred": points_pred,
            "n": int(len(points_pred)),
            "computed_at": time.time(),
        }
        self.save_cache_path(cache_key, out)
        return out
```

### Discussion

This is the standard cacheable node pattern:

- Require a `cache_key`.
- If `overwrite` is false, allow cache hits.
- Otherwise compute and persist output with `save_cache_path`.

The method operates on datasets (`points -> points_pred`), which aligns with later plotting and loss steps.

### Step 3: Build the One-Node Graph

### Code

```py
def build_graph(*, root: str) -> dict:
    return {
        "Predict": {
            "name": "Predict",
            "clas": "LinearPredictNode",
            "settings": {
                "data_session": {"root": root, "unlocked": True},
                "cache_path": "Predict_node/DATA",
            },
            "dependencies": {
                "predict": {
                    "points": ["__ref", "points"],
                    "model": ["__ref", "model"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "pred_cache_key"],
                }
            },
        }
    }
```

### Discussion

Every arg to `predict` is wired from the input feature dict using `__ref`.
This is the minimal demonstration of graph execution.

### Step 4: Driver (`main`) and Run

### Code

```py
def main() -> None:
    root_path = Path.cwd() / "_graph_tutorial_run_03_linear_predict"
    root_path.mkdir(parents=True, exist_ok=True)

    graph = build_graph(root=str(root_path))
    bp = BaseProcessor(graph, context={"LinearPredictNode": LinearPredictNode})

    gauss = Gaussian2DParams(
        {
            Gaussian2DParams.mu_x: 0.0,
            Gaussian2DParams.mu_y: 0.0,
            Gaussian2DParams.std_major: 2.0,
            Gaussian2DParams.std_minor: 1.0,
            Gaussian2DParams.theta_deg: 20.0,
        }
    )
    points, seed_used = sample_gaussian2d_points(n=10, gauss=gauss, seed=None)

    out = bp.process(
        {
            "points": points,
            "model": BasicModel({BasicModel.m: 3.0, BasicModel.b: 1.0}),
            "overwrite": True,
            "pred_cache_key": "state/LinearPredictNode.predict.demo.json",
        }
    )
    r = out["Predict"]["predict"]
    print("seed_used:", seed_used)
    print("predictions:", {"n": r["n"], "points_pred": r["points_pred"], "computed_at": r["computed_at"]})
    print(f"node caches rooted at: {root_path}")
    print("image_paths: (none)")


if __name__ == "__main__":
    main()
```

### Discussion

Tutorial 03 generates its input dataset in the driver to keep the graph minimal.
Later tutorials move the dataset creation into a generator node.

Run:

`python3 guide_tutoial_03_linear_predict_minimal.py`

---

## Tutorial 04 (Graph B): Visualization (Generate -> Plot)

Goal:

- Node 1 generates points (mature 2D Gaussian).
- Node 2 renders a scatter plot + model line to PNG bytes.
- Driver writes the PNG and prints the path under `image_paths:`.

### Task: Create `guide_tutoial_04_scatter_line_plot.py`

### Step 1: Imports

### Code

```py
"""
guide_tutoial_04_scatter_line_plot.py

Graph Optimization Tutorial 04: Visualization graph (scatter + y=mx+b line).
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
```

### Discussion

Tutorial 04 reuses the same payload types and generator math from the shared module.

### Step 2: Generator Node

### Code

```py
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
```

### Discussion

This node is a dataset producer:

- It emits `points`.
- It records `seed_used` and `gauss` in `meta`.
- With `seed=None` it produces new results every run.

### Step 3: Plot Node

### Code

```py
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

        points = [Point(p) for p in (points or [])]
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
```

### Discussion

This node produces a binary artifact (`png_bytes`). The driver writes it to disk.
The `ax.set_aspect("equal")` call prevents axis scaling from distorting the ellipse.

### Step 4: Build the Two-Node Graph

### Code

```py
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
```

### Discussion

The key tutorial moment is this dependency:

- `Plot.plot.points` pulls from `Gen.generate.points` via `__ref`.

### Step 5: Driver (`main`) and Run

### Code

```py
def main() -> None:
    root_path = Path.cwd() / "_graph_tutorial_run_04_scatter_line"
    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "OUTPUT").mkdir(parents=True, exist_ok=True)

    graph = build_graph(root=str(root_path))
    bp = BaseProcessor(
        graph,
        context={"RandomPointsNode": RandomPointsNode, "ScatterLinePlotNode": ScatterLinePlotNode},
    )

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
            "title": "2D Gaussian (rotated) + model line",
            "n_line": 200,
            "overwrite": True,
            "gen_cache_key": "state/RandomPointsNode.generate.demo.json",
            "plot_cache_key": "state/ScatterLinePlotNode.plot.demo.png.json",
        }
    )

    png = out["Plot"]["plot"]["png_bytes"]
    out_path = root_path / "OUTPUT" / "scatter_line.png"
    out_path.write_bytes(png)
    print(f"wrote: {out_path}")
    print(f"node caches rooted at: {root_path}")
    print("image_paths:")
    print(str(out_path))


if __name__ == "__main__":
    main()
```

Run:

`python3 guide_tutoial_04_scatter_line_plot.py`

---

## Tutorial 05 (C.1): Loss Measurement + Residual Visualization

Goal:

- Compute a numeric loss for a candidate model `y = m*x + b` on a dataset.
- Visualize residuals by drawing a vertical gray line from each dot to the model line at the same `x`.

This tutorial adds two new nodes:

1. `AbsLossNode`: computes mean absolute error (`loss_l1`).
2. `LossPlotNode`: produces a PNG that overlays scatter + line + residuals.

We reuse `RandomPointsNode` from Tutorial 04 to generate points.

### Task: Create `guide_tutoial_05_loss_and_residual_plot.py`

### Step 1: Imports

### Code

```py
import io
import time
from pathlib import Path

from nodejobs.dependencies.CachingComponent import CachingComponent
from processing_graph.BaseProcessor import BaseProcessor

from guide_tutoial_common_gaussian import BasicModel, Gaussian2DParams, Point
from guide_tutoial_04_scatter_line_plot import RandomPointsNode
```

### Discussion

We reuse the typed payloads from the common module, and import `RandomPointsNode` from Tutorial 04 to avoid duplicating generator code.

### Step 2: Implement `AbsLossNode`

### Code

```py
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
        out = {"loss_l1": float(loss_l1), "n": int(n)}
        self.save_cache_path(cache_key, out)
        return out
```

### Discussion

This computes mean absolute error. We keep it minimal and deterministic given the same inputs.

### Step 3: Implement `LossPlotNode` (residual lines)

### Code

```py
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
        x_min = min(xs)
        x_max = max(xs)

        line_xs = [x_min + (x_max - x_min) * (i / (int(n_line) - 1)) for i in range(int(n_line))]
        line_ys = [(m * x) + b for x in line_xs]

        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(6, 5), dpi=160)
        ax.scatter(xs, ys, alpha=0.85, label="points")
        ax.plot(line_xs, line_ys, linewidth=2.0, label=f"y = {m:.3f}x + {b:.3f}")

        # Residuals: vertical gray lines from each dot to the model line.
        for p in points:
            if p.y is None:
                continue
            x = float(p.x)
            y = float(p.y)
            y_pred = (m * x) + b
            ax.plot([x, x], [y, y_pred], color="gray", alpha=0.35, linewidth=1.0)

        ax.set_aspect("equal", adjustable="box")
        ...

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        plt.close(fig)

        out = {"png_bytes": buf.getvalue(), "mime": "image/png"}
        self.save_cache_path(cache_key, out)
        return out
```

### Discussion

The residual lines make “loss” concrete. Each gray segment is the absolute error for that point.

### Step 4: Build the Graph (Gen -> Loss -> Plot)

### Code

```py
def build_graph(*, root: str) -> dict:
    def settings(node_id: str) -> dict:
        return {"data_session": {"root": root, "unlocked": True}, "cache_path": f"{node_id}_node/DATA"}

    return {
        "Gen": {"...": "..."},
        "Loss": {
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
```

### Discussion

This is the first fan-in moment of the optimization series:

- `Plot.plot` consumes points + model + computed loss.

### Step 5: Driver and Run

Run:

`python3 guide_tutoial_05_loss_and_residual_plot.py`
