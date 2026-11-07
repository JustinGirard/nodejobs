# Graph Optimization 2 Guide (Build-Along)

This guide continues from `graph_optimization_guide.md` (Tutorials 03–05).

Goal:

1. Repeatedly execute a loss subgraph (up to 100 steps)
2. Update model parameters `(m,b)` in a convergence attempt
3. Output two plots:
   - (A) unoptimized residual plot
   - (B) optimized residual plot

This guide is written in build-along style:

1. Task
2. Code
3. Discussion

Prereqs (already built):

- `guide_tutoial_common_gaussian.py`
- `guide_tutoial_05_loss_and_residual_plot.py` (contains `AbsLossNode` + `LossPlotNode`)

---

## Tutorial 06: Optimize m,b with Repeated Subgraph Execution

We will build one new script:

- `guide_tutoial_06_optimize_mb.py`

What it does:

1. Generate one fixed dataset for this run (so before/after is comparable)
2. Define a loss subgraph that publishes objective + Newton-like stats via `__outputs`
3. Run an optimization loop (<= 100 steps) that repeatedly executes the subgraph via `GraphExecutionNode`
4. Render two residual plots using `LossPlotNode`

---

### Task: Create `guide_tutoial_06_optimize_mb.py`

Create a new file: `guide_tutoial_06_optimize_mb.py` in this same directory.

---

### Step 1: Imports

### Code

```py
import random
import time
from pathlib import Path

from nodejobs.dependencies.CachingComponent import CachingComponent
from nodejobs.dependencies.graph_executor import GraphExecutionNode
from processing_graph.BaseProcessor import BaseProcessor

from guide_tutoial_common_gaussian import (
    BasicModel,
    Gaussian2DParams,
    Point,
    sample_gaussian2d_points,
)
from guide_tutoial_05_loss_and_residual_plot import AbsLossNode, LossPlotNode
```

### Discussion

- We will execute an inline subgraph repeatedly using `GraphExecutionNode`.
- We reuse the existing L1 loss and residual plot nodes from Tutorial 05.

---

### Step 2: Implement `MseStatsNode` (Newton-like stats)

We optimize using MSE because it yields a clean analytic gradient + Hessian for the linear model.

### Code

```py
class MseStatsNode(CachingComponent):
    """MSE loss + gradient + Hessian terms for y = m*x + b."""

    def stats(
        self,
        points: list[Point],
        model: BasicModel,
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for MseStatsNode.stats")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("mse"), (int, float)):
                return cached

        points = [Point(p) for p in (points or [])]
        model = BasicModel(model)
        m = float(model.m)
        b = float(model.b)

        xs = []
        ys = []
        for p in points:
            if p.y is None:
                continue
            xs.append(float(p.x))
            ys.append(float(p.y))

        n = len(xs)
        if n == 0:
            out = {
                "mse": 0.0,
                "n": 0,
                "grad_dm": 0.0,
                "grad_db": 0.0,
                "hess_dmm": 0.0,
                "hess_dmb": 0.0,
                "hess_dbb": 0.0,
            }
            self.save_cache_path(cache_key, out)
            return out

        sum_r2 = 0.0
        sum_xr = 0.0
        sum_r = 0.0
        sum_x2 = 0.0
        sum_x = 0.0

        for x, y in zip(xs, ys):
            r = y - (m * x + b)
            sum_r2 += r * r
            sum_xr += x * r
            sum_r += r
            sum_x2 += x * x
            sum_x += x

        mse = sum_r2 / float(n)
        grad_dm = (-2.0 / float(n)) * sum_xr
        grad_db = (-2.0 / float(n)) * sum_r
        hess_dmm = (2.0 / float(n)) * sum_x2
        hess_dmb = (2.0 / float(n)) * sum_x
        hess_dbb = 2.0

        out = {
            "mse": float(mse),
            "n": int(n),
            "grad_dm": float(grad_dm),
            "grad_db": float(grad_db),
            "hess_dmm": float(hess_dmm),
            "hess_dmb": float(hess_dmb),
            "hess_dbb": float(hess_dbb),
        }
        self.save_cache_path(cache_key, out)
        return out
```

### Discussion

- `grad_dm` and `grad_db` are the objective gradient.
- `hess_*` are Hessian terms used for a Newton-like parameter step.

---

### Step 3: Build the Subgraph to Optimize (`__outputs`)

We create a small loss subgraph that publishes both:

- `loss_l1` (human-friendly)
- MSE + gradient + Hessian terms (optimizer-friendly)

### Code

```py
def build_loss_subgraph(*, root: str) -> dict:
    def settings(node_id: str) -> dict:
        return {"data_session": {"root": root, "unlocked": True}, "cache_path": f"{node_id}_node/DATA"}

    return {
        "__outputs": {
            "loss_l1": ["__ref", "LossL1", "loss", "loss_l1"],
            "mse": ["__ref", "LossMse", "stats", "mse"],
            "grad_dm": ["__ref", "LossMse", "stats", "grad_dm"],
            "grad_db": ["__ref", "LossMse", "stats", "grad_db"],
            "hess_dmm": ["__ref", "LossMse", "stats", "hess_dmm"],
            "hess_dmb": ["__ref", "LossMse", "stats", "hess_dmb"],
            "hess_dbb": ["__ref", "LossMse", "stats", "hess_dbb"],
            "n": ["__ref", "LossMse", "stats", "n"],
        },
        "LossL1": {
            "name": "LossL1",
            "clas": "AbsLossNode",
            "settings": settings("LossL1"),
            "dependencies": {"loss": {}},
        },
        "LossMse": {
            "name": "LossMse",
            "clas": "MseStatsNode",
            "settings": settings("LossMse"),
            "dependencies": {"stats": {}},
        },
    }
```

### Discussion

We intentionally do NOT use external refs (`["__ref","points"]`) inside this inline subgraph.

Reason:

- `GraphExecutionNode` only builds “field aliases” for external refs when executing a graph from `execution_graph_path`.
- For inline graphs (Python dicts), passing `values={"points": ...}` triggers the alias system and fails.

Instead, we pass subgraph inputs using dot-path override keys, e.g.:

- `LossL1.loss.points`
- `LossMse.stats.model`
Published outputs are materialized by the wrapper.

---

### Step 4: Build the Outer Graph that Executes the Subgraph

### Code

```py
def build_outer_graph(*, root: str) -> dict:
    return {
        "Eval": {
            "name": "Eval",
            "clas": "GraphExecutionNode",
            "settings": {"data_session": {"root": root, "unlocked": True}, "cache_path": "Eval_node/DATA"},
            "dependencies": {
                "execute": {
                    "graph": ["__ref", "loss_subgraph"],
                    "values": ["__ref", "values"],
                    "overwrite": True,
                    "overwrite_sub": False,
                    "cache_key": ["__ref", "eval_cache_key"],
                }
            },
        }
    }
```

### Discussion

Each optimizer step runs:

- `bp.process({loss_subgraph, values, eval_cache_key})`

which triggers a full subgraph execution and returns `__outputs`.

---

### Step 5: Optimization Loop and Before/After Plots

### Code

Copy the full implementation from `guide_tutoial_06_optimize_mb.py`.

In particular, note the `values = {...}` dict in `eval_model()` uses dot-path keys like:

1. `LossL1.loss.points`
2. `LossL1.loss.model`
3. `LossMse.stats.points`
4. `LossMse.stats.model`

### Discussion

We use a damped Newton-like update (Levenberg style) with a small random jitter:

- damping (`lam`) stabilizes Hessian inversion
- jitter helps avoid edge cases

We plot:

- unoptimized residual plot (start model)
- optimized residual plot (final model)

---

Run:

`python3 guide_tutoial_06_optimize_mb.py`

Outputs:

- `./_graph_tutorial_run_06_optimize/OUTPUT/unoptimized.png`
- `./_graph_tutorial_run_06_optimize/OUTPUT/optimized.png`

---

## Tutorial 07: Group Optimization (3 datasets, 1 shared model)

Goal:

- Evaluate one shared model `(m,b)` across 3 different datasets.
- Optimize the shared model to minimize the group loss (mean MSE across datasets).
- Visualize the practical tradeoff:
  - the optimized model improves group loss
  - it generally will not be best for any particular dataset
- Output 6 plots:
  - `ds0_before.png`, `ds1_before.png`, `ds2_before.png`
  - `ds0_after.png`, `ds1_after.png`, `ds2_after.png`

This tutorial reuses `LossPlotNode` (Tutorial 05) to show residuals per dataset.

### Task: Create `guide_tutoial_07_optimize_group_mb.py`

Create a new file: `guide_tutoial_07_optimize_group_mb.py` in this same directory.

### Discussion

The key lesson is model selection difficulty:

- The datasets are similar but not identical.
- We constrain ourselves to one `(m,b)`.
- Optimization is forced to compromise.
