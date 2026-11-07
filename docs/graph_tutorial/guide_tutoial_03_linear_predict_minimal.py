"""
guide_tutoial_03_linear_predict_minimal.py

Graph Optimization Tutorial 03: Minimal linear prediction graph (points + model).

Goal:
- Execute the smallest possible "model graph": one node, one method.
- Predict y from x using y = m*x + b.

Input contract (v1, used by later optimization tutorials):
- points: list[Point]  (Point is BaseData: x required, y optional)
- model: BasicModel    (BaseData: m, b)
- overwrite: bool
- cache_key: str

What this script demonstrates:
1. Standard CachingComponent node signature:
   - overwrite: bool = False
   - cache_key: str | None = None
2. A BaseProcessor graph with one node and __ref dependency wiring.
3. Node-level caching with load_cache_path/save_cache_path.
4. Output shape for dataset-style prediction.

Run:
  python3 guide_tutoial_03_linear_predict_minimal.py

Outputs:
- Prints predicted points and a computed_at timestamp.
- Writes node caches under:
    ./_graph_tutorial_run_03_linear_predict/Predict_node/DATA/...
"""

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

class SimplePoint(BaseData):
    x: float
    y_pred: float
    y: (float, None)

class LinearPredictNode(CachingComponent):
    """Minimal linear model: y = m*x + b (vectorized over points)."""

    def predict(
        self,
        points: list[Point],
        model: BasicModel,
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        # ---- Standard cache gate --------------------------------------------------
        if cache_key is None:
            raise Exception("cache_key is required for LinearPredictNode.predict")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("points_pred"), list):
                # Rehydrate typed outputs so cache hit matches cache miss.
                cached = dict(cached)
                cached["model"] = BasicModel(cached.get("model") or {})
                cached["points_pred"] = [SimplePoint(p) for p in (cached.get("points_pred") or [])]
                return cached

        # ---- Validate + wrap at boundary (BaseData standard) ---------------------
        if not isinstance(points, list):
            raise TypeError("points must be a list[Point]")
        points = [Point(p) for p in points]
        model = BasicModel(model)

        m = float(model.m)
        b = float(model.b)

        points_pred: list[SimplePoint] = []
        for i, p in enumerate(points):
            if not isinstance(p, (dict, BaseData)):
                raise TypeError(f"points[{i}] must be a dict or Point")
            p = Point(p)
            x = float(p.x)
            y_pred = (m * x) + b
            points_pred.append(
                SimplePoint(
                    {
                        SimplePoint.x: x,
                        SimplePoint.y_pred: float(y_pred),
                        # Pass through y when present (used later for loss tutorials).
                        SimplePoint.y: p.y,
                    }
                )
            )

        out = {
            "model": BasicModel({BasicModel.m: m, BasicModel.b: b}),
            "points_pred": points_pred,
            "n": int(len(points_pred)),
            # Timestamp makes cache behavior obvious in a tutorial.
            "computed_at": time.time(),
        }
        self.save_cache_path(cache_key, out)
        return out


def build_graph(*, root: str) -> dict:
    root = str(root)
    if not root:
        raise ValueError("root must be a non-empty path")

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


def main() -> None:
    # Dedicated root run directory under the CWD.
    root_path = Path.cwd() / "_graph_tutorial_run_03_linear_predict"
    root_path.mkdir(parents=True, exist_ok=True)

    graph = build_graph(root=str(root_path))

    # Context binds the string "clas" name to the Python class.
    ctx = {"LinearPredictNode": LinearPredictNode}

    bp = BaseProcessor(graph, context=ctx)

    # Generate input points using the shared mature 2D Gaussian parameterization.
    # This keeps Tutorial 03 focused on the model node, while producing realistic point inputs.
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

    feature = {
        "points": points,
        "model": BasicModel({BasicModel.m: 3.0, BasicModel.b: 1.0}),
        "overwrite": True,
        "pred_cache_key": "state/LinearPredictNode.predict.demo.json",
    }

    out = bp.process(dict(feature))
    r = out["Predict"]["predict"]
    print("seed_used:", seed_used)
    print("predictions:", {"n": r["n"], "points_pred": r["points_pred"], "computed_at": r["computed_at"]})

    print(f"node caches rooted at: {root_path}")
    print("image_paths: (none)")


if __name__ == "__main__":
    main()
