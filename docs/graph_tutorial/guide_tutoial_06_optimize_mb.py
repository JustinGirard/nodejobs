"""
guide_tutoial_06_optimize_mb.py

Graph Optimization Tutorial 06: Optimize (m,b) via repeated subgraph execution.

We optimize a Newton-friendly objective (MSE), and still report/plot L1 for interpretability.

Outputs:
- ./_graph_tutorial_run_06_optimize/OUTPUT/unoptimized.png
- ./_graph_tutorial_run_06_optimize/OUTPUT/optimized.png
"""

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
                "computed_at": time.time(),
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
            "computed_at": time.time(),
        }
        self.save_cache_path(cache_key, out)
        return out


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
            # Filled via GraphExecutionNode dot-path overrides (see eval_model()).
            "dependencies": {"loss": {}},
        },
        "LossMse": {
            "name": "LossMse",
            "clas": "MseStatsNode",
            "settings": settings("LossMse"),
            # Filled via GraphExecutionNode dot-path overrides (see eval_model()).
            "dependencies": {"stats": {}},
        },
    }


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
                    # We always want fresh evaluations during optimization.
                    "overwrite": True,
                    "overwrite_sub": False,
                    # Explicit cache_key so GraphExecutionNode doesn't reuse the default.
                    "cache_key": ["__ref", "eval_cache_key"],
                }
            },
        }
    }


def solve_2x2(a11, a12, a21, a22, b1, b2):
    det = (a11 * a22) - (a12 * a21)
    if abs(det) < 1e-12:
        return None
    x1 = ((b1 * a22) - (a12 * b2)) / det
    x2 = ((a11 * b2) - (b1 * a21)) / det
    return x1, x2


def main() -> None:
    root_path = Path.cwd() / "_graph_tutorial_run_06_optimize"
    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "OUTPUT").mkdir(parents=True, exist_ok=True)

    # 1) Generate one dataset (fixed within this run so before/after is comparable)
    gauss = Gaussian2DParams(
        {
            Gaussian2DParams.mu_x: 0.0,
            Gaussian2DParams.mu_y: 0.0,
            Gaussian2DParams.std_major: 2.0,
            Gaussian2DParams.std_minor: 1.0,
            Gaussian2DParams.theta_deg: 35.0,
        }
    )
    points, seed_used = sample_gaussian2d_points(n=80, gauss=gauss, seed=None)

    # 2) Build graphs
    loss_subgraph = build_loss_subgraph(root=str(root_path))
    outer_graph = build_outer_graph(root=str(root_path))

    # 3) Bind subgraph implementations for GraphExecutionNode
    GraphExecutionNode.set_context({"AbsLossNode": AbsLossNode, "MseStatsNode": MseStatsNode})
    bp = BaseProcessor(outer_graph, context={"GraphExecutionNode": GraphExecutionNode})

    # 4) Evaluation helper (executes the subgraph and returns published outputs)
    eval_counter = 0

    def eval_model(model: BasicModel) -> dict:
        nonlocal eval_counter
        eval_counter += 1
        # IMPORTANT:
        # GraphExecutionNode only builds field aliases from execution_graph_path graphs.
        # For inline graphs, use dot-path override keys (they bypass alias_fields entirely).
        values = {
            "LossL1.loss.points": points,
            "LossL1.loss.model": model,
            "LossL1.loss.overwrite": True,
            "LossL1.loss.cache_key": "state/AbsLossNode.loss.t06.json",
            "LossMse.stats.points": points,
            "LossMse.stats.model": model,
            "LossMse.stats.overwrite": True,
            "LossMse.stats.cache_key": "state/MseStatsNode.stats.t06.json",
        }
        out = bp.process(
            {
                "loss_subgraph": loss_subgraph,
                "eval_cache_key": f"eval.t06.{eval_counter}",
                "values": values,
            }
        )
        return out["Eval"]["execute"]

    # 5) Initial model (intentionally not good)
    model0 = BasicModel({BasicModel.m: -1.0, BasicModel.b: 1.0})
    s0 = eval_model(model0)

    # 6) Optimize with damped Newton + random jitter, accept if improves
    rng = random.Random(int(time.time_ns() % (2**31 - 1)))
    m = float(model0.m)
    b = float(model0.b)
    best_mse = float(s0["mse"])

    lam = 1e-3
    jitter = 0.10
    no_improve = 0

    for _ in range(100):
        cur = eval_model(BasicModel({BasicModel.m: m, BasicModel.b: b}))
        cur_mse = float(cur["mse"])

        g1 = float(cur["grad_dm"])
        g2 = float(cur["grad_db"])
        h11 = float(cur["hess_dmm"]) + lam
        h12 = float(cur["hess_dmb"])
        h21 = float(cur["hess_dmb"])
        h22 = float(cur["hess_dbb"]) + lam

        sol = solve_2x2(h11, h12, h21, h22, g1, g2)
        if sol is None:
            dm = rng.gauss(0.0, jitter)
            db = rng.gauss(0.0, jitter)
        else:
            dm, db = sol
            # Newton-like step: theta <- theta - inv(H)*grad
            dm = -dm + rng.gauss(0.0, jitter)
            db = -db + rng.gauss(0.0, jitter)

        cand_m = m + dm
        cand_b = b + db
        cand = eval_model(BasicModel({BasicModel.m: cand_m, BasicModel.b: cand_b}))
        cand_mse = float(cand["mse"])

        if cand_mse < best_mse:
            m, b = cand_m, cand_b
            best_mse = cand_mse
            lam = max(lam / 2.0, 1e-9)
            jitter = max(jitter * 0.98, 1e-4)
            no_improve = 0
        else:
            lam = min(lam * 2.0, 1e6)
            jitter = max(jitter * 0.99, 1e-4)
            no_improve += 1

        # Simple convergence stop: if no improvements for a while, bail.
        if no_improve >= 20 and cur_mse <= best_mse * 1.000001:
            break

    model_opt = BasicModel({BasicModel.m: m, BasicModel.b: b})
    s_opt = eval_model(model_opt)

    # 7) Plot before/after using LossPlotNode (residual visualization from Tutorial 05)
    plot_graph = {
        "Plot": {
            "name": "Plot",
            "clas": "LossPlotNode",
            "settings": {"data_session": {"root": str(root_path), "unlocked": True}, "cache_path": "Plot_node/DATA"},
            "dependencies": {
                "plot": {
                    "points": ["__ref", "points"],
                    "model": ["__ref", "model"],
                    "loss_l1": ["__ref", "loss_l1"],
                    "title": ["__ref", "title"],
                    "n_line": ["__ref", "n_line"],
                    "overwrite": ["__ref", "overwrite"],
                    "cache_key": ["__ref", "plot_cache_key"],
                }
            },
        }
    }
    plot_bp = BaseProcessor(plot_graph, context={"LossPlotNode": LossPlotNode})

    png0 = plot_bp.process(
        {
            "points": points,
            "model": model0,
            "loss_l1": float(s0["loss_l1"]),
            "title": "unoptimized",
            "n_line": 200,
            "overwrite": True,
            "plot_cache_key": "state/LossPlotNode.plot.unoptimized.t06.png.json",
        }
    )["Plot"]["plot"]["png_bytes"]

    png1 = plot_bp.process(
        {
            "points": points,
            "model": model_opt,
            "loss_l1": float(s_opt["loss_l1"]),
            "title": "optimized",
            "n_line": 200,
            "overwrite": True,
            "plot_cache_key": "state/LossPlotNode.plot.optimized.t06.png.json",
        }
    )["Plot"]["plot"]["png_bytes"]

    out0 = root_path / "OUTPUT" / "unoptimized.png"
    out1 = root_path / "OUTPUT" / "optimized.png"
    out0.write_bytes(png0)
    out1.write_bytes(png1)

    print("seed_used:", seed_used)
    print(
        "start:",
        {"m": float(model0.m), "b": float(model0.b), "mse": float(s0["mse"]), "loss_l1": float(s0["loss_l1"])},
    )
    print(
        "final:",
        {"m": float(model_opt.m), "b": float(model_opt.b), "mse": float(s_opt["mse"]), "loss_l1": float(s_opt["loss_l1"])},
    )
    print("image_paths:")
    print(str(out0))
    print(str(out1))


if __name__ == "__main__":
    main()
