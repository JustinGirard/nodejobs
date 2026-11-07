"""
guide_tutoial_07_optimize_group_mb.py

Tutorial 07: Group optimization (3 datasets, 1 shared model).

Goal:
- Create 3 datasets with conflicting linear structure (different ellipse rotations).
- Optimize ONE shared (m,b) to minimize group objective = mean MSE across datasets.
- Output 6 residual plots:
  - ds0_before.png, ds1_before.png, ds2_before.png
  - ds0_after.png,  ds1_after.png,  ds2_after.png

Run:
  python3 guide_tutoial_07_optimize_group_mb.py
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
from guide_tutoial_06_optimize_mb import MseStatsNode


class MergeGroupStatsNode(CachingComponent):
    """Merge per-dataset eval outputs into group objective + records.

    Output:
      {
        "records": [ {eval_id, mse, loss_l1, n}, ... ],
        "group": {
          "mse": mean(mse_i),
          "grad_dm": mean(grad_dm_i),
          "grad_db": mean(grad_db_i),
          "hess_dmm": mean(hess_dmm_i),
          "hess_dmb": mean(hess_dmb_i),
          "hess_dbb": mean(hess_dbb_i),
          "n": sum(n_i)
        }
      }
    """

    def merge(
        self,
        evals: list[dict],
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for MergeGroupStatsNode.merge")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("group"), dict):
                return cached

        if not isinstance(evals, list):
            raise TypeError("evals must be a list[dict]")

        records = []
        k = 0
        sum_mse = 0.0
        sum_gm = 0.0
        sum_gb = 0.0
        sum_hmm = 0.0
        sum_hmb = 0.0
        sum_hbb = 0.0
        sum_n = 0

        for i, e in enumerate(evals):
            if not isinstance(e, dict):
                raise TypeError(f"evals[{i}] must be a dict")

            mse = float(e.get("mse"))
            loss_l1 = float(e.get("loss_l1"))
            n = int(e.get("n"))
            records.append(
                {
                    "eval_id": e.get("eval_id"),
                    "mse": mse,
                    "loss_l1": loss_l1,
                    "n": n,
                }
            )

            sum_mse += mse
            sum_gm += float(e.get("grad_dm"))
            sum_gb += float(e.get("grad_db"))
            sum_hmm += float(e.get("hess_dmm"))
            sum_hmb += float(e.get("hess_dmb"))
            sum_hbb += float(e.get("hess_dbb"))
            sum_n += n
            k += 1

        denom = float(k) if k > 0 else 1.0
        out = {
            "records": records,
            "group": {
                "mse": sum_mse / denom,
                "grad_dm": sum_gm / denom,
                "grad_db": sum_gb / denom,
                "hess_dmm": sum_hmm / denom,
                "hess_dmb": sum_hmb / denom,
                "hess_dbb": sum_hbb / denom,
                "n": int(sum_n),
            },
            "computed_at": time.time(),
        }
        self.save_cache_path(cache_key, out)
        return out


class EchoEvalNode(CachingComponent):
    """Publish only eval_id for downstream merge."""

    def echo(self, eval_id: str, overwrite: bool = False, cache_key: str | None = None) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for EchoEvalNode.echo")
        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("eval_id"), str):
                return cached
        out = {"eval_id": str(eval_id), "computed_at": time.time()}
        self.save_cache_path(cache_key, out)
        return out


def write_eval_subgraph(*, export_path: Path, root: str) -> None:
    """Exported so GraphExecutionNode can build field aliases when graph is a path."""
    import json

    def settings(node_id: str) -> dict:
        return {"data_session": {"root": root, "unlocked": True}, "cache_path": f"{node_id}_node/DATA"}

    g = {
        "__outputs": {
            "eval_id": ["__ref", "Meta", "echo", "eval_id"],
            "loss_l1": ["__ref", "LossL1", "loss", "loss_l1"],
            "n": ["__ref", "LossL1", "loss", "n"],
            "mse": ["__ref", "LossMse", "stats", "mse"],
            "grad_dm": ["__ref", "LossMse", "stats", "grad_dm"],
            "grad_db": ["__ref", "LossMse", "stats", "grad_db"],
            "hess_dmm": ["__ref", "LossMse", "stats", "hess_dmm"],
            "hess_dmb": ["__ref", "LossMse", "stats", "hess_dmb"],
            "hess_dbb": ["__ref", "LossMse", "stats", "hess_dbb"],
        },
        "Meta": {
            "name": "Meta",
            "clas": "EchoEvalNode",
            "settings": settings("Meta"),
            "dependencies": {"echo": {}},
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

    export_path.write_text(json.dumps(g, indent=2) + "\n", encoding="utf-8")


def build_group_eval_graph(*, root: str, eval_subgraph_path: str) -> dict:
    def settings(node_id: str) -> dict:
        return {"data_session": {"root": root, "unlocked": True}, "cache_path": f"{node_id}_node/DATA"}

    def eval_node(node_id: str, points_key: str) -> dict:
        return {
            "name": node_id,
            "clas": "GraphExecutionNode",
            "settings": settings(node_id),
            "dependencies": {
                "execute": {
                    "graph": eval_subgraph_path,
                    "values": {
                        "Meta.echo.eval_id": node_id,
                        "Meta.echo.overwrite": ["__ref", "overwrite"],
                        "Meta.echo.cache_key": ["__ref", f"{node_id}_echo_cache_key"],
                        "LossL1.loss.points": ["__ref", points_key],
                        "LossL1.loss.model": ["__ref", "model"],
                        "LossL1.loss.overwrite": ["__ref", "overwrite"],
                        "LossL1.loss.cache_key": ["__ref", f"{node_id}_l1_cache_key"],
                        "LossMse.stats.points": ["__ref", points_key],
                        "LossMse.stats.model": ["__ref", "model"],
                        "LossMse.stats.overwrite": ["__ref", "overwrite"],
                        "LossMse.stats.cache_key": ["__ref", f"{node_id}_mse_cache_key"],
                    },
                    "overwrite": True,
                    "overwrite_sub": False,
                    "cache_key": ["__ref", f"{node_id}_eval_cache_key"],
                }
            },
        }

    return {
        "Eval0": eval_node("Eval0", "points0"),
        "Eval1": eval_node("Eval1", "points1"),
        "Eval2": eval_node("Eval2", "points2"),
        "Merge": {
            "name": "Merge",
            "clas": "MergeGroupStatsNode",
            "settings": settings("Merge"),
            "dependencies": {
                "merge": {
                    "evals": [
                        ["__ref", "Eval0", "execute"],
                        ["__ref", "Eval1", "execute"],
                        ["__ref", "Eval2", "execute"],
                    ],
                    "overwrite": True,
                    "cache_key": ["__ref", "merge_cache_key"],
                }
            },
        },
    }


def solve_2x2(a11, a12, a21, a22, b1, b2):
    det = (a11 * a22) - (a12 * a21)
    if abs(det) < 1e-12:
        return None
    x1 = ((b1 * a22) - (a12 * b2)) / det
    x2 = ((a11 * b2) - (b1 * a21)) / det
    return x1, x2


def render_dataset_plot(*, root_path: Path, points: list[Point], model: BasicModel, loss_l1: float, tag: str) -> Path:
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
    bp = BaseProcessor(plot_graph, context={"LossPlotNode": LossPlotNode})
    png = bp.process(
        {
            "points": points,
            "model": model,
            "loss_l1": float(loss_l1),
            "title": tag,
            "n_line": 200,
            "overwrite": True,
            "plot_cache_key": f"state/LossPlotNode.plot.{tag}.png.json",
        }
    )["Plot"]["plot"]["png_bytes"]

    out_path = root_path / "OUTPUT" / f"{tag}.png"
    out_path.write_bytes(png)
    return out_path


def main() -> None:
    root_path = Path.cwd() / "_graph_tutorial_run_07_group_opt"
    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "OUTPUT").mkdir(parents=True, exist_ok=True)
    (root_path / "EXPORTS").mkdir(parents=True, exist_ok=True)

    # 1) Three datasets that are similar (same general tilt) but not identical.
    # The main difference is their center (mu_y), so a single shared b must compromise.
    base = {
        Gaussian2DParams.mu_x: 0.0,
        Gaussian2DParams.std_major: 2.0,
        Gaussian2DParams.std_minor: 0.8,
    }

    ga0 = Gaussian2DParams({**base, Gaussian2DParams.mu_y: -0.6, Gaussian2DParams.theta_deg: 22.0})
    ga1 = Gaussian2DParams({**base, Gaussian2DParams.mu_y: 0.0, Gaussian2DParams.theta_deg: 28.0})
    ga2 = Gaussian2DParams({**base, Gaussian2DParams.mu_y: 0.6, Gaussian2DParams.theta_deg: 25.0})

    # Fixed seeds so this tutorial is repeatable.
    points0, seed0 = sample_gaussian2d_points(n=80, gauss=ga0, seed=1001)
    points1, seed1 = sample_gaussian2d_points(n=80, gauss=ga1, seed=1002)
    points2, seed2 = sample_gaussian2d_points(n=80, gauss=ga2, seed=1003)

    # 2) Export eval subgraph
    export_path = root_path / "EXPORTS" / "group_eval_subgraph.json"
    write_eval_subgraph(export_path=export_path, root=str(root_path))

    # 3) Context for GraphExecutionNode
    GraphExecutionNode.set_context(
        {
            "EchoEvalNode": EchoEvalNode,
            "AbsLossNode": AbsLossNode,
            "MseStatsNode": MseStatsNode,
        }
    )

    group_graph = build_group_eval_graph(root=str(root_path), eval_subgraph_path=str(export_path))
    bp = BaseProcessor(
        group_graph,
        context={
            "GraphExecutionNode": GraphExecutionNode,
            "MergeGroupStatsNode": MergeGroupStatsNode,
        },
    )

    def eval_group(model: BasicModel, step_key: str) -> dict:
        out = bp.process(
            {
                "points0": points0,
                "points1": points1,
                "points2": points2,
                "model": model,
                "overwrite": True,
                "Eval0_eval_cache_key": f"t07/{step_key}/Eval0",
                "Eval1_eval_cache_key": f"t07/{step_key}/Eval1",
                "Eval2_eval_cache_key": f"t07/{step_key}/Eval2",
                "Eval0_echo_cache_key": f"state/EchoEvalNode.echo.Eval0.{step_key}.json",
                "Eval1_echo_cache_key": f"state/EchoEvalNode.echo.Eval1.{step_key}.json",
                "Eval2_echo_cache_key": f"state/EchoEvalNode.echo.Eval2.{step_key}.json",
                "Eval0_l1_cache_key": f"state/AbsLossNode.loss.Eval0.{step_key}.json",
                "Eval1_l1_cache_key": f"state/AbsLossNode.loss.Eval1.{step_key}.json",
                "Eval2_l1_cache_key": f"state/AbsLossNode.loss.Eval2.{step_key}.json",
                "Eval0_mse_cache_key": f"state/MseStatsNode.stats.Eval0.{step_key}.json",
                "Eval1_mse_cache_key": f"state/MseStatsNode.stats.Eval1.{step_key}.json",
                "Eval2_mse_cache_key": f"state/MseStatsNode.stats.Eval2.{step_key}.json",
                "merge_cache_key": f"state/MergeGroupStatsNode.merge.{step_key}.json",
            }
        )
        return out["Merge"]["merge"]

    # 4) Before model
    model0 = BasicModel({BasicModel.m: 0.0, BasicModel.b: 0.0})
    before = eval_group(model0, "before")

    # 5) Optimize one shared model across all three datasets
    rng = random.Random(int(time.time_ns() % (2**31 - 1)))
    m = float(model0.m)
    b = float(model0.b)
    best = float(before["group"]["mse"])

    lam = 1e-3
    jitter = 0.05
    no_improve = 0

    for step in range(100):
        cur = eval_group(BasicModel({BasicModel.m: m, BasicModel.b: b}), f"step{step:03d}")
        g = cur["group"]

        g1 = float(g["grad_dm"])
        g2 = float(g["grad_db"])
        h11 = float(g["hess_dmm"]) + lam
        h12 = float(g["hess_dmb"])
        h21 = float(g["hess_dmb"])
        h22 = float(g["hess_dbb"]) + lam

        sol = solve_2x2(h11, h12, h21, h22, g1, g2)
        if sol is None:
            dm = rng.gauss(0.0, jitter)
            db = rng.gauss(0.0, jitter)
        else:
            dm, db = sol
            dm = -dm + rng.gauss(0.0, jitter)
            db = -db + rng.gauss(0.0, jitter)

        cand_m = m + dm
        cand_b = b + db
        cand = eval_group(BasicModel({BasicModel.m: cand_m, BasicModel.b: cand_b}), f"cand{step:03d}")
        cand_mse = float(cand["group"]["mse"])

        if cand_mse < best:
            m, b = cand_m, cand_b
            best = cand_mse
            lam = max(lam / 2.0, 1e-9)
            jitter = max(jitter * 0.98, 1e-4)
            no_improve = 0
        else:
            lam = min(lam * 2.0, 1e6)
            jitter = max(jitter * 0.99, 1e-4)
            no_improve += 1

        if no_improve >= 20:
            break

    model_opt = BasicModel({BasicModel.m: m, BasicModel.b: b})
    after = eval_group(model_opt, "after")

    # 6) Render 6 plots
    rec_before = {r["eval_id"]: r for r in before["records"]}
    rec_after = {r["eval_id"]: r for r in after["records"]}

    p0b = render_dataset_plot(root_path=root_path, points=points0, model=model0, loss_l1=rec_before["Eval0"]["loss_l1"], tag="ds0_before")
    p1b = render_dataset_plot(root_path=root_path, points=points1, model=model0, loss_l1=rec_before["Eval1"]["loss_l1"], tag="ds1_before")
    p2b = render_dataset_plot(root_path=root_path, points=points2, model=model0, loss_l1=rec_before["Eval2"]["loss_l1"], tag="ds2_before")

    p0a = render_dataset_plot(root_path=root_path, points=points0, model=model_opt, loss_l1=rec_after["Eval0"]["loss_l1"], tag="ds0_after")
    p1a = render_dataset_plot(root_path=root_path, points=points1, model=model_opt, loss_l1=rec_after["Eval1"]["loss_l1"], tag="ds1_after")
    p2a = render_dataset_plot(root_path=root_path, points=points2, model=model_opt, loss_l1=rec_after["Eval2"]["loss_l1"], tag="ds2_after")

    print("seeds:", {"ds0": seed0, "ds1": seed1, "ds2": seed2})
    print("start_model:", {"m": float(model0.m), "b": float(model0.b), "group_mse": float(before["group"]["mse"])})
    print("final_model:", {"m": float(model_opt.m), "b": float(model_opt.b), "group_mse": float(after["group"]["mse"])})
    print("before_records:", before["records"])
    print("after_records:", after["records"])
    print("image_paths:")
    print(str(p0b))
    print(str(p1b))
    print(str(p2b))
    print(str(p0a))
    print(str(p1a))
    print(str(p2a))


if __name__ == "__main__":
    main()
