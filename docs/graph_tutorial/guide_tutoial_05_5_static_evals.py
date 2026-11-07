"""
guide_tutoial_05_5_static_evals.py

Tutorial 05.5: Static batch evals (3 subgraph executions) + merge.

Goal:
- Execute the same exported eval subgraph three times with three static model params.
- Gather only model params + loss into one merged output dict.

Run:
  python3 guide_tutoial_05_5_static_evals.py

Outputs:
- Writes: ./_graph_tutorial_run_05_5_evals/OUTPUT/evals.json
"""

import json
import time
from pathlib import Path

from nodejobs.dependencies.CachingComponent import CachingComponent
from nodejobs.dependencies.graph_executor import GraphExecutionNode
from processing_graph.BaseProcessor import BaseProcessor

from guide_tutoial_common_gaussian import BasicModel, Gaussian2DParams, Point, sample_gaussian2d_points
from guide_tutoial_05_loss_and_residual_plot import AbsLossNode


class EchoEvalNode(CachingComponent):
    """Publish eval_id + model params for downstream merge."""

    def echo(
        self,
        eval_id: str,
        model: BasicModel,
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for EchoEvalNode.echo")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("eval_id"), str):
                return cached

        model = BasicModel(model)
        out = {
            "eval_id": str(eval_id),
            "m": float(model.m),
            "b": float(model.b),
            "computed_at": time.time(),
        }
        self.save_cache_path(cache_key, out)
        return out


class MergeEvalRecordsNode(CachingComponent):
    """Merge eval outputs into one dict: {'records': [...]} (no ranking/selection)."""

    def merge(
        self,
        evals: list[dict],
        overwrite: bool = False,
        cache_key: str | None = None,
    ) -> dict:
        if cache_key is None:
            raise Exception("cache_key is required for MergeEvalRecordsNode.merge")

        if not overwrite:
            cached = self.load_cache_path(cache_key)
            if isinstance(cached, dict) and isinstance(cached.get("records"), list):
                return cached

        if not isinstance(evals, list):
            raise TypeError("evals must be a list[dict]")

        records = []
        for i, e in enumerate(evals):
            if not isinstance(e, dict):
                raise TypeError(f"evals[{i}] must be a dict")

            records.append(
                {
                    "eval_id": e.get("eval_id"),
                    "m": e.get("m"),
                    "b": e.get("b"),
                    "loss_l1": e.get("loss_l1"),
                    "n": e.get("n"),
                }
            )

        out = {"records": records, "computed_at": time.time()}
        self.save_cache_path(cache_key, out)
        return out


def write_eval_subgraph(*, export_path: Path, root: str) -> None:
    def settings(node_id: str) -> dict:
        return {"data_session": {"root": root, "unlocked": True}, "cache_path": f"{node_id}_node/DATA"}

    g = {
        "__outputs": {
            "eval_id": ["__ref", "Meta", "echo", "eval_id"],
            "m": ["__ref", "Meta", "echo", "m"],
            "b": ["__ref", "Meta", "echo", "b"],
            "loss_l1": ["__ref", "Loss", "loss", "loss_l1"],
            "n": ["__ref", "Loss", "loss", "n"],
        },
        "Meta": {
            "name": "Meta",
            "clas": "EchoEvalNode",
            "settings": settings("Meta"),
            # Filled by GraphExecutionNode dot-path overrides (see build_outer_graph()).
            "dependencies": {"echo": {}},
        },
        "Loss": {
            "name": "Loss",
            "clas": "AbsLossNode",
            "settings": settings("Loss"),
            # Filled by GraphExecutionNode dot-path overrides (see build_outer_graph()).
            "dependencies": {"loss": {}},
        },
    }

    export_path.write_text(json.dumps(g, indent=2) + "\n", encoding="utf-8")


def build_outer_graph(*, root: str, eval_subgraph_path: str) -> dict:
    def settings(node_id: str) -> dict:
        return {"data_session": {"root": root, "unlocked": True}, "cache_path": f"{node_id}_node/DATA"}

    def eval_node(node_id: str, model: BasicModel) -> dict:
        return {
            "name": node_id,
            "clas": "GraphExecutionNode",
            "settings": settings(node_id),
            "dependencies": {
                "execute": {
                    "graph": eval_subgraph_path,
                    "values": {
                        # Use dot-path overrides so we can use standard arg names like `cache_key`.
                        "Meta.echo.eval_id": node_id,
                        "Meta.echo.model": model,
                        "Meta.echo.overwrite": True,
                        "Meta.echo.cache_key": f"state/EchoEvalNode.echo.{node_id}.json",
                        "Loss.loss.points": ["__ref", "points"],
                        "Loss.loss.model": model,
                        "Loss.loss.overwrite": True,
                        "Loss.loss.cache_key": f"state/AbsLossNode.loss.{node_id}.json",
                    },
                    "overwrite": True,
                    "overwrite_sub": False,
                    "cache_key": f"evals/{node_id}",
                }
            },
        }

    return {
        "Eval0": eval_node("Eval0", BasicModel({BasicModel.m: -1.0, BasicModel.b: 1.0})),
        "Eval1": eval_node("Eval1", BasicModel({BasicModel.m: 0.8, BasicModel.b: -0.2})),
        "Eval2": eval_node("Eval2", BasicModel({BasicModel.m: 1.5, BasicModel.b: 0.3})),
        "Merge": {
            "name": "Merge",
            "clas": "MergeEvalRecordsNode",
            "settings": settings("Merge"),
            "dependencies": {
                "merge": {
                    "evals": [
                        ["__ref", "Eval0", "execute"],
                        ["__ref", "Eval1", "execute"],
                        ["__ref", "Eval2", "execute"],
                    ],
                    "overwrite": True,
                    "cache_key": "state/MergeEvalRecordsNode.merge.evals_3.json",
                }
            },
        },
    }


def main() -> None:
    root_path = Path.cwd() / "_graph_tutorial_run_05_5_evals"
    root_path.mkdir(parents=True, exist_ok=True)
    (root_path / "OUTPUT").mkdir(parents=True, exist_ok=True)
    (root_path / "EXPORTS").mkdir(parents=True, exist_ok=True)

    # One dataset for all evals
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

    # Export eval subgraph once and reuse it three times
    export_path = root_path / "EXPORTS" / "eval_subgraph.json"
    write_eval_subgraph(export_path=export_path, root=str(root_path))

    # Bind context for GraphExecutionNode subgraph execution
    GraphExecutionNode.set_context({"EchoEvalNode": EchoEvalNode, "AbsLossNode": AbsLossNode})

    outer_graph = build_outer_graph(root=str(root_path), eval_subgraph_path=str(export_path))
    bp = BaseProcessor(
        outer_graph,
        context={
            "GraphExecutionNode": GraphExecutionNode,
            "MergeEvalRecordsNode": MergeEvalRecordsNode,
        },
    )

    out = bp.process({"points": points})
    merged = out["Merge"]["merge"]

    out_path = root_path / "OUTPUT" / "evals.json"
    out_path.write_text(json.dumps(merged, indent=2) + "\n", encoding="utf-8")

    print("seed_used:", seed_used)
    print("records:", merged["records"])
    print("output_paths:")
    print(str(out_path))


if __name__ == "__main__":
    main()
