# Graph Tutorial Overview

## Graph Bundle Format and Addressing (Canonical)

This repo uses a stable bundle convention for exported graphs and their execution artifacts. Many tools and endpoints assume this layout (including `GraphExecutionNode` and MCP `graph_op`).

### Bundle layout (on disk)

A “graph bundle” is a directory tree with at least one exported execution graph JSON under `executions/`:

```
<bundle_root>/                       # this is "root_path"
  executions/
    <graph_name>.json                # this is "execution_graph_path" (relative)
    runs/
      <cache_key_hash>.json          # graph-level run envelopes / guards
    <graph_name>/...                 # derived run/artifact directory (by export stem)
  nd_<node_id>/...                   # per-node caches/artifacts (often)
```

### Addressing rule: root_path vs execution_graph_path

When a tool/API needs to target an exported graph JSON, it uses two layers:

- `root_path`: the **bundle root directory** (authorization/containment boundary).
- `execution_graph_path`: the **export graph JSON path inside the bundle**, usually `executions/<graph_name>.json`.

Example:
- `root_path = "/abs/path/to/my_bundle"`
- `execution_graph_path = "executions/cat_graph.json"`
- Absolute export file = `"/abs/path/to/my_bundle/executions/cat_graph.json"`

### Important: execution_graph_path is not a run directory

`execution_graph_path` points to the exported graph JSON file. Runtime outputs are placed under directories derived from that export (typically under `executions/<graph_name>/...` and `executions/runs/...`).

### Special case: "__root" registry (server-managed bundle)

Some MCP operations treat `root_path="__root"` as an alias for the server-managed registry bundle:

- `<public_root>/.node_mcp_server/root_graph/`

The canonical registry export graph JSON is:

- `executions/root_graph.json`

This folder is the curated entrypoint for the graph tutorial series.

Purpose:
These guides teach the repo’s workflow-graph style used for agents, evals, and optimizers. After completing them you can: author cacheable node methods with stable input/output “slots”, wire nodes declaratively with `__ref`, run graphs deterministically via `BaseProcessor`, package multi-node pipelines as reusable subgraphs via `GraphExecutionNode` + `__outputs`, and produce reproducible artifacts (JSON, PNGs) with explicit cache invalidation.

Conventions:
- Wiring: `['__ref','key']` reads from the input feature dict (`feature['key']`); `['__ref','Node','method','slot']` reads an upstream node output (`feature['Node']['method']['slot']`).
- Execution: `BaseProcessor` runs a graph dict directly; `GraphExecutionNode` runs an exported graph (or inline dict) as a single “node” and can publish a stable API via `__outputs`.
- Caching: every callable node method takes `overwrite` and `cache_key`; node-level caches are per-method. `GraphExecutionNode` adds graph-envelope caching keyed by its own `cache_key`.
- Artifacts: nodes can return bytes (e.g. PNG) in output dicts; scripts/drivers write “final artifacts” under `OUTPUT/` and print paths.

How to run:
Run scripts from this folder, e.g. `python3 guide_tutoial_01_scatter.py`. Each script creates a dedicated run directory under the current working directory (e.g. `_graph_tutorial_run_05_loss/`) containing `INPUT/` (optional fixtures), `EXPORTS/` (when exporting subgraphs), `OUTPUT/` (final artifacts), and per-node caches under `*_node/DATA/`. Scripts print `wrote:` plus `image_paths:` so you can quickly find outputs.

---

## Why Workflows (Agents, Evals, Optimizers)

These tutorials intentionally model agents, evaluation suites, and optimizers as workflows (graphs and subgraphs). The reasons are architectural as much as they are practical:

1. You get a stable orchestration boundary (Composite + Facade). Modeling an agent chain as a graph (and a subgraph as a single node) lets you treat many steps as one unit with a clear interface. This is a Composite (workflows made of workflows) and a Facade (simple external API over complex internals), which is crucial when you need to ship multi-stage LLM flows without exposing internal glue.
2. Dependency Injection becomes first-class (Strategy + Abstract Factory). With `clas` resolved via a runtime `context`, the workflow describes what role a node plays, not the concrete implementation. That enables Strategy swaps (different retrievers, different LLM providers, different scoring logic) and an Abstract Factory style host that binds implementations per environment (local vs CI vs prod).
3. You can publish contracts and prevent coupling (Ports & Adapters / Hexagonal). `__outputs` is effectively a port: the workflow publishes a small stable set of outputs and hides internal node ids. Downstream consumers and agents depend on the contract, not the wiring, which prevents brittle integration when you refactor internals.
4. Repeatability is engineered via explicit caching (Memoization + Command). Each node method is a Command with explicit inputs (`overwrite`, `cache_key`) and cached outputs. This turns agent runs and eval runs into reproducible computations instead of ephemeral side effects, which is essential for cost control and auditability.
5. Evals become programs, not notebooks (Test Harness pattern). Static eval suites (multiple `GraphExecutionNode` executions + merge) behave like a test harness: fixed scenarios, standardized outputs, consolidated report. You can run them in CI, on schedule, or triggered by an agent, with deterministic artifacts and comparable metrics.
6. Optimizers become iterative controllers over a fixed interface (Controller + Feedback Loop). When an optimizer only depends on published metrics from a workflow subgraph, it becomes a generic controller: propose params, run eval workflow, read metrics, update. That separation makes tuning safer, observable, and modular.
7. You get observability-friendly structure (traceable runs). Workflows naturally surface intermediate outputs, artifacts, and parameters. With stable cache keys and run roots, you can correlate outputs to a run id and replay analyses, which is the practical backbone of production debugging for agent systems.
8. You can scale from single-run to batch to group objectives (Map/Reduce pattern). Running the same subgraph multiple times with different inputs is Map; merging outputs is Reduce. That pattern is what you need for prompt sweeps, tool policy sweeps, dataset sweeps, cohort analyses, and group optimization.
9. You can manage cost and correctness separately (CQRS-like split). Workflows let you separate expensive compute stages (LLM calls, retrieval) from lightweight reporting stages (merges, plots, summaries). With caching and explicit overwrite, you can run the expensive path rarely and the reporting path often.
10. Agents become safer because what they are allowed to change is constrained (Configuration-as-Code + Least Authority). When an agent operates by supplying values to a workflow (inputs, params, cache keys) rather than rewriting imperative code, you narrow the blast radius and make changes easier to review and roll back.

---

## Guides (In Order)

### 1) `graph_processing_guide.md`
Paragraph: This guide is the foundation of the entire series: it explains the graph execution model (what a “graph” dict/JSON looks like, how node specs are structured, and how `__ref` wires values between nodes) and how graphs are run end-to-end through `processing_graph.BaseProcessor`. It teaches the repeatable node authoring pattern used throughout the repo: nodes subclass `CachingComponent`, every callable method accepts `overwrite` and `cache_key`, and caching is explicit and per-method via `load_cache_path`/`save_cache_path` under a `DataSession` root and `cache_path` namespace. It also covers dependency injection via the `context` map (string `clas` resolution), how results are materialized into the feature dict under `feature[node_id][method_name]`, and how binary outputs (like PNG bytes) can be cached safely. The outcome is that a reader can author small nodes, wire them declaratively, execute them deterministically, and understand where artifacts and cache entries land on disk.

Key takeaways (practical):
1. Make the flow declarative so it’s teachable and reviewable. Practical outcome: teammates/agents can understand the pipeline without tracing code. Technical detail: a graph is a dict/JSON keyed by node id; each node has `clas`, `settings`, and `dependencies` so intent lives in data, not control flow.
2. Make dependencies explicit to prevent hidden coupling. Practical outcome: safe reuse/reorder of steps without “mystery variables.” Technical detail: use `__ref` for wiring: `['__ref','key']` reads `feature[key]` and `['__ref','Node','method','slot']` reads upstream `feature['Node']['method']['slot']`.
3. Standardize execution so debugging and behavior are predictable. Practical outcome: one mental model across all flows. Technical detail: run graphs via `BaseProcessor.process(feature)`, which resolves `dependencies`, runs node methods, and writes outputs back into the feature under `feature[node_id][method_name]`.
4. Cache work at the smallest sensible unit (node method) to make iteration cheap. Practical outcome: reruns skip expensive steps (LLM calls, stats, plots). Technical detail: nodes subclass `CachingComponent` and cache per-method using `load_cache_path(cache_key)` / `save_cache_path(cache_key, out)` under `DataSession.root` + `cache_path`.
5. Control recomputation intentionally. Practical outcome: avoid stale results and avoid waste. Technical detail: every callable method takes `overwrite: bool` and `cache_key: str | None`; if `overwrite=False` and cache exists, return cached, else recompute and persist.
6. Treat artifacts as first-class cached outputs. Practical outcome: pipelines produce inspectable evidence (plots, rendered outputs) that’s stable across runs. Technical detail: `CachingComponent` is binary-safe (e.g., `png_bytes: bytes` can be saved/rehydrated) so artifacts can be returned as part of node output dicts.
7. Make implementations swappable without rewiring graphs. Practical outcome: easy mocking, environment swaps, and upgrades. Technical detail: `clas` is a string resolved through the executor `context` map (`{"ClassName": PythonClass}`), enabling dependency injection.
8. Keep output shape stable so downstream composition is easy. Practical outcome: add stages without refactoring everything. Technical detail: downstream nodes reference specific upstream slots via `__ref` and node methods should return predictable dict fields (“slots”).
9. Keep graphs focused on compute; keep drivers responsible for persistence. Practical outcome: portability across local/CI/prod and less cache pollution. Technical detail: nodes return dicts/bytes; the tutorial driver writes final files under `OUTPUT/` and prints paths, while caches live under `*_node/DATA`.
10. Engineer reproducibility and traceability. Practical outcome: you can compare experiments and trust results. Technical detail: use fixed seeds when needed, version cache keys when behavior changes, and keep explicit roots/namespaces (`DataSession.root`, `cache_path`).

Tutorials under this guide:
1. `guide_tutoial_01_scatter.py`
Builds a 3-node `BaseProcessor` graph (`Load -> Stats -> Scatter`) using dataset-style nodes and `__ref` fan-in wiring. Produces `_graph_tutorial_run/OUTPUT/scatter.png` and writes/uses `_graph_tutorial_run/INPUT/points_10.json`; caches live under `_graph_tutorial_run/*_node/DATA/`.

---

### 2) `graph_subgraph_gude.md`
Paragraph: This guide introduces subgraphs as a first-class packaging unit by teaching `GraphExecutionNode` as a wrapper around `BaseProcessor` execution. It focuses on turning “a multi-node pipeline” into “a single node” by executing a graph by exported JSON path, binding inputs at the wrapper boundary, and publishing a stable external API via `__outputs` so callers don’t need to know internal node ids. It clarifies the two-layer caching model you get with subgraphs: graph-level run caching (the wrapper’s execution envelope keyed by `cache_key` with run guards) versus node-level caching (each node’s own `CachingComponent` caches), and how `overwrite` and `overwrite_sub` affect each layer. The outcome is that a reader can export a subgraph once, reuse it many times safely (with isolated run roots and cache namespaces), and build “meta-graphs” where each node is itself a whole graph execution.

Key takeaways (practical):
1. Package multi-step pipelines into one reusable unit. Practical outcome: teams treat “a whole chain” as a single callable component. Technical detail: `GraphExecutionNode` wraps `BaseProcessor` execution of an exported JSON graph (or inline dict) and exposes it as one node in a parent graph.
2. Define a stable contract so callers don’t depend on internals. Practical outcome: you can refactor subgraph internals without breaking downstream users/agents. Technical detail: use `__outputs` in the exported subgraph; `GraphExecutionNode` resolves these refs and injects published outputs into the returned dict as top-level keys.
3. Get predictable caching semantics at two levels. Practical outcome: fast reruns and controlled invalidation. Technical detail: graph-level caching lives in `GraphExecutionNode.execute(cache_key=..., overwrite=...)` (run envelope + guard), while node-level caching stays inside each node’s `CachingComponent` method cache gate (`overwrite`/`cache_key`).
4. Prevent duplicate spend and race-condition artifacts. Practical outcome: fewer repeated LLM calls / fewer “two runs overwrote each other.” Technical detail: `GraphExecutionNode` uses run guards keyed by its `cache_key` under `executions/runs` to serialize/mark in-progress work and cache run results.
5. Keep subgraph execution isolated so reuse doesn’t collide. Practical outcome: safe fan-out and batch runs. Technical detail: each wrapper instance uses its own `cache_path` namespace; `GraphExecutionNode` can run under a dedicated run root (“bundle root”) so artifacts and caches are separated per execution node.
6. Make exported graphs safe to reuse across different callers. Practical outcome: the same export can be used many times without accidental writeback collisions. Technical detail: when a graph is executed by path, `GraphExecutionNode` may copy it into its cache namespace for execution safety.
7. Bind external inputs cleanly at the wrapper boundary. Practical outcome: “same subgraph, different inputs” becomes trivial (evaluation suites, prompt variants, dataset batches). Technical detail: `GraphExecutionNode.execute(values=...)` applies the values payload into the subgraph’s external refs.
8. Have a reliable escape hatch for advanced injection. Practical outcome: automation isn’t blocked by alias schema mismatches. Technical detail: dot-path override keys like `Node.method.arg` are expanded into `Node.dependencies.method.arg` and written directly.
9. Publish additional aliases without editing the export. Practical outcome: evolve outputs gradually and keep compatibility. Technical detail: wrapper `outputs_map` injects resolved outputs into the returned result dict (in addition to the subgraph’s `__outputs`).
10. Make artifact paths usable by humans and tooling. Practical outcome: printed paths/links remain correct regardless of where execution happened. Technical detail: `GraphExecutionNode` normalizes output paths at the boundary so callers receive outer-root-stable paths.

Tutorials under this guide:
1. `guide_tutoial_02_dynamic_subgraphs.py`
Exports an inline-records scatter subgraph JSON, then dynamically builds a meta-graph with `Exec0..Exec9` `GraphExecutionNode` instances (unique cache namespaces and graph-level cache keys). Produces `_graph_tutorial_run_02_dynamic/EXPORTS/scatter_subgraph_inline_records.json` and `_graph_tutorial_run_02_dynamic/OUTPUT/scatter_00.png` through `scatter_09.png`.

---

### 3) `graph_optimization_guide.md`
Paragraph: This guide starts the ML-flavored part of the series and is written as a build-along: it teaches how to represent model execution, visualization, and loss as graph nodes while staying consistent with the same `__ref` wiring and cache-gate conventions from earlier guides. It introduces typed payloads via `BaseData` (notably `Point`, `BasicModel`, and `Gaussian2DParams`) and a shared module (`guide_tutoial_common_gaussian.py`) so multiple tutorials can reuse the same mature 2D Gaussian generator (translation + ellipse spread + rotation) without duplication. The outcomes are practical and artifact-driven: readers build a minimal model-execution graph (predict `y = m*x + b` over a dataset of points), then a visualization graph that overlays a model line over a scatter plot, then a loss + residual visualization where loss becomes tangible via vertical residual segments. This guide establishes the “shape discipline” used later: graphs pass datasets as `list[Point]`, models as `BasicModel`, and artifacts (plots) as PNG bytes written by the driver, with scripts printing output paths for easy inspection.

Key takeaways (practical):
1. Typed payloads reduce agent mistakes and glue-code churn. Practical outcome: fewer broken runs from missing keys / inconsistent schemas. Technical detail: use `BaseData` types (`Point`, `BasicModel`, `Gaussian2DParams`) and pass them through graphs as first-class inputs (`points`, `model`, `gauss`) rather than ad-hoc dicts.
2. A shared “common” module prevents tutorial drift and enables reuse. Practical outcome: teams build a small library of reusable primitives instead of copying code between scripts. Technical detail: `guide_tutoial_common_gaussian.py` centralizes the 2D Gaussian sampling function and shared `BaseData` classes so Tutorial 03/04/05 stay consistent.
3. Model execution as a dataset transform matches real pipeline workloads. Practical outcome: the same code path handles single-point demos and batch evaluation. Technical detail: `LinearPredictNode.predict(points, model)` operates on `list[Point]` and returns `points_pred` (list of `SimplePoint`) rather than only scalar `predict_one`.
4. Uniform cache gates keep ML-ish nodes predictable to run and debug. Practical outcome: cheap reruns and explicit recompute control like the non-ML graphs. Technical detail: every node method keeps the same `overwrite` + `cache_key` pattern and persists via `CachingComponent.save_cache_path`.
5. Compute stays in nodes; persistence stays in drivers. Practical outcome: graphs remain portable across environments while drivers decide where artifacts go. Technical detail: plot nodes return `png_bytes` and scripts write files under `OUTPUT/` and print paths for preview.
6. Visualization is a downstream branch that demonstrates real fan-in. Practical outcome: teams can add diagnostics without disturbing upstream computation. Technical detail: `ScatterLinePlotNode.plot(points, model, ...)` consumes both dataset and model params; it’s wired via `__ref` from generator output plus external model input.
7. The “mature Gaussian” parameterization makes datasets controllable and interpretable. Practical outcome: experiments are repeatable and explainable. Technical detail: `Gaussian2DParams(mu_x, mu_y, std_major, std_minor, theta_deg)` defines translation + ellipse spread + rotation; sampling is deterministic given a fixed `seed`.
8. Loss is both numeric and visual, which accelerates debugging and trust. Practical outcome: engineers can see why a score is high/low. Technical detail: `AbsLossNode.loss` computes `loss_l1`, while `LossPlotNode.plot` draws vertical residual lines from `(x,y)` to `(x, m*x+b)`.
9. Plot correctness matters for teaching; preserve geometry. Practical outcome: teams don’t learn misleading visual intuition from distorted charts. Technical detail: use `ax.set_aspect("equal")` so ellipses and residuals render truthfully.
10. Early shape conventions enable composition into eval/optimization later. Practical outcome: later guides can build batch evals and optimization loops without refactoring earlier nodes. Technical detail: keep stable keys and slots (`points`, `model`, `loss_l1`, `png_bytes`, `points_pred`) across tutorials.

Shared tutorial module(s):
1. `guide_tutoial_common_gaussian.py`
Defines shared typed payloads (`Point`, `BasicModel`, `Gaussian2DParams`) and a reusable rotated-ellipse Gaussian sampler (`sample_gaussian2d_points`). This keeps Tutorials 03–07 consistent (shared schemas, deterministic/reproducible seeds) without copy-pasting math/types across scripts.

Tutorials under this guide:
1. `guide_tutoial_03_linear_predict_minimal.py`
One-node “model execution” graph: `LinearPredictNode.predict(points, model)` returns `points_pred` and metadata. Outputs are printed to stdout; caches live under `_graph_tutorial_run_03_linear_predict/Predict_node/DATA/`.

2. `guide_tutoial_04_scatter_line_plot.py`
Two-node graph `Gen.generate -> Plot.plot` that generates Gaussian points and renders scatter + model line to a PNG. Produces `_graph_tutorial_run_04_scatter_line/OUTPUT/scatter_line.png`.

3. `guide_tutoial_05_loss_and_residual_plot.py`
Three-node graph `Gen.generate -> Loss.loss -> Plot.plot` that computes L1 loss and renders residual (vertical) lines to a PNG. Produces `_graph_tutorial_run_05_loss/OUTPUT/loss_plot.png` and prints `loss_l1`.

---

### 4) `graph_evals_guide.md`
Paragraph: This guide is intentionally not optimization: it teaches how to run a small, static batch of evaluations (three fixed subgraph executions) and merge the outputs into a single report object. The emphasis is on clarity and test-like structure: each eval is a named concern (`Eval0/1/2`), each runs the same exported eval subgraph, and the only merged outputs are the model parameters and loss metrics (no points, no ranking, no “best”). It highlights a practical operational lesson: using an exported subgraph path allows `GraphExecutionNode` to derive field aliases cleanly, while still keeping method-level `cache_key` usage standard; and when necessary, dot-path overrides let you inject values into subgraph dependencies without inventing nonstandard parameter names. The outcome is a reusable pattern for “run N fixed graph checks and consolidate outputs”, useful for regression-style validation and comparison suites.

Key takeaways (practical):
1. Static eval suites make quality measurable without turning everything into training. Practical outcome: repeatable checks for prompts/tools/parsers without optimization bias. Technical detail: the outer graph is a fixed meta-graph with `Eval0/1/2` nodes (each `clas: "GraphExecutionNode"`) and one downstream merge node.
2. Reusing one eval contract prevents drift across evaluators. Practical outcome: apples-to-apples comparisons over time. Technical detail: all eval nodes execute the same exported subgraph JSON path (same `__outputs` contract) but inject different model values.
3. Publishing a small output contract keeps reports stable and automatable. Practical outcome: CI/dashboards/agents consume a predictable schema. Technical detail: the exported eval subgraph defines `__outputs` (e.g. `eval_id`, `m`, `b`, `loss_l1`, `n`) and `GraphExecutionNode` materializes them as top-level keys.
4. Exported graphs keep input binding predictable for teams and agents. Practical outcome: fewer “why did this break when automated” failures. Technical detail: using `graph=<path>` lets `GraphExecutionNode.get_fields()` derive field aliases; values can be provided cleanly and executed under an isolated cache namespace.
5. Dot-path overrides are the escape hatch for reliable injection. Practical outcome: you can still run evals even when alias fields aren’t available or are confusing. Technical detail: pass keys like `Meta.echo.cache_key` / `Loss.loss.points` inside `GraphExecutionNode.values` to write directly into `node.dependencies.<method>.<arg>`.
6. Unique per-eval run keys prevent collisions in concurrent runs. Practical outcome: safe parallel eval execution in CI or multi-agent runs. Technical detail: each `GraphExecutionNode.execute` uses a unique graph-level `cache_key` (`evals/Eval0`, etc.) so run guards and envelope caches do not collide.
7. Node-level caching still reduces cost inside eval suites. Practical outcome: cheap reruns even when suites get large. Technical detail: subgraph node methods still accept `overwrite` and standard `cache_key` and can cache per-eval (`state/<Node>.<method>.<EvalId>.json`).
8. Merged outputs should be intentionally small and reviewable. Practical outcome: humans and agents get “the report”, not a nested execution tree. Technical detail: the merge node performs schema-select and emits only `{eval_id,m,b,loss_l1,n}` in a `records` list.
9. Not computing “best” keeps the pattern test-like and extensible. Practical outcome: future evals can represent different concerns without forced ranking. Technical detail: the merge node does not sort/select; it only aggregates results into `records`.
10. Shared dataset isolates the variable under test. Practical outcome: if loss changes, it’s because the model/prompt/tool changed, not the data. Technical detail: the driver generates one `points` dataset once and passes it into all eval nodes so each eval runs against identical data.

Tutorials under this guide:
1. `guide_tutoial_05_5_static_evals.py`
Exports a small eval subgraph (`__outputs`: `eval_id`, `m`, `b`, `loss_l1`, `n`), runs it 3 times via `GraphExecutionNode` (`Eval0/1/2`), then merges into a single report. Produces `_graph_tutorial_run_05_5_evals/EXPORTS/eval_subgraph.json` and `_graph_tutorial_run_05_5_evals/OUTPUT/evals.json`.

---

### 5) `graph_optimization_2_guide.md`
Paragraph: This guide moves from “measure loss” into “iteratively improve parameters” while keeping everything inside the same graph ecosystem: repeated subgraph execution, published outputs, explicit caching, and artifact outputs. It introduces Newton-like optimization mechanics for a linear model by adding an analytic MSE stats node that emits gradient and Hessian terms, then shows how to wrap that into an evaluation subgraph whose outputs can drive an optimization loop for up to 100 steps. It also captures an important implementation nuance for this repo: when executing inline subgraph dicts, `GraphExecutionNode` input binding may require dot-path dependency overrides (rather than alias fields derived from an export path), and the guide documents that boundary explicitly so future authors don’t hit the same “Unknown field alias” trap. The outcome is twofold: a single-dataset optimization demo that produces before/after residual plots, and a group-optimization demo that fits one shared model across three similar-but-not-identical datasets and outputs six plots (three before, three after) to teach the practical compromise of choosing one parameter set that performs acceptably across multiple data distributions.

Key takeaways (practical):
1. Iterative improvement becomes a repeatable pipeline pattern, not bespoke “training code”. Practical outcome: you can tune a chain/flow in small steps and reproduce the run later. Technical detail: each optimization step is “execute the same eval subgraph again” using `GraphExecutionNode.execute(...)`, with published outputs (`__outputs`) as the only interface to the optimizer loop.
2. Optimize a stable objective while still monitoring human-friendly metrics. Practical outcome: the tuning loop behaves predictably, and you still communicate outcomes clearly. Technical detail: Tutorial 06 optimizes MSE (smooth) via `MseStatsNode` but reports/plots L1 via `AbsLossNode` and `LossPlotNode` (residual lines).
3. Avoid heavy ML dependencies when the math is simple. Practical outcome: optimization can run anywhere (local/CI/agent) without PyTorch/JAX. Technical detail: `MseStatsNode.stats(points, model)` computes analytic `mse`, gradient (`grad_dm`, `grad_db`), and Hessian terms (`hess_dmm`, `hess_dmb`, `hess_dbb`) for the linear model.
4. Keep parameter updates auditable and robust. Practical outcome: on-call engineers can reason about updates and failures. Technical detail: `(m,b)` updates use a simple `solve_2x2(...)` and a damped Newton step (Levenberg-style, adding `lam` to the Hessian diagonal).
5. Prevent optimizer instability and production thrash with guardrails. Practical outcome: tuning doesn’t oscillate wildly. Technical detail: damping (`lam`) grows/shrinks based on improvement; small random jitter is added to steps to avoid degenerate cases; simple “no improvement” stopping criteria ends the loop.
6. Know when subgraph input binding requires dot-path overrides. Practical outcome: agents won’t get stuck on the “Unknown field alias” trap. Technical detail: when executing an inline subgraph dict, `GraphExecutionNode.values` must use keys like `LossMse.stats.points` / `LossL1.loss.model` (dot-path overrides into `dependencies`) rather than relying on field alias injection derived from `execution_graph_path`.
7. Avoid run collisions so repeated evaluation is safe under concurrency. Practical outcome: you can run optimization and eval suites in parallel without corrupting results. Technical detail: provide unique graph-level `cache_key` values for each `GraphExecutionNode` evaluation (and/or per-step identifiers) so run guards and envelope caches don’t collide.
8. Keep “before vs after” comparisons scientifically valid. Practical outcome: you can trust the improvement is due to parameter changes, not data changes. Technical detail: single-dataset optimization generates one dataset once per run and reuses it across all steps; plots are generated twice with distinct `plot_cache_key` values.
9. Group optimization teaches real-world generalization tradeoffs. Practical outcome: teams learn how one chain config must serve multiple cohorts/tasks. Technical detail: group evaluation runs the same evaluation logic across three datasets (three `GraphExecutionNode` nodes `Eval0..Eval2`) and aggregates stats via `MergeGroupStatsNode` into a group objective (mean MSE and mean gradient/Hessian terms).
10. Make tradeoffs observable, not abstract. Practical outcome: you can see who improved/worsened across segments. Technical detail: Tutorial 07 writes 6 residual plots (3 datasets before with the same model, 3 datasets after with the same optimized model) using `LossPlotNode` with vertical residual lines and consistent plotting settings (including aspect preservation).

Tutorials under this guide:
1. `guide_tutoial_06_optimize_mb.py`
Runs a damped Newton-like loop by repeatedly executing an inline loss/stats subgraph via `GraphExecutionNode`. Produces `_graph_tutorial_run_06_optimize/OUTPUT/unoptimized.png` and `_graph_tutorial_run_06_optimize/OUTPUT/optimized.png`.

2. `guide_tutoial_07_optimize_group_mb.py`
Runs group optimization for one shared `(m,b)` across 3 datasets using an exported eval subgraph + `GraphExecutionNode` fan-out and a merge node for group objective terms. Produces `_graph_tutorial_run_07_group_opt/EXPORTS/group_eval_subgraph.json` and 6 plots under `_graph_tutorial_run_07_group_opt/OUTPUT/`: `ds0_before.png`, `ds1_before.png`, `ds2_before.png`, `ds0_after.png`, `ds1_after.png`, `ds2_after.png`.

---

## Appendix

### Files Included

1. `graph_processing_guide.md`
Direct graph execution via `BaseProcessor`: graph shape, `__ref` wiring, node authoring pattern (`CachingComponent` + `overwrite/cache_key`), artifact caching.

2. `graph_subgraph_gude.md`
Subgraphs via `GraphExecutionNode`: `__outputs` contracts, graph-envelope caching vs node-level caching, run roots/namespaces, and collision avoidance.

3. `graph_optimization_guide.md`
Build-along for typed payloads, model execution, plotting, and loss visualization as graph nodes (Tutorials 03–05).

4. `graph_evals_guide.md`
Static batch evals: multiple `GraphExecutionNode` runs of the same eval subgraph + merge into a small report object (Tutorial 05.5).

5. `graph_optimization_2_guide.md`
Optimization loops using repeated subgraph execution (Tutorial 06) and group objectives across multiple datasets (Tutorial 07).

### Common Pitfalls / Debugging

1. GraphExecutionNode inline graph vs exported graph path
When executing `graph=<path-to-export.json>`, `GraphExecutionNode` can derive “field aliases” from external `__ref`s; this makes simple `values={...}` payloads ergonomic. For meta-graphs and repeated runs, prefer dot-path overrides like `Node.method.arg` (e.g. `LossMse.stats.points`) because they map deterministically into `dependencies` and avoid alias collisions (multiple `cache_key`/`overwrite` fields).

2. Caching gotchas
Avoid collisions by keeping `cache_path` unique per `GraphExecutionNode` instance and `cache_key` unique per run. Use node-level `overwrite=True` to force recompute of expensive steps (LLM calls/plots/stats) and change `cache_key` paths when semantics change. For stochastic generators, either pin seeds (repeatability) or record `seed_used` in outputs (reproducibility). For binary artifacts (PNG bytes), rely on `CachingComponent` binary-safe caching and write final files under `OUTPUT/` in the driver.
