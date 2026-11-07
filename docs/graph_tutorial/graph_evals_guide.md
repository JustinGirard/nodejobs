# Graph Evals Guide (05.5, Static Batch)

This guide is a simple, static example of running multiple independent eval subgraphs and merging their outputs.

What we build:

1. A small exported eval subgraph (JSON) that publishes:
   - `eval_id`
   - `m`, `b`
   - `loss_l1`, `n`
2. A static outer graph that runs the same exported subgraph 3 times:
   - `Eval0`, `Eval1`, `Eval2`
3. A merge node that gathers results into one output dict:
   - `{ "records": [ ... ] }`

This is not an optimization guide:

- We do not compute “best”.
- Each eval is treated as an independent concern.

Prereqs:

- `guide_tutoial_common_gaussian.py`
- `guide_tutoial_05_loss_and_residual_plot.py` (for `AbsLossNode`)

---

## Tutorial 05.5: Three Static Evals + Merge

### Task: Create `guide_tutoial_05_5_static_evals.py`

Create a new file: `guide_tutoial_05_5_static_evals.py` in this directory.

---

### Step 1: Build the Exported Eval Subgraph

We export a subgraph JSON file so `GraphExecutionNode` can derive field aliases from the external `__ref`s.
That makes the values payload easy to understand:

- `values["points"]`
- `values["model"]`
- `values["eval_id"]`

### Code (subgraph shape)

```json
{
  "__outputs": {
    "eval_id": ["__ref","Meta","echo","eval_id"],
    "m": ["__ref","Meta","echo","m"],
    "b": ["__ref","Meta","echo","b"],
    "loss_l1": ["__ref","Loss","loss","loss_l1"],
    "n": ["__ref","Loss","loss","n"]
  },
  "Meta": {
    "clas": "EchoEvalNode",
    "dependencies": {
      "echo": {}
    }
  },
  "Loss": {
    "clas": "AbsLossNode",
    "dependencies": {
      "loss": {}
    }
  }
}
```

### Discussion

- `EchoEvalNode` ensures we publish the tested parameters (`m,b`) alongside the loss.
- `AbsLossNode` computes mean absolute error (`loss_l1`).
- `__outputs` defines the published contract that the wrapper returns.

This guide uses dot-path overrides in `GraphExecutionNode.values` (e.g. `Meta.echo.cache_key`).
That keeps everything using the standard argument name `cache_key` and avoids confusing alias fields.

---

### Step 2: Build the Outer Static Graph (3 Evaluators)

The outer graph contains:

- 3 `GraphExecutionNode` nodes (`Eval0/1/2`)
- 1 merge node (`Merge`)

Each Eval node runs the same exported eval subgraph path, but with different `values`.

---

### Step 3: Merge the Results

The merge node receives a list of results:

- `["__ref","Eval0","execute"]`
- `["__ref","Eval1","execute"]`
- `["__ref","Eval2","execute"]`

And returns:

```json
{"records": [{"eval_id":"Eval0","m":...,"b":...,"loss_l1":...,"n":...}, ...]}
```

No “best” is computed.

---

### Step 4: Run

`python3 guide_tutoial_05_5_static_evals.py`

Outputs:

- Prints merged records
- Writes `./_graph_tutorial_run_05_5_evals/OUTPUT/evals.json`
