# graph_execution_node_tests.py
import os
import shutil
import unittest
import json
from typing import Tuple

from nodejobs.dependencies.BaseSession import DataSession
from nodejobs.dependencies.graph_executor import GraphExecutionNode


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_TMP = os.path.join(THIS_DIR, "graph_executor_tests")


def fresh_root_and_session(test_name: str) -> Tuple[str, DataSession]:
    root = os.path.join(BASE_TMP, test_name)
    if os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root, exist_ok=True)
    session = DataSession({"root": root, DataSession.f_unlocked: True})
    return root, session


class GraphExecutionNodeTests(unittest.TestCase):
    def test_builds_dynamic_fields_from_external_refs(self):
        root, session = fresh_root_and_session(
            "test_builds_dynamic_fields_from_external_refs"
        )
        comp = GraphExecutionNode({"data_session": session, "cache_path": "./cache_graph_exec"})

        # Minimal graph: two external __ref prompts -> expect prompt + prompt_2 aliases.
        graph = {
            "nd_first": {
                "dependencies": {
                    "run_prediction": {
                        "query": {
                            "prompt": [
                                "__ref",
                                "missing_node",
                                "run_prediction",
                                "prompt",
                            ]
                        }
                    }
                }
            },
            "nd_second": {
                "dependencies": {
                    "run_prediction": {
                        "query": {
                            "prompt": [
                                "__ref",
                                "missing_node",
                                "run_prediction",
                                "prompt",
                            ]
                        }
                    }
                }
            },
        }

        temp_graph_path = os.path.join(root, "graph_with_external_ref.json")
        with open(temp_graph_path, "w", encoding="utf-8") as f:
            json.dump(graph, f)

        fields = comp.get_fields({"execution_graph_path": temp_graph_path})
        self.assertIsInstance(fields, dict)
        self.assertIn("prompt", fields)
        self.assertIn("prompt_2", fields)
        self.assertTrue(str(fields["prompt"].get("path", "")).endswith(".prompt"))
        self.assertTrue(
            str(fields["prompt_2"].get("path", "")).endswith(".prompt")
        )


if __name__ == "__main__":
    import dotenv

    dotenv.load_dotenv()
    # unittest.main()
    unittest.main(defaultTest="GraphExecutionNodeTests.test_two_node_graph_flat_args")


"""
================================================================================
NOTES: Graph Export vs Graph Run (FILES-ONLY)
================================================================================

This comment block is intentionally about *files* (what they are, and when they
are updated). It is not about return values, UI behavior, or any frontend layer.


TERMS

1) Graph Export
   - A JSON file that defines the processing graph to execute.
   - It is "source" input: node ids, dependencies, parameters, and __outputs.
   - Examples used by this test suite:
     - BASE_SRC/cat_graph.json (committed test input)
     - root/graph_with_external_ref.json (written by this test as a temp export)

2) Graph Run
   - The on-disk outputs produced by executing a graph.
   - This typically includes:
     - cached result JSON (executor-managed persisted output)
     - run guard / run status state (executor-managed persisted state)
     - artifacts produced by nodes (images, videos, etc.)


ABOUT THESE TWO PYTHON FILES

1) ai_services/nodes/graph_executor.py
   Purpose
   - Implements GraphExecutionNode and GraphExecutionService.
   - Loads an exported graph JSON (when given a path) and executes it.
   - Persists graph run outputs.

   When this file itself is updated
   - Only when a developer edits the Python source.
   - It should never change as a side effect of executing a graph.

   What file(s) it reads
   - Graph export JSON files (read-only) via load_graph() using json.load().

   What file(s) it writes/updates
   - Graph run outputs:
     - cached run results (JSON written under its cache prefixes)
     - run guard / run status records (persisted state under its run prefixes)
     - any node-produced artifacts created during processing

   What file(s) it does NOT write
   - It does not write the graph export JSON back to disk.
     There is no json.dump() / write-back to execution_graph_path in
     graph_executor.py.


2) ai_services/nodes/graph_executor_tests.py (this file)
   Purpose
   - Creates an isolated temp root directory for the run.
   - Writes a temporary exported graph JSON for test setup.
   - Executes the graph through GraphExecutionNode.
   - Performs basic assertions about produced outputs.

   When this file itself is updated
   - Only when a developer edits the Python source.
   - It should never change as a side effect of running the test.

   What file(s) this test writes/updates when you run it
   - Temp workspace under BASE_TMP:
     - fresh_root_and_session() removes and recreates BASE_TMP/<test_name>/
   - Temporary export graph JSON written into that workspace:
     - root/graph_with_external_ref.json
   - Graph run outputs produced by execution:
     - cached run results / run guard state under the configured cache_path
     - any node-produced artifacts under the configured run root


QUICK "WHEN CAN IT CHANGE?" SUMMARY

1) Graph export JSON
   - Changes only if some code explicitly writes it (open(..., "w") + json.dump).
   - In these two files:
     - graph_executor.py: reads export JSON; does not rewrite it.
     - graph_executor_tests.py: writes a temporary export JSON as test setup.

2) Graph run outputs
   - Change whenever you execute a graph and the executor/nodes persist caches,
     run guard state, and/or artifacts.
   - In these two files:
     - graph_executor.py: implements run execution and persistence.
     - graph_executor_tests.py: triggers runs in a temp workspace.
================================================================================
"""


"""
================================================================================
ADDITIONAL NOTES: cache_dir (cache_path) vs graph root vs session root
================================================================================

This section exists because the terms are easy to conflate when multiple
GraphExecutionNode instances execute the same exported graph.


TERMS AS USED IN THIS CODEBASE

1) session root
   - The DataSession root directory.
   - Set when constructing GraphExecutionNode (or any CachingComponent) via the
     "data_session" parameter.
   - Example (service wrapper):
       GraphExecutionService.execute(...)
       -> GraphExecutionNode({ data_session: DataSession({ root: <root> }), ... })
   - Example (this test):
       fresh_root_and_session() creates DataSession({ root: BASE_TMP/<test_name> })

2) cache_dir (called "cache_path" in code)
   - A directory name (relative to the session root) that scopes *executor cache
     state* for a component instance.
   - It is passed at node construction time:
       GraphExecutionNode({ data_session: session, cache_path: "./cache_graph_exec" })
   - Everything stored via save_cache/load_cache lives under:
       <session_root>/<cache_path>/...
     This includes:
     - persisted run results (GraphExecutionNode.save_cache(..., prefix="executions/runs"))
     - run guard / run status records (prefix="executions/runs")
     - FieldSet schema and values ("fields" and "values")

3) graph root (also called run_root here)
   - The directory the ProcessingGraph nodes are configured to use as their
     data_session root during execution.
   - In GraphExecutionNode.execute():
      - If you execute by graph export *path* (graph is a string) and that path
        is outside the current cache_dir, the export is copied into the cache_dir
        and run_root is derived from the copied export path (a sibling directory
        named after the copied export stem).
      - If you execute with an inline graph dict, run_root falls back to the
        cache_dir root (the cache DataSession root).
    - Practical implication:
      - node-produced artifacts tend to land under run_root.


WHY cache_dir MUST BE UNIQUE (TO AVOID RUN COLLISIONS)

GraphExecutionNode persists its run outputs under its cache_dir (cache_path)
using a cache_key. If multiple GraphExecutionNode instances share the same:

1) session root
2) cache_dir (cache_path)
3) cache_key

then they will read/write the same cached result files and the same run-guard
records.

In graph_executor.py, if cache_key is not provided, it defaults to the constant:
    "unversioned_output"

That makes collisions very easy to trigger when multiple nodes share the same
cache_dir.

To avoid collisions when multiple GraphExecutionNode instances execute against
the same exported graph, ensure at least one of the following is unique per
instance:

1) cache_dir (cache_path): recommended as the "namespace" lever.
   - e.g. set cache_path to the graph node id.
2) cache_key: supply a unique cache_key per run.
3) session root: isolate each executor instance under a different DataSession
   root.

================================================================================
"""
