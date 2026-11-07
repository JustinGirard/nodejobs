# BaseData Cross-Language Tests

Directory layout:

- `python/test_base_data.py` - Python BaseData semantic tests.
- `csharp/Nodejobs.BaseData.Tests/` - C# BaseData semantic test project.
- `typescript/test_base_data.ts` - TypeScript BaseData semantic tests.
- `runner/test_runner.py` - aggregate runner for Python + C# with unified reporting.

Run aggregate suite:

- `python -m tests.base_data.runner.test_runner`
- `python -m tests.base_data.runner.test_runner -v`
- `python -m tests.base_data.runner.test_runner --language python`
- `python -m tests.base_data.runner.test_runner --language csharp`
- `python -m tests.base_data.runner.test_runner --language typescript`

Script entrypoint:

- `nodejobs-basedata-test`

Artifacts:

- `test-results/base-data-tests/python.log`
- `test-results/base-data-tests/csharp.log`
- `test-results/base-data-tests/typescript.log`
- `test-results/pytest-base-data.xml`
- `tests/base_data/csharp/Nodejobs.BaseData.Tests/test-results/csharp-base-data.trx`
