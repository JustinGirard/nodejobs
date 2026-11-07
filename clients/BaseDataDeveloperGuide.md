# BaseData Developer Router Guide

Purpose: keep BaseData behavior synchronized across Python, TypeScript, and C# with one canonical contract source.

Root guide path (always reference this in related docs): `nodejobs/clients/BaseDataDeveloperGuide.md`

## Governance Summary (AGENTS.md + Standards)

- This guide is governed by `docs/AGENTS.md` and the language-specific BaseData standards guides.
- Root guide for all BaseData governance blurbs: `nodejobs/clients/BaseDataDeveloperGuide.md`.
- `AGENTS.md` defines the operating contract: route first, smallest correct change, strict scope control, no adjacent work, and no optimistic null/empty fills for required data.
- **Strict adherence is critical** for all code entering the codebase, including upgrades and refactors.
- Going forward, **QA review and acceptance criteria are primarily dictated by standards compliance** (contract ownership, parity discipline, and failure behavior) in addition to behavior correctness.

## Canonical Rule (Non-Negotiable)

- **Python is canonical** for both:
  - runtime semantics, and
  - developer ergonomic surface.
- When language differences exist, TypeScript and C# must match Python behavior as closely as possible.
- If there is a discrepancy, resolve in this order:
  1) Verify Python behavior in `nodejobs/nodejobs/dependencies/BaseData.py`.
  2) Update TypeScript and C# to match Python.
  3) Update tests in all three language suites.
  4) Update standards docs if policy wording changed.

## References Router (Read First)

- **Global operating contract**
  - `docs/AGENTS.md`
    Router and behavior contract for request classification, scope boundaries, proposal/approval flow, and minimal-change rules. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.

- **Proposal/approval response contract**
  - `product_manager/agent_skills/one_shot/code_proposal.md`
    Required proposal structure and editing-phase constraints used when producing code proposals. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.

- **Canonical implementation (source of truth)**
  - `nodejobs/nodejobs/dependencies/BaseData.py`
    Python semantic authority and canonical developer surface. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.

- **Parity implementations**
  - `nodejobs/clients/typescript/BaseData.ts`
    TypeScript parity implementation (`BaseData.define`, schema path, typed keys/properties). Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.
  - `nodejobs/clients/javascript/src/BaseData.js`
    JavaScript parity implementation for browser/runtime consumers. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.
  - `nodejobs/clients/csharp/Nodejobs.Client/BaseData.cs`
    C# parity implementation (`RequiredKeys/OptionalKeys/Defaults`, IDictionary behavior). Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.

- **Standards (policy + usage contract)**
  - `docs/conding_standards_py.md`
    Canonical usage policy and behavior ownership rules for Python. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.
  - `docs/coding_standards_ts.md`
    TS contract/behavior separation + `BaseData.define` policy. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.
  - `docs/conding_standards_csharp.md`
    C# contract/behavior separation + defaults ownership policy. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.

- **Cross-language semantic tests (must stay current)**
  - `nodejobs/tests/base_data/python/test_base_data.py`
    Canonical semantic assertions. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.
  - `nodejobs/tests/base_data/typescript/test_base_data.ts`
    TS parity assertions. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.
  - `nodejobs/tests/base_data/csharp/Nodejobs.BaseData.Tests/BaseDataSemanticTests.cs`
    C# parity assertions. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.

- **Test orchestration**
  - `nodejobs/tests/base_data/README.md`
    Test suite map + commands. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.
  - `nodejobs/tests/base_data/runner/test_runner.py`
    Unified cross-language test runner/reporting entrypoint. Root guide: `nodejobs/clients/BaseDataDeveloperGuide.md`.

## Standards-Driven Acceptance

- Standards conformance is a first-class acceptance gate, not optional guidance.
- Root guide reference required in standards/governance blurbs: `nodejobs/clients/BaseDataDeveloperGuide.md`.
- Any BaseData-related change can be rejected if it violates:
  - `docs/AGENTS.md`
  - `docs/conding_standards_py.md`
  - `docs/coding_standards_ts.md`
  - `docs/conding_standards_csharp.md`
- Behavior correctness alone is not sufficient when standards are violated.

## Synchronization Contract

Any BaseData behavior change is incomplete unless all are updated together:

1. Python `BaseData.py` (canonical behavior),
2. TypeScript `BaseData.ts` parity,
3. C# `BaseData.cs` parity,
4. Python/TS/C# BaseData tests,
5. Relevant standards docs.

## Incremental Conformance Policy

- Default policy for touched code: incrementally move code toward this standard set.
- Anchor policy source for this rule: `nodejobs/clients/BaseDataDeveloperGuide.md`.
- Surgical refactors are welcome when behavior is unchanged and scope remains local.
- Net line-count reduction is preferred when it improves clarity and preserves semantics.
- Do not broaden scope; apply conformance improvements only where directly touched by the change.

## Required Parity Surface

Keep these synchronized across implementations:

- Construction boundary and error behavior (`basedata_requires_object`, required-key/type failures).
- Required/optional/default semantics (including omitted default timing behavior).
- Hook lifecycle (`pre_process`, field validation, every-validation).
- Nested BaseData conversion and recursive validation behavior.
- Undeclared key preservation.
- Environment mapping behavior and failure contract.
- Mutation-time validation behavior.
- Nullish-default compatibility: if a field explicitly declares a nullish default, nullish assignments are type-compatible and retain their assigned value (TypeScript treats `null`/`undefined` as interchangeable for this check; Python/C# apply native null only).
- Wildcard (`*`) semantics where supported by implementation.
- Static diagnostics contract parity: `BaseData.DiagnosticPrint(title, index, start_id, end_id, description_of_location, interesting_conditionals, repeat_delay)` and `BaseData.DiagnosticDump()` are index-keyed, seconds-gated, bounded-by-index stores (`diagnostics`, `starts`, `ends`) with no-op suppression writes.

## PR / Diff Checklist (BaseData Changes)

- [ ] Python behavior updated (or confirmed unchanged).
- [ ] TS and C# parity reviewed and updated.
- [ ] Python/TS/C# BaseData semantic tests updated.
- [ ] Cross-language runner still passes.
- [ ] Standards docs updated if policy changed.
- [ ] Notes explicitly state: "Python behavior is canonical."
- [ ] Touched code moved incrementally toward standards compliance (no behavior drift).
