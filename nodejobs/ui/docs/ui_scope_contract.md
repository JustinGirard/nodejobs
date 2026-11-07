# Nodejobs PyQt UI Scope Contract

## Ownership Boundaries
- UI code ownership: `nodejobs/nodejobs/ui/`
- Backend API ownership: `nodejobs/nodejobs/jobs.py`
- Persistence ownership: `nodejobs/nodejobs/jobdb.py`

## API Inventory (Verified)
- Jobs listing/pump trigger: `Jobs.list_status(filter=None)`
- Job status by id/pump trigger: `Jobs.get_status(job_id)`
- Job stop/terminate: `Jobs.stop(job_id, wait_time=1)`
- Job logs: `Jobs.job_logs(job_id)`
- Schedules list: `Jobs.list_schedules(filter=None)`
- Schedule create/update: `Jobs.set_schedule(schedule)`
- Schedule remove: `Jobs.remove_schedule(schedule_id)`
- Scheduler semantics: `list_status()` and `get_status()` execute due schedules (pump behavior).

## UI Action Mapping

### Jobs Tab
| UI Action | Backend Method | Inputs | Output | Mapping Status |
| --- | --- | --- | --- | --- |
| Refresh jobs list | `Jobs.list_status(filter=None)` | optional `filter` | `JobRecordDict` keyed by `self_id` | mapped |
| Filter jobs by status/id | `Jobs.list_status(filter={...})` | `JobFilter`-compatible dict | filtered `JobRecordDict` | mapped |
| View selected job details | `Jobs.get_status(job_id)` | `job_id` | `JobRecord` | mapped |
| Open selected job logs | `Jobs.job_logs(job_id)` | `job_id` | `(stdout, stderr)` strings | mapped |
| Stop selected job | `Jobs.stop(job_id, wait_time=1)` | `job_id`, optional wait | updated `JobRecord`/None | mapped |
| Edit selected job metadata | none | n/a | n/a | gap: no direct mutable job-record API |

### Schedules Tab
| UI Action | Backend Method | Inputs | Output | Mapping Status |
| --- | --- | --- | --- | --- |
| Refresh schedule list | `Jobs.list_schedules(filter=None)` | optional `filter` | `ScheduleRecordDict` keyed by `schedule_id` | mapped |
| Filter schedules | `Jobs.list_schedules(filter={...})` | `ScheduleFilter`-compatible dict | filtered `ScheduleRecordDict` | mapped |
| View selected schedule details | `Jobs.list_schedules(filter={"schedule_id": ...})` | `schedule_id` | single-entry dict view | mapped |
| Create schedule | `Jobs.set_schedule(schedule)` | validated schedule payload | `ScheduleRecord` | mapped |
| Edit schedule | `Jobs.set_schedule(schedule)` | same `schedule_id` + updated fields | `ScheduleRecord` | mapped |
| Remove schedule | `Jobs.remove_schedule(schedule_id)` | `schedule_id` | remove response | mapped |

### Cross-Tab/Policy
| UI Concern | Backend Method | Notes |
| --- | --- | --- |
| Auto-refresh cadence | `list_status` / `get_status` | refresh cadence affects scheduler pump execution side effects |
| Adapter construction | `Jobs(db_path=...)` | require explicit `db_path`; do not rely on fallback home temp path |

## Gap Registry
- GAP-001: No first-class API to mutate arbitrary non-runtime fields of an existing `JobRecord` from UI; current job UX should scope edits to stop/log/view unless backend API is added.
- GAP-002: Schedule detail-by-id has no dedicated `get_schedule(schedule_id)` helper; use `list_schedules(filter={"schedule_id": ...})` until backend adds one.

## Guard Rule
- UI actions without mapped backend APIs remain blocked from implementation until a backend mapping or explicit adapter fallback is approved.
