# UI Classes Inventory

This file documents every class currently under `nodejobs/nodejobs/ui`.

Total classes: **31**

## Top-level window and tabs

| Class | Type | File | Purpose |
|---|---|---|---|
| `MainWindow` | `QMainWindow` | `nodejobs/nodejobs/ui/main_window.py` | App shell that wires tabs, connection flow, and refresh controller. |
| `JobsTab` | `QWidget` | `nodejobs/nodejobs/ui/tabs/jobs_tab.py` | Jobs operator tab: refresh, run, details, logs, open directory, stop, running-only filter. |
| `SchedulesTab` | `QWidget` | `nodejobs/nodejobs/ui/tabs/schedules_tab.py` | Schedules operator tab: list, create/edit, enable/disable, details, remove. |
| `ComponentsTab` | `QWidget` | `nodejobs/nodejobs/ui/tabs/components_tab.py` | Component demo/integration tab used for staged UI behavior checks. |

## Reusable UI components

| Class | Type | File | Purpose |
|---|---|---|---|
| `ConnectionBar` | `QWidget` | `nodejobs/nodejobs/ui/components/connection_bar.py` | DB path entry, browse/connect/disconnect actions, connection status text. |
| `ActionToolbar` | `QWidget` | `nodejobs/nodejobs/ui/components/action_toolbar.py` | Configurable action buttons that emit action IDs. |
| `SearchFilterBar` | `QWidget` | `nodejobs/nodejobs/ui/components/search_filter_bar.py` | Search box + clear button, emits query changes. |
| `NotificationCenter` | `QWidget` | `nodejobs/nodejobs/ui/components/notification_center.py` | Inline info/warn/error status line with clear action. |
| `TableViewPanel` | `QWidget` | `nodejobs/nodejobs/ui/components/table_view_panel.py` | Wrapper around table model/proxy/view with preserved selection and scroll state. |
| `DetailPane` | `QWidget` | `nodejobs/nodejobs/ui/components/detail_pane.py` | Read-only key/value display of selected record details. |
| `DateTimePicker` | `QWidget` | `nodejobs/nodejobs/ui/components/date_time_picker.py` | UTC datetime picker with ISO parsing/serialization helpers. |

## Dialogs

| Class | Type | File | Purpose |
|---|---|---|---|
| `EditDialog` | `QDialog` | `nodejobs/nodejobs/ui/dialogs/edit_dialog.py` | Generic field editor for key/value records. |
| `RunJobDialog` | `QDialog` | `nodejobs/nodejobs/ui/dialogs/run_job_dialog.py` | Collects run payload (`job_id`, command, `cwd`, `envs`). |
| `JobDetailsDialog` | `QDialog` | `nodejobs/nodejobs/ui/dialogs/job_details_dialog.py` | Read-only formatted JSON view of a job record. |
| `JobLogsDialog` | `QDialog` | `nodejobs/nodejobs/ui/dialogs/job_logs_dialog.py` | Tabbed stdout/stderr log viewer for a job. |
| `ScheduleDialog` | `QDialog` | `nodejobs/nodejobs/ui/dialogs/schedule_dialog.py` | Create/edit schedule form including interval and environment fields. |
| `ScheduleDetailsDialog` | `QDialog` | `nodejobs/nodejobs/ui/dialogs/schedule_details_dialog.py` | Read-only formatted JSON view of a schedule record. |

## Controllers and policy

| Class | Type | File | Purpose |
|---|---|---|---|
| `AppState` | `BaseData` | `nodejobs/nodejobs/ui/controllers/app_state.py` | Runtime UI state (`db_path`, `connected`, `dev_mode`, `error`) and env bootstrap helpers. |
| `RefreshController` | `QObject` | `nodejobs/nodejobs/ui/controllers/refresh_controller.py` | Topic-based refresh broadcaster with timer-driven ticks and manual triggers. |
| `SelectionController` | `QObject` | `nodejobs/nodejobs/ui/controllers/selection_controller.py` | Single source of truth for selected row state with `selection_changed` signal. |
| `PolicyController` | class | `nodejobs/nodejobs/ui/controllers/policy_controller.py` | Shared UI policy methods for selection guards and status/error message formatting. |

## Models and typed rows

| Class | Type | File | Purpose |
|---|---|---|---|
| `RecordTableModel` | `QAbstractTableModel` | `nodejobs/nodejobs/ui/models/record_table_model.py` | Generic table model over typed row objects. |
| `RecordFilterProxy` | `QSortFilterProxyModel` | `nodejobs/nodejobs/ui/models/record_filter_proxy.py` | Case-insensitive text filtering across row columns. |
| `UiJobRow` | `BaseData` | `nodejobs/nodejobs/ui/types/ui_rows.py` | Typed schema for job table row data and dialog payload fields. |
| `UiScheduleRow` | `BaseData` | `nodejobs/nodejobs/ui/types/ui_rows.py` | Typed schema for schedule table row data and dialog payload fields. |

## Adapters

| Class | Type | File | Purpose |
|---|---|---|---|
| `ApiAdapter` | class | `nodejobs/nodejobs/ui/adapters/api_adapter.py` | Mock/demo adapter used by component demos. |
| `JobsAdapter` | class | `nodejobs/nodejobs/ui/adapters/jobs_adapter.py` | UI-to-backend adapter for jobs list/run/stop/logs/details. |
| `SchedulesAdapter` | class | `nodejobs/nodejobs/ui/adapters/schedules_adapter.py` | UI-to-backend adapter for schedules list/get/set/remove and job ID lookup. |

## Async and error services

| Class | Type | File | Purpose |
|---|---|---|---|
| `_TaskSignals` | `QObject` | `nodejobs/nodejobs/ui/services/task_runner.py` | Internal signal carrier for async task success/error callbacks. |
| `TaskRunner` | class | `nodejobs/nodejobs/ui/services/task_runner.py` | Thread-pool runner for non-blocking tab operations. |
| `ErrorPresenter` | class | `nodejobs/nodejobs/ui/services/error_presenter.py` | Converts exceptions to concise UI-facing messages. |
