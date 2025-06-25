## Overview

The `nodejobs` repository provides a schema-driven framework for managing and orchestrating system jobs and processes with persistent storage and structured data access. Core components include classes for schema validation, job record management, process control, and database interaction. If you are considering a deeper dive on nodejobs, and maybe want to extend it or use it as part of your application, you will want to read this document. If you want to USE node jobs, the README is good place to start.


<img width="690" alt="image" src="https://github.com/user-attachments/assets/e409482a-cf50-44e6-9e3f-292ab7a0034d" />


### Main Modules and Their Responsibilities

1. **Persistent Storage Module (`jobdb.py`):**  
   This module implements a schema-aware, persistent storage layer, backed by a mix of SQLite and source files. It manages CRUD operations for job records, logs, and statuses. Classes like `JobDB` encapsulate database interactions, wrapping raw data into schema-validated objects and ensuring data consistency across the system.

2. **Job Management Interface (`jobs.py`):**  
   Serving as the main API layer, this module offers high-level methods for job lifecycle management:
   - `run(command, job_id)` to spawn new jobs.
   - `stop(job_id)` to terminate running processes.
   - `get_status(job_id)` and `list_status()` for monitoring.
   - `job_logs(job_id)` to retrieve process logs.
   
   It coordinates with the database module to update job statuses and track metadata, and leverages data schema classes (`JobRecord`, `JobFilter`) for validation. This module acts as the bridge between process control and data persistence.

4. **Process Control Module (`processes.py`):**  
   This module manages actual system process interactions, utilizing subprocesses and libraries like `psutil`. It handles launching jobs, monitoring their process IDs (`pid`), checking their status, and terminating processes as needed. It communicates with the database module to update job statuses based on process states, ensuring synchronization between process lifecycle and stored data.

5. **Utilities and Patterns:**  
   Throughout the codebase, there is a consistent application of schema validation patterns:
   - Classes define their schema through `get_keys()`.
   - Data is accessed via `instance.field`.
   - Filters (`JobFilter`) encapsulate query criteria, again employing schema-defined keys.
   
   These patterns promote robustness, facilitate debugging, and enable flexible querying.


### Key Classes and Data Structures

- **`JobRecord`**  
  A schema-driven class representing individual job entries. It defines fields like `self_id`, `status`, `last_update`, `last_pid`, and log-related fields. It also includes nested status constants (e.g., `JobRecord.Status.c_running`, `JobRecord.Status.c_finished`). It ensures each job's data adheres to the schema, supports status updates, and log retrieval.

- **`JobFilter`**  
  Defines filtering criteria for querying job records. It uses schema constants such as `self_id`, `status` It enables schema-validated filtering when listing jobs from the database.

- **`JobRecordDict`**  
  A container class that holds multiple `JobRecord` instances, keyed by job `self_id`.

- **`JobDB`**  
  Handles persistent storage of job records in a SQLite database (`jobs.db`). It provides methods for updating statuses (`update_status()`), retrieving logs (`job_logs()`), and listing jobs with filtering support (`list_status()`). 

- **`Jobs`**  
  The main interface for managing jobs. It supports starting (`run()`), stopping (`stop()`), listing (`list_status()`), and querying job statuses (`get_status()`). It interacts with `JobDB` for persistence and with `Processes` for subprocess management.

- **`Processes`**  
  Manages subprocesses associated with jobs, including launching (`run()`), finding (`find()`), and terminating (`stop()`) processes. It maintains an internal registry of active processes, periodically reaping completed ones via background threads. It integrates with system process inspection tools like `psutil`.

### Coding Standards and Developer Notes

- **Documentation and Testing**  
  The repository includes comprehensive high-fidelity code snippets and test cases demonstrating usage patterns, such as starting jobs, polling for status changes, capturing logs, and handling errors gracefully. See the examples directory.

- **File and Log Management**  
  Log files are stored according to directory and filename schema, with existing logs cleaned before process launch to ensure clarity. If you notice that a job crashes, it is direct to just open the logs:
```python
# If starting with a debug directory:
jobs = Jobs(db_path='./debug_jobs')
```
  <img width="275" alt="image" src="https://github.com/user-attachments/assets/7ca1d550-6da7-4082-b06b-a0ebcb56d36a" />

- **Process Management**  
  Subprocesses are launched with `preexec_fn=os.setsid` for process group control, enabling clean termination and signal handling. Processes are tracked via process IDs stored in job records.

This architecture emphasizes schema-driven data integrity, reliable process control, and persistent job tracking, facilitating scalable and maintainable job orchestration within the system.

## Intended External Use

The `nodejobs` repository provides a set of core classes designed to facilitate external developer workflows related to job management, process control, and data schema validation. Below are the primary classes intended for external consumption, along with example snippets demonstrating typical, minimal usage scenarios.

### `Jobs`

The `Jobs` class offers a high-level interface for creating, controlling, and monitoring jobs.

```python
from nodejobs.jobs import Jobs

# Initialize Jobs with a database path
jobs = Jobs(db_path="/path/to/jobdb")

# Start a new job
result = jobs.run(command="echo Hello, World!", job_id="job123")
print(result)  # {'self_id': 'job123', 'status': 'starting', 'last_pid': 12345}

# List current jobs with their statuses
status_dict = jobs.list_status()
print(status_dict)

# Stop a job
jobs.stop(job_id="job123")
```

---

### `JobRecord`

`JobRecord` encapsulates individual job data with schema validation and provides constants for field access.

```python
from nodejobs.jobdb import JobRecord

# Wrap raw job data into a schema-validated record
raw_job = JobRecord({
    JobRecord.self_id: "job123",
    JobRecord.status: "running",
    JobRecord.last_pid: 12345,
})
rec = JobRecord(raw_job)

# Access fields using constants
print(rec.self_id)  # "job123"
print(rec.status)   # "running"
```

---

### `JobFilter`

`JobFilter` is used for filtering job listings based on specified criteria.

```python
from nodejobs.jobdb import JobFilter

# Create a filter for jobs with status "running"
filter = JobFilter({JobFilter.status: "running"})

# Use filter in listing jobs (assuming a method like jobs.list_status(filter))
# jobs.list_status(filter=filter)
```


The `nodejobs` repository is designed with a clear separation between schema-driven data classes and behavior/control classes, promoting modularity, maintainability, and data integrity.

### Data Classes (Schema & Data Validation)
- **Purpose:**  
  Encapsulate job-related data with enforced schema and type validation to ensure consistency across the system.
- **Base Class:** `BaseData`  
  - Provides schema enforcement through class constants (`f_<field>`) for field keys.  
  - Implements `get_keys()` to define required and optional fields, including nested `BaseData` objects.  
  - Wraps raw dictionaries into validated, structured instances that facilitate safe data access.
- **Main Data Classes:**  
  - **`JobRecord`**: Represents a single job's state, metadata, logs, timestamps, and progress.  
  - **`JobFilter`**: Defines filtering criteria for querying or listing jobs.  
  - **`JobRecordDict`**: Manages collections of `JobRecord` instances, providing dictionary-like access and bulk operations.
- **Access Pattern:**  
  Data is accessed via `instance[instance.f_<field>]`, ensuring consistent key usage and validation.

### Behavior & Control Classes
- **`Jobs`**  
  - Acts as the high-level API for managing jobs: starting (`run`), stopping (`stop`), listing (`list_status`), and retrieving status (`get_status`).  
  - Utilizes `JobRecord` and `JobFilter` for data validation and filtering logic.  
  - Interacts with the persistent storage layer (`JobDB`) to save and retrieve job states and logs.
- **`JobDB`**  
  - Handles persistent storage of job records, typically using an SQLite database.  
  - Provides methods such as `update_status()`, `list_status()`, and `job_logs()`.  
  - Wraps database records into `JobRecord` instances, maintaining schema validation.
- **`Processes`**  
  - Manages system subprocesses associated with jobs.  
  - Launches new processes (`run`), locates active processes (`find`), and terminates them (`stop`).  
  - Uses `job_id` to link system processes with their metadata in `JobDB`.  
  - Maintains an internal registry of active subprocesses, often running background cleanup threads.

### Summary
- **Data Classes (`JobRecord`, `JobFilter`)** enforce schema validation, data integrity, and consistent data access.  
- **Behavior Classes (`Jobs`, `JobDB`, `Processes`)** implement the core logic for job lifecycle management, process control, and persistence.  
- **Interactions** are mediated through data classes for validation and consistency, allowing control classes to orchestrate job execution, monitoring, and cleanup seamlessly.

This architecture promotes a modular, schema-validated core system with a clear separation between data schemas and operational behaviors, facilitating robustness and ease of maintenance.# Class Summaries

## JobRecord
Represents an individual job's data, including status, timestamps, logs, and metadata, with schema validation and access constants.
- **Key Methods and Properties:**
  - `__init__(self, data: dict)`: Wraps raw job data into a validated schema object.
  - `get_keys() -> (dict, dict)`: Defines required and optional fields with types, including nested `Status`.
  - Access fields via constants like `JobRecord.self_id`, `JobRecord.status`, `JobRecord.last_pid`.
  - Nested class `Status` with constants such as `JobRecord.Status.c_running`, `JobRecord.Status.c_finished`, etc.
- **Implementation Notes / Usage Hints:**
  - Use `JobRecord.<field>` constants for schema-safe key access.
  - Use `(instance:JobRecord).<field>` for values.
  - Suitable for maintaining consistent, schema-validated job state data within the system.

---

## JobFilter
Defines filtering criteria for querying job records, ensuring schema consistency and type correctness.
- **Key Methods and Properties:**
  - `__init__(self, data: dict)`: Creates a filter object with specified criteria.
  - `get_keys() -> (dict, dict)`: Returns schema mappings for filter fields like `self_id`, `status`, etc.
- **Implementation Notes / Usage Hints:**
  - Use class constants such as `self_id`, `status` for setting filter criteria.
  - Facilitates type-checked filtering when querying job data via `list_status()` or similar methods.

---

## JobRecordDict
A collection class managing multiple `JobRecord` instances, keyed by job ID, with schema validation.
- **Implementation Notes / Usage Hints:**
  - Designed for bulk management of job records, ensuring they conform to the schema.
  - Use the class constants from `JobRecord` to access fields consistently.
  - Provides schema validation and structured access for collections of job data.

---

## JobDB
Provides persistent storage and retrieval of job records, logs, and statuses, typically backed by SQLite.
- **Key Methods and Properties:**
  - `update_status(self, job: JobRecord)`: Updates or inserts a job record in the database.
  - `list_status(self, filter: JobFilter = None) -> list[JobRecord]`: Retrieves filtered job records.
  - `job_logs(self, job_id: str) -> Tuple[str, str]`: Retrieves stdout and stderr logs for a job.
- **Implementation Notes / Usage Hints:**
  - Wraps raw database data into `JobRecord` instances for schema enforcement.

---

## Jobs
Acts as the main interface for managing job lifecycle operations such as running, stopping, listing, and retrieving status.
- **Key Methods and Properties:**
  - `__init__(self, db_path: str)`: Initializes the job manager with database persistence.
  - `run(self, command: str, job_id: str) -> dict`: Starts a new job process and returns a status dict.
  - `stop(self, job_id: str) -> dict or None`: Stops a running job by ID.
  - `list_status(self, filter: JobFilter = None) -> JobRecordDict`: Lists jobs with optional filtering.
  - `get_status(self, job_id: str) -> JobRecord`: Retrieves current status for a specific job.

- **Implementation Notes / Usage Hints:**
  - Uses `JobRecord` and `JobFilter` schemas for data validation and filtering criteria.
  - Encapsulates job control logic, process management, and status updates.
  - Recommended to access job data fields via the class constants for consistency.

---

## Processes
Manages subprocesses associated with jobs, enabling launching, monitoring, finding, and stopping processes.
- **Key Methods and Properties:**
  - `__init__(self, job_db: JobDB)`: Initializes with a reference to `JobDB`.
  - `run(self, command: list, job_id: str, envs: dict, cwd: str, logdir: str, logfile: str) -> process`: Launches a process linked to a job.
  - `find(self, job_id: str) -> process or None`: Locates a process by job ID by matching process PIDs.
  - `stop(self, job_id: str) -> None`: Terminates the process associated with a job ID.
  - `list(self) -> list`: Lists all active processes linked to jobs with added `job_id` attribute.
- **Implementation Notes / Usage Hints:**
  - Uses `psutil` to inspect running processes and match by PID for process management.
  - Maintains an internal registry of active subprocesses mapped by `job_id`.
  - Facilitates process lifecycle control in conjunction with job status updates.
