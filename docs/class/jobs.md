The `Jobs` class is designed to streamline the management of background tasks, providing a structured way to start, monitor, and control multiple processes. It addresses common challenges in handling long-running or asynchronous operations, such as tracking status, retrieving logs, and stopping jobs reliably. This makes it suitable for environments where process orchestration and logging are important—think batch workflows, scheduled tasks, or long-duration computations. The intended audience includes developers and system administrators who need a flexible yet organized approach to process management.

At a high level, `Jobs` adopts a schema-driven design pattern. It leverages data classes like `JobRecord` and `JobFilter` to enforce data consistency and integrity, while interacting with a lightweight database (`JobDB`) for persistent storage of job states. Process management is handled via subprocess control, allowing for starting, monitoring, and stopping jobs seamlessly. This separation of concerns—between data handling, process control, and storage—makes the system adaptable and easier to extend.

### Key Concepts & Responsibilities

- **Job Lifecycle Management:** Initiates new jobs with specified commands, tracks their process IDs, and updates their status throughout their lifecycle.
- **Monitoring:** Checks the current status of active or completed jobs, reflecting real-time process states and database records.
- **Logging:** Retrieves stdout and stderr logs associated with individual jobs for review or debugging purposes.
- **Control Operations:** Provides methods to stop jobs gracefully or forcefully, updating the status accordingly.
- **Listing and Filtering:** Offers the ability to list all jobs, with optional filters such as running, failed, or completed states.

**Design Patterns & Ideas:**
- Schema enforcement through data classes ensures data consistency.
- Use of a lightweight NoSQL database (`JobDB`) facilitates flexible storage and retrieval.
- Separation of process control (`Processes`) from data management (`JobRecord`, `JobFilter`) promotes clear interfaces.
- Methods are designed to be predictable and easy to interact with, supporting straightforward process orchestration.

### High-Level Usage Examples

**Instantiation:**

```python
# Create a Jobs manager with a specified database path
jobs_manager = Jobs(db_path="/path/to/job_db")
```

**Starting a Job:**

```python
# Launch a new command with a unique job ID
job_record = jobs_manager.run(command="python script.py", job_id="job_001")
```

**Monitoring and Control:**

```python
# Check the status of a specific job
status = jobs_manager.get_status(job_id="job_001")

# Fetch logs for the job
stdout, stderr = jobs_manager.job_logs(job_id="job_001")

# Stop a running job
jobs_manager.stop(job_id="job_001")
```

This setup offers a straightforward way to manage background tasks systematically, with clear separation between process control and data handling, making it a practical choice for orchestrating multiple processes reliably.### `__init__(self, db_path: str = None)`  

- Initializes a new instance of the `Jobs` class, setting up internal configurations to manage and track jobs. This constructor prepares the environment for subsequent job operations such as run, stop, and status queries.  

- **Parameters**  
• `db_path` (`str`, optional): Path to the directory where job data and logs will be stored. Defaults to `None`, which may trigger default setup routines.  

- **Returns**  
• None: This method constructs the object without returning a value.  

- **Raises**  
• `Exception`: If the provided `db_path` is invalid or if directory setup encounters issues.  

- **Examples**  
```python
jobs_manager = Jobs(db_path='/path/to/job/data')
```  

---

### `run(self, command: str, job_id: str, cwd: str = None) -> JobRecord` 🚀  

- Starts a new job by executing a specified command in a subprocess, associates it with a unique job ID, and tracks its execution status. It updates internal records and logs output, returning a `JobRecord` that reflects the current state of the job.  

- **Parameters**  
• `command` (`str`): The shell command or executable to run as a job.  
• `job_id` (`str`): The unique identifier for tracking this job.  
• `cwd` (`str`, optional): The working directory in which to execute the command; defaults to the current directory if not provided.  

- **Returns**  
• `JobRecord`: An object encapsulating the job’s current status, process ID, and associated metadata.  

- **Raises**  
• `Exception`: If the command fails to start or if there are issues with the specified working directory.  

- **Examples**  
```python
job_record = jobs_manager.run(command='echo "Hello, World!"', job_id='job001')
print(f"Started job with ID: {job_record.self_id} and status: {job_record.status}")
```  

---

### `stop(self, job_id: str) -> JobRecord` 🛑  

- Attempts to terminate a running job associated with the provided job ID. It updates the internal record to reflect the final state and ensures the subprocess is properly terminated. The method returns an updated `JobRecord` indicating whether the stop was successful.  

- **Parameters**  
• `job_id` (`str`): The identifier of the job to be stopped.  

- **Returns**  
• `JobRecord`: The updated record with the job’s final status, including success or failure of termination.  

- **Raises**  
• `Exception`: If the job cannot be found or if termination encounters an issue.  

- **Examples**  
```python
final_record = jobs_manager.stop('job001')
print(f"Job {final_record.self_id} stopped with status: {final_record.status}")
```  

---

### `job_logs(self, job_id: str) -> Tuple[str, str]`  

- Retrieves the standard output and standard error logs for a specific job by reading from stored log files. It returns a tuple containing the stdout and stderr logs as strings, or error messages if logs are unavailable.  

- **Parameters**  
• `job_id` (`str`): The identifier of the job whose logs are to be fetched.  

- **Returns**  
• `Tuple[str, str]`: The stdout and stderr logs as strings.  

- **Raises**  
• None explicitly; errors in log retrieval are communicated via error messages within the logs.  

- **Examples**  
```python
stdout, stderr = jobs_manager.job_logs('job001')
print(f"Stdout: {stdout}\nStderr: {stderr}")
```  

---

### `list_status(self, filter: JobFilter = None) -> JobRecordDict`  

- Retrieves a dictionary of all current job records, optionally filtered based on criteria such as status, directory, or last update time. This provides a comprehensive overview of all jobs managed by the system.  

- **Parameters**  
• `filter` (`JobFilter`, optional): Criteria for filtering jobs; defaults to `None`, returning all jobs.  

- **Returns**  
• `JobRecordDict`: A dictionary keyed by job ID with `JobRecord` objects as values, matching the filter criteria.  

- **Raises**  
• `Exception`: If the underlying database query fails.  

- **Examples**  
```python
all_jobs = jobs_manager.list_status()
for job_id, record in all_jobs.items():
    print(f"Job ID: {job_id}, Status: {record.status}")
```  

---

### `get_status(self, job_id: str) -> JobRecord`  

- Fetches the current status record of a specific job identified by its job ID. This method provides real-time information about the job’s state, useful for monitoring or decision-making.  

- **Parameters**  
• `job_id` (`str`): The unique identifier of the job to query.  

- **Returns**  
• `JobRecord`: The current record of the specified job, including its status and metadata.  

- **Raises**  
• `KeyError` or custom exception if the job ID does not exist.  

- **Examples**  
```python
status_record = jobs_manager.get_status('job001')
print(f"Job {status_record.self_id} is currently {status_record.status}")
```

---

### `bind(self, job_id: str, include: Tuple[str, ...] = ("stdout","stderr"), from_beginning: bool = False, poll_interval: float = 0.25, heartbeat_interval: float = 5.0, last_event_id: Optional[int] = None) -> Iterator[StreamEvent]`

- Streams live job output by tailing stdout/stderr log files and yielding `StreamEvent` instances:
  - `type`: `"stdout" | "stderr" | "status" | "heartbeat"`
  - `text`: chunk content for stdout/stderr (if present)
  - `status`: status string for `status` events
  - `seq`: monotonically increasing event id (int)
  - `ts`: ISO timestamp

- Example:
```python
for ev in jobs.bind("job123", include=("stdout","stderr"), from_beginning=True):
    if ev.type == "stdout":
        handle_stdout(ev.text)
    elif ev.type == "status" and ev.status in ("finished","stopped","failed"):
        break
```

- Errors:
  - `ValueError` if `job_id` is unknown or log paths are not recorded yet.
  - Ends naturally after a terminal status and no new output.

---

### `sse(self, job_id: str, include: Tuple[str, ...] = ("stdout","stderr"), from_beginning: bool = False, poll_interval: float = 0.25, heartbeat_interval: float = 5.0, last_event_id: Optional[str] = None) -> Iterator[str]`

- Wraps `bind()` and yields properly framed SSE events:
  - `id: <seq>`
  - `event: <type>`
  - `data: <json payload>`
  - blank line delimiter between events

- Example (FastAPI):
```python
from starlette.responses import StreamingResponse
@app.get("/jobs/{job_id}/stream")
def stream(job_id: str):
    return StreamingResponse(
        jobs.sse(job_id, include=("stdout","stderr")),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache", "X-Accel-Buffering":"no"},
    )
```

- Resume: Provide HTTP `Last-Event-ID` to continue from the next `seq`.
- Errors: Propagates `ValueError` from `bind()` for unknown `job_id`.

---

### `@staticmethod subscribe_sse(url: str, session=None, **requests_kwargs) -> Iterable[Event]`

- Python helper for consuming SSE endpoints; requires `sseclient-py`.
- Usage:
```python
from nodejobs import Jobs
try:
    for ev in Jobs.subscribe_sse("http://127.0.0.1:8000/jobs/job123/stream"):
        print(ev.event, ev.id, ev.data)
except ImportError as e:
    # [stream] events stream error: sseclient-py is required for event streaming
    print(e)
```
