

The `JobDB` class is designed to manage and access job-related data stored in a lightweight, schema-driven database. It aims to streamline the process of tracking job statuses, logs, and metadata in a structured format, which can be especially helpful in workflows that involve job scheduling or monitoring. By providing a clear and organized way to record and retrieve job information, `JobDB` helps developers and system operators keep tabs on ongoing tasks without the need for complex database setups. 

### IMPORTANT: To use Jobs, you never need to use the JobDB directly (and should not). The only reason to read this file is to extend or modify the code.

Its intended audience includes developers working on job orchestration, logging, or monitoring systems who prefer a straightforward, schema-based approach to data management. The only reason to use this library, would be if you want an all in one, import only, job management solution.

### Key Concepts & Responsibilities  
- **Managing job records:** `JobDB` offers methods to insert, update, and retrieve information about jobs, such as statuses, logs, and metadata.  
- **Schema enforcement:** It uses data classes like `JobRecord` and `JobFilter` to validate data and maintain consistency across operations.  
- **Log handling:** The class provides functionality to fetch logs associated with specific jobs, often reading from designated log directories.  
- **Filtering and querying:** It supports filtering job records based on various criteria—such as job ID, status, or timestamps—making it easier to find relevant entries.  
- **Abstraction layer:** Acting as a wrapper around raw database operations, `JobDB` simplifies data access by returning structured objects and handling underlying complexities.

### High-Level Usage Examples

*Instantiation:*  
```python
job_db = JobDB(db_path="/path/to/job/storage")
```

*Updating a job status:*  
```python
job_record = {
    JobRecord.self_id: "job123",
    JobRecord.status: JobRecord.Status.c_running,
    # additional fields...
}
job_db.update_status(job_record)
```

*Retrieving logs for a job:*  
```python
stdout, stderr = job_db.job_logs("job123")
```

This overview should give a clear picture of `JobDB`'s role: a practical, schema-oriented tool to keep track of job progress and details in a systematic way. 🚀### `__init__(self, db_path: str)`  

- Initializes a `JobDB` instance by establishing a connection to a SQLite database located at the specified path. This setup enables subsequent database operations related to job records, such as status updates, log retrieval, and filtering.  
- **Parameters**  
• `db_path` (`str`): The directory path where the `jobs.db` SQLite file resides.  

- **Returns**  
• `None`: The constructor sets up the internal database connection for the instance.  

- **Raises**  
• `Exception`: If `db_path` is `None`, indicating an invalid or missing path. 🚧  

- **Examples**  
```python
from nodejobs.jobdb import JobDB
db = JobDB(db_path="/path/to/job/database")
# The database connection is now ready for further operations
```  

---

### `update_status(self, job: dict) -> Any`  

- Upserts (inserts or updates) a job record within the `process_status` table in the database, ensuring the record reflects the latest status. It wraps the input dictionary into a `JobRecord`, validates it, then performs the database operation. 🚀  
- **Parameters**  
• `job` (`dict`): A dictionary containing at least `self_id` and `status`, representing the current state of a job.  

- **Returns**  
• The result of the `execute()` call, typically indicating success, such as a status or row count.  

- **Raises**  
• `Exception`: If the operation encounters an error during database interaction or validation.  

- **Examples**  
```python
db = JobDB(db_path="/path/to/job/database")
job_data = {
    "self_id": "job123",
    "status": "running",
    "last_update": "2023-10-01T12:00:00"
}
result = db.update_status(job=job_data)
# The job status has been upserted; result indicates success
```  

---

### `job_logs(self, self_id: str) -> Tuple[str, str]`  

- Retrieves the standard output and error logs associated with a specific job, identified by `self_id`. It queries the database for log directory and filename, then reads the logs from the filesystem, returning their contents or error messages if logs are unavailable. 📝  
- **Parameters**  
• `self_id` (`str`): The unique identifier of the job whose logs are to be fetched.  

- **Returns**  
• `Tuple[str, str]`: A tuple containing the stdout and stderr logs as strings. If logs cannot be accessed, respective error messages are returned.  

- **Raises**  
• `Exception`: If there is an error during database query or file I/O operations.  

- **Examples**  
```python
db = JobDB(db_path="/path/to/job/database")
stdout, stderr = db.job_logs(self_id="job123")
print("Stdout:", stdout)
print("Stderr:", stderr)
```  

---

### `list_status(self, filter: Optional[dict] = None) -> JobRecordDict`  

- Retrieves a collection of job records matching an optional filtering criterion. It converts the filter dictionary into a `JobFilter` object (if provided), performs a database search, and wraps each result into a `JobRecord`. The output is a structured `JobRecordDict` keyed by job `self_id`. 🔍  
- **Parameters**  
• `filter` (`Optional[dict]`): A dictionary specifying filtering criteria, such as job status or ID. Defaults to `None`, meaning no filter.  

- **Returns**  
• `JobRecordDict`: A dictionary-like object mapping job identifiers to `JobRecord` instances matching the filter.  

- **Raises**  
• `Exception`: If the database query fails or returns an error.  

- **Examples**  
```python
db = JobDB(db_path="/path/to/job/database")
jobs = db.list_status(filter={"status": "running"})
for job_id, record in jobs.items():
    print(f"Job {job_id} is currently {record.status}")
```
