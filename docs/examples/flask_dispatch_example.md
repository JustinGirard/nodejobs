# Title: Flask-Based Job Dispatcher Using nodejobs

## Short Demonstration:
This example demonstrates the design of a Flask-based HTTP dispatcher that manages asynchronous job submission and status tracking utilizing the `nodejobs` module. The system receives POST requests at `/submit_task`, assigns each request a unique job identifier, enqueues the task using `nodejobs.Job`, and provides a `/status/<job_id>` endpoint for clients to poll for job completion, pending status, or failure. The implementation highlights the proper use of `nodejobs.Job`, `JobRecord`, and associated methods for process control, status monitoring, and result retrieval, ensuring adherence to best practices for asynchronous task management.

## Solution Implementation

```python
# Import necessary modules
from flask import Flask, request, jsonify
import uuid
import time
import threading
import nodejobs

app = Flask(__name__)

# Registry to hold Job instances keyed by job_id
jobs_registry = {}

def enqueue_job(job_id: str, payload: dict):
    """
    Enqueue a job using nodejobs.Job.
    This function initializes a Job instance, runs it with the specified command,
    and stores it in the global registry.
    """
    command = payload.get("command")
    cwd = payload.get("cwd")
    # Initialize nodejobs.Job with the specified job_id
    job = nodejobs.Job(job_id=job_id)
    # Run the job with command and cwd; run() returns a JobRecord
    job_record = job.run(command=command, cwd=cwd)
    # Store the Job instance for status polling
    jobs_registry[job_id] = job
    print(f"Enqueued job {job_id} with command: {command}")

@app.route('/submit_task', methods=['POST'])
def submit_task():
    """
    Handles incoming POST requests to submit a new task.
    Generates a unique job_id, enqueues the job, and returns status endpoint.
    """
    # Parse JSON payload
    payload = request.get_json()
    # Generate unique job_id using UUID
    job_id = f"task_{uuid.uuid4()}"
    # Enqueue job with the provided payload
    enqueue_job(job_id=job_id, payload=payload)
    # Construct the status endpoint URL
    status_url = f"/status/{job_id}"
    # Return 202 Accepted with job_id and status endpoint
    response = jsonify({"job_id": job_id, "status_endpoint": status_url})
    response.status_code = 202
    print(f"Received task request, assigned job_id={job_id}")
    return response

@app.route('/status/<string:job_id>', methods=['GET'])
def check_status(job_id):
    """
    Polls the status of the job with the given job_id.
    Responds with pending, complete, or failed status along with result or error message.
    """
    # Retrieve the Job instance from the registry
    job = jobs_registry.get(job_id)
    if job is None:
        # No such job found
        print(f"Status check: job {job_id} not found")
        return jsonify({"error": "Job not found"}), 404

    # Obtain JobRecord using get_status()
    job_record = job.get_status()
    status = job_record.status
    print(f"Status check for {job_id}: status={status}")

    # Check if job is still starting or running
    if status in (nodejobs.JobRecord.Status.c_starting, nodejobs.JobRecord.Status.c_running):
        # Job is still processing; respond with pending
        print(f"Returning pending for {job_id}")
        return jsonify({"job_id": job_id, "status": "pending"}), 202

    # Check if job has finished successfully
    elif status == nodejobs.JobRecord.Status.c_finished:
        # Retrieve output logs
        output_logs = job.job_logs()
        # Assume stdout contains JSON string with result
        try:
            result = json.loads(output_logs[0])
        except json.JSONDecodeError:
            # Fallback if output is not JSON
            result = output_logs[0]
        print(f"Job {job_id} completed with result: {result}")
        return jsonify({"job_id": job_id, "status": "complete", "result": result}), 200

    # Check if job failed
    elif status in (nodejobs.JobRecord.Status.c_failed, nodejobs.JobRecord.Status.c_failed_start):
        # Retrieve error message or logs
        error_message = "Job failed during execution"
        print(f"Job {job_id} failed with error: {error_message}")
        return jsonify({"job_id": job_id, "status": "failed", "error": error_message}), 200

    # Handle unknown states
    else:
        print(f"Job {job_id} in unknown state: {status}")
        return jsonify({"job_id": job_id, "status": "unknown"}), 200
```

## Overall Approach and Key Concepts

This implementation employs the `nodejobs` module to manage asynchronous job processing within a Flask application. The key steps include:

- **Job Enqueuing:** When a POST request arrives at `/submit_task`, a unique job identifier (`job_id`) is generated using `uuid.uuid4()`. A `nodejobs.Job` instance is created with this `job_id`, and its `run()` method is invoked with the command and working directory extracted from the request payload. The `run()` method returns a `JobRecord`, which tracks the job's execution status and logs.

- **Job Registration:** The `Job` instance is stored in a global dictionary (`jobs_registry`) with the `job_id` as the key. This allows subsequent status polling and result retrieval via the `get_status()` method on the `Job` object.

- **Status Polling:** The `/status/<job_id>` endpoint retrieves the corresponding `Job` object from `jobs_registry`. It calls `get_status()` to obtain a `JobRecord`, then inspects the `status` attribute. Depending on the status:
  - If still starting or running, respond with HTTP 202 and `{"job_id": ..., "status": "pending"}`.
  - If finished successfully, call `job.job_logs()` to retrieve output logs, attempt to parse JSON result, and respond with HTTP 200 including the result.
  - If failed, respond with HTTP 200 including an error message.
  - If the job is in an unknown state, respond accordingly.

- **Logging and Comments:** Each decision branch includes `print()` statements that describe the flow, such as returning pending status, job completion with result, or failure, facilitating debugging and traceability.

This design ensures that task submission, status management, and result retrieval leverage the `nodejobs` infrastructure, demonstrating its proper usage for process control, job tracking, and log management. It emphasizes the importance of using `nodejobs.Job`, `JobRecord`, and associated methods according to their intended patterns, avoiding manual process handling and ensuring scalability and reliability in asynchronous task execution.```python
backup_job_record = nodejobs.Job.run(command=backup_command, job_id=generate_unique_job_name(job_id))
```
This line calls `nodejobs.Job.run()` with named arguments: `command=backup_command` (the shell command string to execute) and `job_id=generate_unique_job_name(job_id)` (a unique identifier generated to prevent job ID collisions). It spawns a new backup job process, tracks it, and returns a `JobRecord` object assigned to `backup_job_record`.

```python
active_jobs = nodejobs.Job.list(filter=dict(status=nodejobs.JobRecord.Status.c_running))
```
This line invokes `nodejobs.Job.list()` with a `filter` argument as a dictionary: `status=nodejobs.JobRecord.Status.c_running`. This filters the list to include only jobs with status "running". It returns a dictionary `active_jobs` with job IDs as keys and `JobRecord` instances as values, representing all currently executing jobs.

```python
status_of_job = nodejobs.Job.get_status(job_id=backup_job_record.f_self_id)
```
This line uses `nodejobs.Job.get_status()` with the named argument `job_id=backup_job_record.f_self_id` (the unique ID of the specific job). It retrieves the latest status of the specified job and returns an updated `JobRecord`, stored in `status_of_job`.

```python
stop_result = nodejobs.Job.stop(job_id=backup_job_record.f_self_id)
```
This line calls `nodejobs.Job.stop()` with `job_id=backup_job_record.f_self_id`, signaling the process to terminate. It attempts a graceful shutdown and returns a `JobRecord` indicating the final status, assigned to `stop_result`.

```python
stdout_log, stderr_log = nodejobs.Job.job_logs(job_id=backup_job_record.f_self_id)
```
This line invokes `nodejobs.Job.job_logs()` with `job_id=backup_job_record.f_self_id`. It reads the log files associated with this job, returning a tuple: `stdout_log` and `stderr_log`, containing the captured standard output and error logs respectively.

---

This sequence demonstrates the consistent use of `nodejobs.Job` class methods with **all** function calls utilizing **named arguments**. It shows how to spawn, list, check status, stop, and retrieve logs of jobs, adhering strictly to the design patterns that leverage `nodejobs` module, `Job`, `JobRecord`, and related classes—all essential for managing jobs programmatically.```python
import os
import time
import uuid
from typing import Tuple, Optional
import nodejobs
from nodejobs.job import JobRecord, JobRecord.Status

class JobManagerDemo:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.jobs_manager: nodejobs.Jobs = self.initialize_job_manager()

    def initialize_job_manager(self) -> nodejobs.Jobs:
        os.makedirs(self.db_path, exist_ok=True)
        jobs_manager = nodejobs.Jobs(db_path=self.db_path)
        assert jobs_manager is not None, "Failed to initialize Jobs manager."
        return jobs_manager

    def spawn_job(self, command: str, job_id: str) -> JobRecord:
        job_record = self.jobs_manager.run(command=command, job_id=job_id)
        assert job_record is not None, "Job spawn returned None."
        assert job_record.f_self_id == job_id, "Job ID mismatch after spawn."
        return job_record

    def wait_for_job_status(self, job_id: str, desired_status: str, timeout_seconds: float = 30.0) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            status_record = self.jobs_manager.get_status(job_id)
            if status_record.f_status == desired_status:
                return True
            time.sleep(0.5)
        return False

    def verify_job_is_running(self, job_id: str) -> None:
        status_record = self.jobs_manager.get_status(job_id)
        assert status_record.f_status == Status.c_running, \
            f"Expected job to be running, but status is {status_record.f_status}."

    def retrieve_job_logs(self, job_id: str) -> Tuple[str, str]:
        stdout_log, stderr_log = self.jobs_manager.job_logs(job_id)
        assert isinstance(stdout_log, str), "Stdout log is not a string."
        assert isinstance(stderr_log, str), "Stderr log is not a string."
        return stdout_log, stderr_log

    def stop_job(self, job_id: str) -> None:
        result_record = self.jobs_manager.stop(job_id)
        assert result_record is not None, "Failed to stop job; result is None."
        assert hasattr(result_record, 'f_self_id'), "Result JobRecord missing 'f_self_id'."

    def cleanup(self) -> None:
        # Placeholder for any cleanup actions if needed
        pass

# Main function demonstrating the usage of the JobManagerDemo class
# Stage 1: Initialize job manager
# Stage 2: Spawn a new job
# Stage 3: Verify job is running
# Stage 4: Wait for job completion
# Stage 5: Retrieve logs
# Stage 6: Stop job if necessary
# Stage 7: Cleanup resources

if __name__ == "__main__":
    # Stage 1. Initialize Job Manager
    demo = JobManagerDemo(db_path="./test_job_db")

    # Stage 2. Spawn a new job
    unique_job_id = str(uuid.uuid4())
    command = "echo 'Starting job'; sleep 2; echo 'Job finished'"
    job_record = demo.spawn_job(command=command, job_id=unique_job_id)

    # Stage 3. Verify job is running
    demo.verify_job_is_running(job_id=unique_job_id)

    # Stage 4. Wait for job to reach 'finished' status
    job_completed = demo.wait_for_job_status(job_id=unique_job_id, desired_status=Status.c_finished, timeout_seconds=10)
    assert job_completed, "Job did not reach finished status within timeout."

    # Stage 5. Retrieve logs
    stdout_logs, stderr_logs = demo.retrieve_job_logs(job_id=unique_job_id)
    print(f"Stdout logs:\n{stdout_logs}")
    print(f"Stderr logs:\n{stderr_logs}")

    # Stage 6. Stop the job (if still running)
    demo.stop_job(job_id=unique_job_id)
    final_status = demo.jobs_manager.get_status(unique_job_id).f_status
    assert final_status in (Status.c_stopped, Status.c_finished), \
        f"Unexpected final status: {final_status}"

    # Stage 7. Cleanup resources
    demo.cleanup()
```