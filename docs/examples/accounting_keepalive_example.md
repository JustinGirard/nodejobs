# Keepalive Monitoring Script for the `accounting_monitor_main` Job

This document provides a structured, object-oriented approach for implementing a keepalive script that ensures the continuous operation of the `accounting_monitor_main` job using the `nodejobs` module. The script periodically checks the job’s status via the `Job.list_status()` method, attempts to restart the job if it is not running using `Job.run()`, verifies the restart, and handles failure scenarios with alerting and controlled termination after multiple consecutive failures. The implementation emphasizes correct usage of the `nodejobs.Job`, `JobRecord`, and `JobFilter` classes, adhering to best practices for process supervision within a Python environment.

---

### Short Demonstration:
This example demonstrates how to utilize the `nodejobs` framework to maintain a persistent background process. It periodically verifies the status of a critical job, restarts it if necessary, and signals alerts upon repeated failures, ensuring robust and automated process supervision.

---

### Code Implementation:

```python
import time
from nodejobs import Job, JobRecord, JobFilter

# Stage 1: Define constants and initialize failure counter
monitor_job = "accounting_monitor_main"
consecutive_failures = 0
max_failures = 20

# Stage 2: Function to check and restart the job if necessary
def check_and_restart_job() -> bool:
    """
    Checks the status of the monitor job.
    If not running, attempts to restart and verifies restart success.
    Returns True if job is confirmed running after check, False otherwise.
    """
    # 2.1: List current status of the job
    status_filter = JobFilter({Job.f_self_id: monitor_job})
    status_dict = Job.list_status(filter=status_filter)
    job_record = status_dict.get(monitor_job)

    # 2.2: Determine if job is not running
    if job_record is None or job_record[Job.f_status] != JobRecord.Status.c_running:
        # 2.3: Attempt to restart the job
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {monitor_job} not running, attempting restart.")
        run_result = Job.run(
            command={Job.f_self_id: monitor_job, Job.f_status: "start"},
            job_id=monitor_job
        )
        # 2.4: Wait 10 seconds for restart to take effect
        time.sleep(10)
        # 2.5: Verify if job has restarted
        status_dict_after = Job.list_status(filter=JobFilter({Job.f_self_id: monitor_job}))
        job_after_restart = status_dict_after.get(monitor_job)
        if job_after_restart is None or job_after_restart[Job.f_status] != JobRecord.Status.c_running:
            # 2.6: Fetch last error and alert
            error_msg = Job.get_error(job_id=monitor_job)
            print(f"Failed to restart {monitor_job}: {error_msg}")
            print("EMAIL TO admin@example.com: Failed to restart {monitor_job} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            return False
        else:
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {monitor_job} successfully restarted.")
            return True
    # 2.7: Job is already running
    return True

# Stage 3: Main loop to monitor and maintain the job
while True:
    # 3.1: Check the status of the job
    is_running = check_and_restart_job()

    if is_running:
        # 3.2: Job is alive, reset failure counter
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {monitor_job} is alive.")
        consecutive_failures = 0
    else:
        # 3.3: Increment failure counter
        consecutive_failures += 1
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {monitor_job} restart attempt failed. Failure count: {consecutive_failures}")
        # 3.4: Exit after 20 consecutive failures
        if consecutive_failures >= max_failures:
            print("Exiting keepalive after 20 failures.")
            break

    # 3.5: Wait 5 minutes before next check
    time.sleep(300)
```

---

### Overall Approach and Key Concepts:

This script exemplifies a robust process supervision pattern leveraging the `nodejobs` API. The core logic involves:

- **Monitoring job status:** Utilizes `Job.list_status()` with a `JobFilter` on `Job.f_self_id` to reliably determine if the target job is running.
- **Automated restart logic:** When the job is not in a `c_running` state, `Job.run()` is invoked with explicit command parameters, aligning with the `nodejobs` API pattern.
- **Verification after restart:** Implements a 10-second delay followed by a status check to confirm successful restart, ensuring resilience against transient failures.
- **Error retrieval and alerting:** If restart attempts fail, `Job.get_error()` fetches the last error message, and an alert is simulated via `print()`.
- **Failure tracking and controlled shutdown:** Maintains a counter of consecutive failures, with a safe exit after 20 unsuccessful restart attempts to prevent infinite loops.
- **Continuous supervision:** The infinite loop with a 5-minute sleep interval maintains persistent monitoring, automatically recovering the job if it terminates.

This implementation underscores the importance of using `nodejobs.Job` methods as intended, applying correct argument patterns, and handling job states explicitly. It demonstrates a practical, maintainable approach to process supervision, suitable for production environments requiring high availability of critical background tasks.```python
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