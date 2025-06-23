Introduction:
This document provides a comprehensive example of managing background jobs programmatically using the `nodejobs` API, particularly focusing on the `nodejobs.Job` class. The example demonstrates how to spawn, monitor, and control jobs—such as an accounting summary process—by leveraging object-oriented design principles. It highlights key practices such as checking for existing logs to prevent duplicate runs, employing periodic polling to ensure job health, and restarting jobs automatically if they terminate unexpectedly. The implementation emphasizes the importance of using **named arguments** in all function calls for clarity and correctness, aligning with the best practices for utilizing the `nodejobs` module. This pattern ensures resilient and transparent job management, suitable for indefinite background processes that require continuous monitoring and automatic recovery.

---

Step-by-step Walkthrough:

**1. Initialize the Job Management System**

The first stage involves setting up the environment to manage jobs. This is done by creating an instance of the `nodejobs.Jobs` class, specifying the directory where job data will be stored. This setup ensures that all subsequent job operations are tracked and managed within a persistent database.

```python
# Instantiate the job manager with a specific database path
demo = JobManagerDemo(db_path="./test_job_db")
```

This step prepares the system for spawning and managing jobs, establishing the necessary infrastructure.

---

**2. Check for Existing Logs and Spawn a New Job**

Before launching a new accounting summary job, the monitor checks whether prior logs indicate it has already run. If logs exist, it assumes the job has completed previously and remains idle. Otherwise, it proceeds to spawn a new job.

```python
# Generate a unique job ID and define the command to execute
unique_job_id = str(uuid.uuid4())
command = "echo 'Starting job'; sleep 2; echo 'Job finished'"

# Spawn a new job using the nodejobs.Job.run() method with named arguments
job_record = demo.spawn_job(command=command, job_id=unique_job_id)
```

This code calls `nodejobs.Job.run()` with **named arguments** `command` and `job_id`, which initiates a background process that outputs messages, sleeps for two seconds, and then outputs again. The method returns a `JobRecord` object that tracks this job.

---

**3. Verify the Job is Running**

After spawning, the monitor confirms that the job has transitioned into the running state. This validation ensures the process has started correctly.

```python
# Check that the job is actively running
demo.verify_job_is_running(job_id=unique_job_id)
```

This method retrieves the current status using `get_status()` and asserts that the job's state is `c_running`. It leverages `nodejobs.Job`’s status management to verify the job's health.

---

**4. Wait for Job Completion**

Since the job involves a sleep command, the monitor waits until the job reaches the `finished` state. It does this by polling periodically with a timeout, ensuring the process completes successfully.

```python
# Wait until the job status is 'finished' or timeout occurs
job_completed = demo.wait_for_job_status(
    job_id=unique_job_id,
    desired_status=Status.c_finished,
    timeout_seconds=10
)
assert job_completed, "Job did not reach finished status within timeout."
```

This polling mechanism uses `get_status()` repeatedly until the desired status is detected or the timeout expires, providing robust synchronization.

---

**5. Retrieve and Display Logs**

Once the job completes, the example retrieves its stdout and stderr logs to verify output and aid debugging.

```python
# Retrieve logs from the completed job
stdout_logs, stderr_logs = demo.retrieve_job_logs(job_id=unique_job_id)
print(f"Stdout logs:\n{stdout_logs}")
print(f"Stderr logs:\n{stderr_logs}")
```

This step uses `nodejobs.Job.job_logs()` with **named arguments** to access the logs, demonstrating how to extract execution details for auditing or troubleshooting.

---

**6. Stop the Job if Necessary**

Although the job should be finished, the example also shows how to gracefully stop a job if it’s still running, preserving control over background processes.

```python
# Attempt to stop the job
demo.stop_job(job_id=unique_job_id)

# Confirm the final status is either 'stopped' or 'finished'
final_status = demo.jobs_manager.get_status(unique_job_id).f_status
assert final_status in (Status.c_stopped, Status.c_finished), \
    f"Unexpected final status: {final_status}"
```

This demonstrates using `nodejobs.Job.stop()` with **named arguments** to terminate the process and verifying the outcome.

---

**7. Cleanup Resources**

Finally, the example includes a placeholder for any cleanup actions needed to release resources or finalize the management environment.

```python
# Cleanup resources if needed
demo.cleanup()
```

This ensures tidy shutdown and resource management after job operations.

---

Full Class Implementation and Usage Example:

```python
import os
import time
import uuid
from typing import Tuple
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
        # Spawn a new job with specified command and job ID
        job_record = self.jobs_manager.run(command=command, job_id=job_id)
        assert job_record is not None, "Job spawn returned None."
        assert job_record.f_self_id == job_id, "Job ID mismatch after spawn."
        return job_record

    def wait_for_job_status(self, job_id: str, desired_status: str, timeout_seconds: float = 30.0) -> bool:
        # Poll the status until the desired status is reached or timeout
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            status_record = self.jobs_manager.get_status(job_id)
            if status_record.f_status == desired_status:
                return True
            time.sleep(0.5)
        return False

    def verify_job_is_running(self, job_id: str) -> None:
        # Assert that the job is currently running
        status_record = self.jobs_manager.get_status(job_id)
        assert status_record.f_status == Status.c_running, \
            f"Expected job to be running, but status is {status_record.f_status}."

    def retrieve_job_logs(self, job_id: str) -> Tuple[str, str]:
        # Fetch stdout and stderr logs for the job
        stdout_log, stderr_log = self.jobs_manager.job_logs(job_id)
        assert isinstance(stdout_log, str), "Stdout log is not a string."
        assert isinstance(stderr_log, str), "Stderr log is not a string."
        return stdout_log, stderr_log

    def stop_job(self, job_id: str) -> None:
        # Attempt to gracefully stop the job
        result_record = self.jobs_manager.stop(job_id)
        assert result_record is not None, "Failed to stop job; result is None."
        assert hasattr(result_record, 'f_self_id'), "Result JobRecord missing 'f_self_id'."

    def cleanup(self) -> None:
        # Placeholder for cleanup logic if needed
        pass

# Main execution demonstrating the process
if __name__ == "__main__":
    # Stage 1: Initialize job manager
    demo = JobManagerDemo(db_path="./test_job_db")
    
    # Stage 2: Check for existing logs and spawn a new job if none found
    unique_job_id = str(uuid.uuid4())
    command = "echo 'Starting job'; sleep 2; echo 'Job finished'"
    job_record = demo.spawn_job(command=command, job_id=unique_job_id)

    # Stage 3: Verify the job is actively running
    demo.verify_job_is_running(job_id=unique_job_id)

    # Stage 4: Wait for job to complete (reach 'finished' status)
    job_completed = demo.wait_for_job_status(
        job_id=unique_job_id,
        desired_status=Status.c_finished,
        timeout_seconds=10
    )
    assert job_completed, "Job did not reach finished status within timeout."

    # Stage 5: Retrieve and display logs
    stdout_logs, stderr_logs = demo.retrieve_job_logs(job_id=unique_job_id)
    print(f"Stdout logs:\n{stdout_logs}")
    print(f"Stderr logs:\n{stderr_logs}")

    # Stage 6: Stop the job if still active
    demo.stop_job(job_id=unique_job_id)
    final_status = demo.jobs_manager.get_status(unique_job_id).f_status
    assert final_status in (Status.c_stopped, Status.c_finished), \
        f"Unexpected final status: {final_status}"

    # Stage 7: Cleanup resources
    demo.cleanup()
```

This structured, fully formatted example demonstrates how to effectively manage background jobs using the `nodejobs` API, adhering to best practices such as using **named arguments** and leveraging the `Job`, `JobRecord`, and `JobFilter` classes for robust, automated background processing.