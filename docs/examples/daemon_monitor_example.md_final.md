**Introduction:**

This example demonstrates how to programmatically manage background jobs using the `nodejobs` module, focusing on the `Job` class. It highlights key operations such as spawning, monitoring, stopping jobs, and retrieving logs, all within an object-oriented design. The example emphasizes best practices like using named arguments in method calls, leveraging `nodejobs.Job` methods rather than manual process management, and maintaining clear, maintainable code for robust job control.

---

**Step-by-step Walkthrough:**

**1. Initialize Job Manager**

The first stage involves setting up the job management environment. We instantiate a `JobManagerDemo` class, which internally initializes a `nodejobs.Jobs` object with a specified database path. This object serves as the interface for all subsequent job operations.

```python
demo = JobManagerDemo(db_path="./test_job_db")
```

This prepares a dedicated environment to spawn, monitor, and control jobs.

---

**2. Spawn a New Job**

Next, we generate a unique identifier for our job to avoid ID collisions, define a shell command that simulates a short task, and spawn the job using the `nodejobs.Job.run()` method.

```python
unique_job_id = str(uuid.uuid4())
command = "echo 'Starting job'; sleep 2; echo 'Job finished'"
job_record = demo.spawn_job(command=command, job_id=unique_job_id)
```

This calls `nodejobs.Job.run()` with named arguments `command` and `job_id`. It initiates the job process, tracks it, and returns a `JobRecord` confirming successful creation.

---

**3. Verify the Job is Running**

After spawning, it's important to verify that the job has started correctly and is in the running state.

```python
demo.verify_job_is_running(job_id=unique_job_id)
```

This method retrieves the current status using `nodejobs.Job.get_status()` and asserts that the status is `c_running`, ensuring the job has transitioned into execution.

---

**4. Wait for Job Completion**

Since the command includes a sleep of 2 seconds, we wait for the job to complete, with a timeout to prevent indefinite blocking.

```python
job_completed = demo.wait_for_job_status(job_id=unique_job_id, desired_status=Status.c_finished, timeout_seconds=10)
assert job_completed, "Job did not reach finished status within timeout."
```

This polling uses `nodejobs.Job.get_status()` repeatedly, checking if the job has reached the `c_finished` status within 10 seconds.

---

**5. Retrieve Logs**

Once the job finishes, we retrieve its logs to verify execution output and diagnose issues if needed.

```python
stdout_logs, stderr_logs = demo.retrieve_job_logs(job_id=unique_job_id)
print(f"Stdout logs:\n{stdout_logs}")
print(f"Stderr logs:\n{stderr_logs}")
```

This calls `nodejobs.Job.job_logs()` with the `job_id`, returning stdout and stderr logs, which are printed for inspection.

---

**6. Stop the Job (if still active)**

Although the job should be finished, this demonstrates how to gracefully stop a job if it remains active unexpectedly.

```python
demo.stop_job(job_id=unique_job_id)
final_status = demo.jobs_manager.get_status(unique_job_id).f_status
assert final_status in (Status.c_stopped, Status.c_finished), \
    f"Unexpected final status: {final_status}"
```

This uses `nodejobs.Job.stop()` with the `job_id`, then verifies that the job's final status is either `stopped` or `finished`.

---

**7. Cleanup Resources**

Finally, any necessary cleanup actions are performed, such as releasing resources or resetting the environment.

```python
demo.cleanup()
```

This placeholder method can be expanded as needed to ensure a clean state for future operations.

---

**Full Class Implementation and Usage:**

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
        job_record = self.jobs_manager.run(command=command, job_id=job_id)
        assert job_record is not None, "Job spawn returned None."
        assert job_record.f_self_id == job_id, "Job ID mismatch after spawn."
        return job_record

    def verify_job_is_running(self, job_id: str) -> None:
        status_record = self.jobs_manager.get_status(job_id)
        assert status_record.f_status == Status.c_running, \
            f"Expected job to be running, but status is {status_record.f_status}."

    def wait_for_job_status(self, job_id: str, desired_status: str, timeout_seconds: float = 30.0) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            status_record = self.jobs_manager.get_status(job_id)
            if status_record.f_status == desired_status:
                return True
            time.sleep(0.5)
        return False

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

# Usage demonstration
if __name__ == "__main__":
    # Stage 1: Initialize job manager
    demo = JobManagerDemo(db_path="./test_job_db")

    # Stage 2: Spawn a new job
    unique_job_id = str(uuid.uuid4())
    command = "echo 'Starting job'; sleep 2; echo 'Job finished'"
    job_record = demo.spawn_job(command=command, job_id=unique_job_id)

    # Stage 3: Verify job is running
    demo.verify_job_is_running(job_id=unique_job_id)

    # Stage 4: Wait for job to reach 'finished' status
    job_completed = demo.wait_for_job_status(job_id=unique_job_id, desired_status=Status.c_finished, timeout_seconds=10)
    assert job_completed, "Job did not reach finished status within timeout."

    # Stage 5: Retrieve logs
    stdout_logs, stderr_logs = demo.retrieve_job_logs(job_id=unique_job_id)
    print(f"Stdout logs:\n{stdout_logs}")
    print(f"Stderr logs:\n{stderr_logs}")

    # Stage 6: Stop the job (if still running)
    demo.stop_job(job_id=unique_job_id)
    final_status = demo.jobs_manager.get_status(unique_job_id).f_status
    assert final_status in (Status.c_stopped, Status.c_finished), \
        f"Unexpected final status: {final_status}"

    # Stage 7: Cleanup resources
    demo.cleanup()
```

---

This structured, detailed example illustrates how to effectively use the `nodejobs` module and its `Job` class for complete programmatic job management, following best practices like consistent use of named arguments and leveraging the API as intended.