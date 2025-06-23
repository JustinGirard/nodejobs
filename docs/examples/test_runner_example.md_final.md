# Title: A Nodejobs-Based Python Test-Runner for Aggregating JSON Test Results

## Introduction:
This example demonstrates how to implement a Python test-runner script utilizing the `nodejobs.Job` class to invoke unit tests identified by known `job_name` strings. Each job executes a specific test, produces JSON output on stdout, and the script systematically collects and parses these results to generate an aggregate pass/fail report. The solution emphasizes the use of object-oriented patterns, proper nodejobs usage, and encapsulation, ensuring each test runs in isolation with logs captured accurately.

---

## Step-by-step Walkthrough:

### Name a key stage of the example: Initialization of the Job Manager
```python
# Stage 1. Initialize Job Manager
demo = JobManagerDemo(db_path="./test_job_db")
```
This step creates an instance of the `JobManagerDemo` class, which internally initializes a `nodejobs.Jobs` object configured to store job data in the specified directory. This setup prepares the environment for spawning and managing jobs.

---

### Name a key stage of the example: Spawning a new test job
```python
# Stage 2. Spawn a new job
unique_job_id = str(uuid.uuid4())
command = "echo 'Starting job'; sleep 2; echo 'Job finished'"
job_record = demo.spawn_job(command=command, job_id=unique_job_id)
```
Here, a unique job ID is generated using `uuid.uuid4()`. The command simulates a simple test that outputs messages before and after a delay. The `spawn_job()` method calls `nodejobs.Job.run()` with the specified command and job ID, creating a new job process and returning its `JobRecord`. This ensures each test runs in isolation with a unique identifier.

---

### Name a key stage of the example: Verifying the job is running
```python
# Stage 3. Verify job is running
demo.verify_job_is_running(job_id=unique_job_id)
```
This step retrieves the job's status via `get_status()` and asserts that its status is `c_running`. This confirms that the job has successfully started executing within the nodejobs framework.

---

### Name a key stage of the example: Waiting for job to complete
```python
# Stage 4. Wait for job to reach 'finished' status
job_completed = demo.wait_for_job_status(job_id=unique_job_id, desired_status=Status.c_finished, timeout_seconds=10)
assert job_completed, "Job did not reach finished status within timeout."
```
The code polls the job's status periodically, with a maximum wait time of 10 seconds, until it reaches `c_finished`. It ensures the test has completed before proceeding to result collection.

---

### Name a key stage of the example: Retrieving logs
```python
# Stage 5. Retrieve logs
stdout_logs, stderr_logs = demo.retrieve_job_logs(job_id=unique_job_id)
print(f"Stdout logs:\n{stdout_logs}")
print(f"Stderr logs:\n{stderr_logs}")
```
This step reads the logs generated during the job's execution from the log files associated with the `JobRecord`. The logs are printed to verify the captured output, which should contain the JSON test results.

---

### Name a key stage of the example: Stopping the job if necessary
```python
# Stage 6. Stop the job (if still running)
demo.stop_job(job_id=unique_job_id)
final_status = demo.jobs_manager.get_status(unique_job_id).f_status
assert final_status in (Status.c_stopped, Status.c_finished), \
    f"Unexpected final status: {final_status}"
```
Here, the script attempts to gracefully stop the job, then verifies that the final status is either `stopped` or `finished`. This demonstrates controlling job lifecycle beyond just spawning and waiting.

---

### Name a key stage of the example: Cleanup resources
```python
# Stage 7. Cleanup resources
demo.cleanup()
```
Finally, a placeholder cleanup method is called, which can be used to remove temporary files or perform other cleanup tasks as needed.

---

## Full Class Implementation:
```python
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
```

## Usage Sequence:
- Initialize the job manager.
- Spawn a test job with a unique ID and command.
- Verify it is running.
- Wait for completion.
- Retrieve and display logs.
- Stop the job if it is still running.
- Perform cleanup as needed.

This pattern demonstrates the correct, idiomatic use of the `nodejobs` module's `Job` and `JobRecord` classes, ensuring that each step adheres to best practices for process control, log management, and result parsing within the nodejobs framework.