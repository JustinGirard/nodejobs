# Title: A Nodejobs-Based Python Test-Runner for Aggregating JSON Test Results

## Short Demonstration:
This example demonstrates how to implement a Python test-runner script utilizing the `nodejobs.Job` class to invoke unit tests identified by known `job_name` strings. Each job executes a specific test, produces JSON output on stdout, and the script systematically collects and parses these results to generate an aggregate pass/fail report. The solution emphasizes the use of object-oriented patterns, proper nodejobs usage, and encapsulation, ensuring each test runs in isolation with logs captured accurately.

---

```python
# 1. Import necessary modules and classes
from typing import List, Dict
import json
import sys
import time
import os
import tempfile
from nodejobs import Job, JobRecord

# 2. Define the test runner class to manage job invocation and result processing
class TestRunner:
    def __init__(self, job_names: List[str], results_dir: str):
        self.job_names: List[str] = job_names
        self.results_dir: str = results_dir
        self.job_results: Dict[str, Dict] = {}
        self.overall_pass: bool = True

    def run_tests(self) -> None:
        for job_name in self.job_names:
            # Stage 1: Instantiate a Job object with the results directory
            job: Job = Job(db_path=self.results_dir)
            # Stage 2: Run the job with the specific test command and job_id
            job_record: JobRecord = job.run(
                command=f"python -m unittest {job_name}",
                job_id=job_name
            )
            # Initialize result storage for this job
            self.job_results[job_name] = {
                'job_record': job_record,
                'json_output': None,
                'success': False
            }
            # Stage 3: Wait for job completion
            self._wait_for_completion(job=job, job_record=job_record)
            # Stage 4: Collect JSON output from stdout log
            json_output: Dict = self._collect_json_output(job_record=job_record)
            self.job_results[job_name]['json_output'] = json_output
            # Stage 5: Update overall pass/fail status based on JSON result
            if json_output is None or not json_output.get('pass', False):
                self.overall_pass = False

    def _wait_for_completion(self, *, job: Job, job_record: JobRecord) -> None:
        timeout_seconds: int = 60
        start_time: float = time.time()
        while True:
            current_status: str = job.get_status(job_record[self.job_record.f_self_id]).status
            if current_status in [JobRecord.Status.c_finished, JobRecord.Status.c_failed_start]:
                break
            if (time.time() - start_time) > timeout_seconds:
                break
            time.sleep(1)

    def _collect_json_output(self, *, job_record: JobRecord) -> Dict:
        stdout_path: str
        stderr_path: str
        stdout_path, _ = self._get_log_paths(job_record=job_record)
        try:
            with open(file=stdout_path, mode='r') as log_file:
                content: str = log_file.read()
            return json.loads(obj=content)
        except Exception:
            return {}

    def _get_log_paths(self, *, job_record: JobRecord) -> (str, str):
        logdir: str = job_record[self.job_record.f_logdir]
        logfile: str = job_record[self.job_record.f_logfile]
        stdout_path: str = os.path.join(logdir, f"{logfile}_stdout.log")
        stderr_path: str = os.path.join(logdir, f"{logfile}_stderr.log")
        return stdout_path, stderr_path

    def report_summary(self) -> None:
        print("Test Results Summary:")
        for job_name, result in self.job_results.items():
            json_result: Dict = result['json_output']
            pass_status: bool = json_result.get('pass', False) if json_result else False
            status_str: str = "PASS" if pass_status else "FAIL"
            print(f"Job '{job_name}': {status_str}")
        overall_status: str = "PASSED" if self.overall_pass else "FAILED"
        print(f"Overall Test Result: {overall_status}")

# 3. Main function orchestrating the test execution
def main() -> None:
    # Stage 1: List of test job names to execute
    test_job_names: List[str] = ["test_module1", "test_module2", "test_module3"]
    # Stage 2: Create temporary directory for storing logs and results
    with tempfile.TemporaryDirectory() as results_directory:
        # Instantiate the test runner with job names and results directory
        test_runner: TestRunner = TestRunner(
            job_names=test_job_names,
            results_dir=results_directory
        )
        # Stage 3: Run all tests and collect results
        test_runner.run_tests()
        # Stage 4: Output a summary report
        test_runner.report_summary()

if __name__ == "__main__":
    main()
```

---

## Overall Approach, Key Concepts, and Under-the-Hood Operations

This solution leverages the `nodejobs` module, specifically the `Job` and `JobRecord` classes, to manage isolated test executions, logs, and result collection systematically. The `TestRunner` class encapsulates the entire process, ensuring a modular, maintainable design aligned with best practices for nodejobs usage:

- **Job instantiation (`Job(db_path=...)`)**: Creates a dedicated environment for each test, with logs stored in specified directories.
- **Job execution (`job.run(command=..., job_id=...)`)**: Executes individual test commands as separate jobs, passing the test name via `job_id`. This isolates tests and logs outputs distinctly.
- **Status polling (`get_status()`)**: Implements polling with a timeout to wait for job completion, utilizing `JobRecord.Status` constants to detect finished or failed states reliably.
- **Log retrieval (`_get_log_paths()`)**: Derives stdout and stderr log file paths from `JobRecord` fields, ensuring logs are read accurately.
- **Result parsing (`_collect_json_output()`)**: Reads stdout logs, which are expected to contain JSON-formatted test results, parsing the content with error handling to avoid crashes.
- **Result aggregation**: Stores individual results in a dictionary, then determines overall success based on each job's JSON 'pass' flag.
- **Reporting (`report_summary()`)**: Summarizes each job’s outcome and the overall pass/fail status in a clear, human-readable format.

This pattern ensures each test runs in a controlled environment, logs are systematically captured, and results are parsed reliably, demonstrating the correct and idiomatic use of the `nodejobs` module's core classes. The design is extendable for larger test suites, promotes code clarity, and emphasizes the importance of using nodejobs' process control features rather than manual subprocess management.```python
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