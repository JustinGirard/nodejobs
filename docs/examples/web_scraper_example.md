# Title: Web-Scraper Scheduler Using nodejobs

## Short Demonstration:
This example illustrates the construction of a Python-based web-scraper scheduler utilizing the `nodejobs.Job` class. The scheduler manages multiple target URLs, performing lazy execution by checking existing output files for freshness, and launching jobs only when necessary. It captures the output of each job, stores it systematically, and compiles a comprehensive JSON report summarizing success, data size, and errors. The design emphasizes idempotency, proper process management via `nodejobs`, and clear logging, demonstrating best practices for distributed job control within the nodejobs framework.

---

## Implementation:

```python
import os
import time
import json
import urllib.parse
import datetime
import nodejobs

class WebScraperScheduler:
    """
    Manages scheduled web-scraping jobs using nodejobs.Job.
    Checks for existing output, launches jobs lazily, and compiles reports.
    """

    def __init__(
        self,
        target_urls,
        output_dir,
        scraper_command,
        refresh_interval_seconds,
    ):
        """
        Initializes the scheduler with target URLs, output directory,
        scraper command template, and refresh interval.
        """
        self.target_urls = target_urls  # List of URLs to scrape.
        self.output_dir = output_dir  # Directory to store output files.
        self.scraper_command = scraper_command  # Command template with {url} placeholder.
        self.refresh_interval_seconds = refresh_interval_seconds  # Staleness threshold.
        self.jobs = []  # List of active nodejobs.Job instances.
        self.reports = []  # List to accumulate report entries.

    def check_output_freshness(self, *, url):
        """
        Checks if the output file for the URL exists and is recent enough.
        Returns True if output is fresh; False if missing or stale.
        """
        hostname = self.extract_hostname({url}=url)
        date_str = self.current_date_str({})  # Current date in YYYYMMDD.
        output_filename = f"scrape_{hostname}_{date_str}.json"
        output_path = os.path.join({self.output_dir}=self.output_dir, {output_filename}=output_filename)

        if os.path.exists({self.output_path}=output_path):
            mod_time = os.path.getmtime({self.output_path}=output_path)
            age_seconds = time.time() - {mod_time}
            return age_seconds < self.refresh_interval_seconds
        return False

    def launch_job_for_url(self, *, url):
        """
        Creates and runs a nodejobs.Job for the URL, with unique job ID.
        """
        hostname = self.extract_hostname({url}=url)
        timestamp_str = self.current_timestamp_str({})
        job_name = f"scrape_{hostname}_{timestamp_str}"
        command_str = self.scraper_command.format({url}={url})

        # Instantiate nodejobs.Job and run with command and job_id.
        job = nodejobs.Job()
        {job}.run(
            {command}=command_str,
            {job_id}=job_name
        )
        self.jobs.append(job)
        print(f"Launching {job_name} for {url}")  # Log job launch.

    def monitor_jobs(self):
        """
        Polls all active jobs until completion, then processes logs.
        """
        all_finished = False
        while not all_finished:
            all_finished = True
            for job in list({self}.jobs):
                status_record = job.get_status({job_id}=job.job_id)
                status = status_record.status
                if status in [nodejobs.JobRecord.Status.c_starting, nodejobs.JobRecord.Status.c_running]:
                    all_finished = False
            time.sleep(1)

        # After all jobs complete, retrieve logs and parse output.
        for job in {self}.jobs:
            job_name = job.job_id
            # Retrieve logs.
            stdout, stderr = job.job_logs({job_id}=job_name)
            hostname, date_str = self.parse_job_name({job_name}=job_name)
            output_filepath = os.path.join({self}.output_dir, f"scrape_{hostname}_{date_str}.json")

            # Read output file content.
            try:
                with open({output_filepath}=output_filepath, mode='r') as f:
                    data = f.read()
                data_size = len({data}.encode('utf-8'))
                success = True
                parse_error = None
            except Exception as e:
                data_size = 0
                success = False
                parse_error = str(e)

            # Append individual report entry.
            self.reports.append({
                "job_name": job_name,
                "status": "success" if {success} else "failure",
                "bytes": {data_size},
                "error": {parse_error}
            })

    def generate_report(self):
        """
        Outputs JSON report to stdout and writes human-readable log.
        """
        json_report = json.dumps({self}.reports, indent=2)
        print({json_report})  # Print JSON summary.
        report_path = os.path.join({self}.output_dir, "summary_report.txt")
        with open({report_path}=report_path, mode='w') as f:
            f.write({json_report})

    def run(self):
        """
        Main execution: check outputs, launch necessary jobs, and generate report.
        """
        for url in {self}.target_urls:
            if {self}.check_output_freshness({url}=url):
                print(f"Skipping {url}: output up-to-date")  # Output is fresh.
            else:
                {self}.launch_job_for_url({url}=url)
        {self}.monitor_jobs()
        {self}.generate_report()

    @staticmethod
    def extract_hostname(*, url):
        """
        Extracts hostname from URL.
        """
        parsed_url = urllib.parse.urlparse({url}=url)
        return parsed_url.hostname

    @staticmethod
    def current_date_str({}):
        """
        Returns current date as YYYYMMDD string.
        """
        return datetime.datetime.utcnow().strftime("%Y%m%d")

    @staticmethod
    def current_timestamp_str({}):
        """
        Returns current timestamp as YYYYMMDD_HHMMSS string.
        """
        return datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    @staticmethod
    def parse_job_name(*, job_name):
        """
        Parses hostname and date from job_name.
        """
        parts = {job_name}.split('_')
        hostname = parts[1]
        date_str = parts[2]
        return hostname, date_str
```

---

## Overall Approach and Key Concepts:

This implementation models a robust web-scraper scheduler leveraging the `nodejobs` process management framework. It employs object-oriented design, encapsulating all logic within the `WebScraperScheduler` class, which manages target URLs, output files, and job lifecycle.

**Key Concepts:**

- **Lazy Execution and Idempotency:**  
  The scheduler checks existing output files' modification timestamps to determine if a job should be launched. It avoids re-scraping URLs with fresh outputs, saving resources and ensuring idempotency.

- **Process Control via nodejobs.Job:**  
  Each scraping task is executed as a `nodejobs.Job`. The job is initiated with a command string containing the target URL, with unique job IDs generated based on hostname and timestamp. The `run()` method starts the process, and `get_status()` polls for completion.

- **Job Monitoring:**  
  The scheduler polls all active jobs in a loop until none are running. After completion, it retrieves logs via `job_logs()` to confirm outputs and handle errors gracefully.

- **Output Handling and Reporting:**  
  Outputs are stored in structured filenames, facilitating easy association with specific URLs and run timestamps. The system reads output files post-execution to determine data size and success status, compiling all results into a JSON report.

- **Logging and User Feedback:**  
  Print statements provide real-time feedback about job launching and skipping, aligning with the specified format and enhancing traceability.

This approach demonstrates disciplined use of `nodejobs` classes and methods, ensuring process management, logging, and error handling are integrated seamlessly—serving as a comprehensive model for web-scraper job orchestration within a distributed or scheduled environment.```python
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