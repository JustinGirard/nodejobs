# Title: Web-Scraper Scheduler Using nodejobs

## Introduction:
This example illustrates the construction of a Python-based web-scraper scheduler utilizing the `nodejobs.Job` class. The scheduler manages multiple target URLs, performing lazy execution by checking existing output files for freshness, and launching jobs only when necessary. It captures the output of each job, stores it systematically, and compiles a comprehensive JSON report summarizing success, data size, and errors. The design emphasizes idempotency, proper process management via `nodejobs`, and clear logging, demonstrating best practices for distributed job control within the nodejobs framework.

---

## Step-by-step Walkthrough:

### **Stage 1: Check Output Freshness**

The first step is to determine if the output file for a given URL already exists and is recent enough to avoid re-scraping.

```python
def check_output_freshness(self, *, url):
    """
    Checks if the output file for the URL exists and is recent enough.
    Returns True if output is fresh; False if missing or stale.
    """
    hostname = self.extract_hostname({url}=url)
    date_str = self.current_date_str({})
    output_filename = f"scrape_{hostname}_{date_str}.json"
    output_path = os.path.join({self.output_dir}=self.output_dir, {output_filename}=output_filename)

    if os.path.exists({self.output_path}=output_path):
        mod_time = os.path.getmtime({self.output_path}=output_path)
        age_seconds = time.time() - {mod_time}
        return age_seconds < self.refresh_interval_seconds
    return False
```

This function inspects whether the output JSON file for a specific URL exists and whether it was modified within the refresh interval. If so, it skips re-scraping that URL.

---

### **Stage 2: Launch a Job for a URL**

If the output is stale or missing, the scheduler spawns a new scraping job.

```python
def launch_job_for_url(self, *, url):
    """
    Creates and runs a nodejobs.Job for the URL, with a unique job ID.
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
```

This creates a `nodejobs.Job` instance, runs it with the formatted command and a unique job ID, and tracks it in the job list.

---

### **Stage 3: Monitor All Active Jobs**

After launching necessary jobs, the scheduler waits until all of them complete by polling their status.

```python
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
```

This process ensures all jobs have finished, then fetches logs and output files to assess success and data size, appending results to a report list.

---

### **Stage 4: Generate Final Report**

Once all jobs are processed, the scheduler outputs a JSON summary and saves a human-readable report.

```python
def generate_report(self):
    """
    Outputs JSON report to stdout and writes human-readable log.
    """
    json_report = json.dumps({self}.reports, indent=2)
    print({json_report})  # Print JSON summary.
    report_path = os.path.join({self}.output_dir, "summary_report.txt")
    with open({report_path}=report_path, mode='w') as f:
        f.write({json_report})
```

This provides a clear summary of the scraping operations, success, and errors.

---

### **Stage 5: Main Execution Flow**

Putting it all together, the `run()` method performs the sequence: check outputs, launch jobs as needed, wait for completion, and generate reports.

```python
def run(self):
    """
    Main execution: check outputs, launch necessary jobs, and generate report.
    """
    for url in {self}.target_urls:
        if {self}.check_output_freshness({url}=url):
            print(f"Skipping {url}: output up-to-date")
        else:
            {self}.launch_job_for_url({url}=url)
    {self}.monitor_jobs()
    {self}.generate_report()
```

---

## Full Class Implementation and Usage:

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
        date_str = self.current_date_str({})
        output_filename = f"scrape_{hostname}_{date_str}.json"
        output_path = os.path.join({self.output_dir}=self.output_dir, {output_filename}=output_filename)

        if os.path.exists({self.output_path}=output_path):
            mod_time = os.path.getmtime({self.output_path}=output_path)
            age_seconds = time.time() - {mod_time}
            return age_seconds < self.refresh_interval_seconds
        return False

    def launch_job_for_url(self, *, url):
        """
        Creates and runs a nodejobs.Job for the URL, with a unique job ID.
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

This approach demonstrates disciplined use of `nodejobs` classes and methods, ensuring process management, logging, and error handling are integrated seamlessly—serving as a comprehensive model for web-scraper job orchestration within a distributed or scheduled environment.