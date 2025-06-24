# Documentation for `JobRecordDict`

## Introduction

`JobRecordDict` is a container class designed to manage collections of `JobRecord` instances within a structured validation framework. It streamlines the handling, validation, and access of multiple job records, primarily in database interactions and data processing workflows.

## Method Reference

### `get_keys() -> (dict, dict)`

Returns two dictionaries indicating required and optional keys for the container.

- **Parameters:**  
  None

- **Returns:**  
  A tuple with two dictionaries:  
  - `required`: Dictionary of mandatory keys (empty in this implementation).  

```python
# usage
job_dict: JobRecordDict = JobRecordDict(in_data)
required_keys, optional_keys = job_dict.get_keys()  # required is empty, optional contains '*'
```

### `__init__(self, in_data: dict)`

Initializes the `JobRecordDict` with input data, typically a dictionary of job records.

- **Parameters:**  
  - `in_data` (dict): Input data containing job records, keyed by their identifiers.

```python
# usage
data: dict = {
    "job_123": {
        JobRecord.self_id: "job_123",
        JobRecord.status: JobRecord.Status.c_running,
        JobRecord.last_update: datetime.datetime.utcnow(),
    }
}
job_collection: JobRecordDict = JobRecordDict(data)
```

### `__getitem__(self, key) -> JobRecord`

Provides access to individual `JobRecord` instances via their keys.

- **Parameters:**  
  - `key`: Identifier for a specific job record.

- **Returns:**  
  Corresponding `JobRecord` object.

```python
# usage
job: JobRecord = job_collection["job_123"]
```

## Example

```python
# usage
from nodejobs.jobdb import JobRecord, JobRecordDict

# Initialize a JobRecord
record_data: dict = {
    JobRecord.self_id: "job_123",
    JobRecord.status: JobRecord.Status.c_running,
    JobRecord.last_update: datetime.datetime.utcnow(),
}
job_record: JobRecord = JobRecord(record_data)

# Wrap into JobRecordDict
job_collection: JobRecordDict = JobRecordDict({record_data[JobRecord.self_id]: record_data})
# Also works: job_collection: JobRecordDict = JobRecordDict({record_data.self_id: record_data})

# Access all records
all_jobs: dict = job_collection
```
