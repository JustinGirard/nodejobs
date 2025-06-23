### Introduction

The `JobFilter` class provides a schema-driven mechanism for constructing and validating filtering criteria when querying job records within the system. It is designed for developers familiar with the system’s data architecture, offering a structured way to specify search parameters that ensure consistency and correctness.

### Short Method Reference

- `__init__(self, filter: dict = None, trim: bool = False)`  
  Initializes a `JobFilter` instance with optional filtering criteria.  
  **Parameters:**  
  - `filter` (dict, optional): A dictionary of key-value pairs representing filter conditions. Defaults to `None`.  
  - `trim` (bool, default=False): When `True`, removes keys not defined in the schema.

- `get_keys(self) -> Tuple[dict, dict]`  
  Returns the schema of the filter, including required and optional fields.  
  **Returns:**  
  - A tuple: `(required_fields: dict, optional_fields: dict)` containing the schema definitions.

### Usage Example

```python
# usage
from your_module import JobFilter

# Initialize filter with specific criteria
filter_criteria: dict = {"status": "running", "dirname": "b"}
job_filter: JobFilter = JobFilter(filter=filter_criteria)

# Retrieve filtered job records
filtered_jobs = self.jobdb.list_status(filter=job_filter)
# filtered_jobs is a dict of job records matching the criteria
```

This class streamlines the creation of validated, schema-compliant filters for job data queries, promoting consistency and reliability in data retrieval operations.