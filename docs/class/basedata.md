# `BaseData` Class Documentation

## Introduction
`BaseData` is a schema-driven container class designed to facilitate structured data management, validation, and consistency within complex systems. It is intended for developers working with nested, schema-dependent data structures, ensuring data integrity and explicit key handling.

## Short Method Reference

### `__init__(self, data: dict = None, trim: bool = False)`
Initializes an instance by wrapping a dictionary into a schema-validated container. It enforces key consistency and can trim extraneous data.
- **Parameters:**
  - `data` (dict, optional): Initial data to populate the instance.
  - `trim` (bool, default=False): If true, removes keys not defined in the schema.

``` python
# usage
# Assuming MySchema is a subclass of BaseData
initial_data: dict = {"status": "active", "value": 42}
instance: MySchema = MySchema(data=initial_data)
```

### `get_keys(self) -> Tuple[dict, dict]`
Returns two dictionaries: required and optional schema fields.
- **Returns:**  
  Tuple containing:
  - Required fields dictionary  
  - Optional fields dictionary

```python
# usage
required_fields, optional_fields = instance.get_keys()
```

### `clean(self) -> None`
Removes any keys from the internal data that are not declared in the schema, maintaining data integrity. By default BaseData classes will leave extra fields alone.
```python
# usage
instance.clean()
```

## Summary
`BaseData` supports schema validation, explicit key management via class constants, and nested data structures, making it a reliable foundation for managing complex, schema-dependent data in a controlled manner.
