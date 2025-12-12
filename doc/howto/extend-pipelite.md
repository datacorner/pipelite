# How To: Extend pipelite

This guide shows you how to create custom data sources and transformers to extend pipelite's functionality.

---

## Overview

pipelite is designed to be extensible. You can create:
- **Custom Data Sources**: Read from or write to any data format or system
- **Custom Transformers**: Implement any data transformation logic
- **Custom Pipelines**: Create new pipeline execution strategies

By the end of this guide, you will:
- Understand the extension architecture
- Create a custom data source
- Create a custom transformer
- Register and use custom components

---

## Prerequisites

- pipelite installed
- Basic Python knowledge
- Understanding of pipelite concepts (extractors, transformers, loaders)

---

## Architecture Overview

pipelite uses abstract base classes that you extend:

```
┌─────────────────┐
│   plBaseObject  │  ← Base for all objects
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
┌───▼───┐ ┌───▼───────┐
│BOData │ │BOTransfor │
│Source │ │mer        │
└───────┘ └───────────┘
    │         │
    ▼         ▼
 Custom    Custom
 Sources   Transformers
```

---

## Creating a Custom Data Source

### Step 1: Create the Module

Create a new Python file `my_datasources/jsonFileDS.py`:

```python
"""
Custom JSON File Data Source for pipelite
Reads and writes JSON files
"""

from pipelite.baseobjs.BODataSource import BODataSource
from pipelite.plDataset import plDataset
import pipelite.constants as C
import json
import pandas as pd
import os


class jsonFileDS(BODataSource):
    """
    A data source for reading and writing JSON files.
    
    Parameters:
        filename: Name of the JSON file
        path: Directory path
        orient: JSON orientation (records, columns, index, split, values)
    """
    
    def __init__(self, config, log):
        super().__init__(config, log)
        self.filename = C.EMPTY
        self.path = C.EMPTY
        self.orient = "records"
    
    @property
    def parametersValidationFile(self):
        """Optional: Return path to JSON schema for parameter validation."""
        return C.EMPTY  # No validation schema
    
    def initialize(self, cfg) -> bool:
        """
        Initialize the data source with configuration parameters.
        
        Args:
            cfg: Configuration object with parameters
            
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            self.filename = cfg.getParameter("filename", C.EMPTY)
            self.path = cfg.getParameter("path", C.EMPTY)
            self.orient = cfg.getParameter("orient", "records")
            
            # Full path to file
            self.filepath = os.path.join(self.path, self.filename)
            
            # Validate file exists for extractors
            if self.objtype == C.PLJSONCFG_EXTRACTOR:
                if not os.path.isfile(self.filepath):
                    raise Exception(f"File not found: {self.filepath}")
            
            return True
            
        except Exception as e:
            self.log.error(f"Initialization error: {e}")
            return False
    
    def read(self) -> plDataset:
        """
        Read data from JSON file.
        
        Returns:
            plDataset containing the data
        """
        try:
            self.log.info(f"Reading JSON file: {self.filepath}")
            
            # Create dataset
            dataset = plDataset(self.config, self.log)
            
            # Read JSON into DataFrame
            df = pd.read_json(self.filepath, orient=self.orient)
            dataset.set(df)
            
            self.log.info(f"Read {dataset.count} rows")
            return dataset
            
        except Exception as e:
            self.log.error(f"Error reading JSON: {e}")
            return super().read()
    
    def write(self, dataset) -> bool:
        """
        Write data to JSON file.
        
        Args:
            dataset: plDataset to write
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.log.info(f"Writing JSON file: {self.filepath}")
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
            
            # Get DataFrame and write
            df = dataset.get()
            df.to_json(self.filepath, orient=self.orient, indent=2)
            
            self.log.info(f"Wrote {dataset.count} rows")
            return True
            
        except Exception as e:
            self.log.error(f"Error writing JSON: {e}")
            return False
```

### Step 2: Create Package Structure

```
my_datasources/
├── __init__.py
└── jsonFileDS.py
```

`my_datasources/__init__.py`:

```python
from .jsonFileDS import jsonFileDS

__all__ = ["jsonFileDS"]
```

### Step 3: Use in Pipeline

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "json_input",
        "classname": "my_datasources.jsonFileDS",
        "parameters": {
            "filename": "data.json",
            "path": "data/input/",
            "orient": "records"
        }
    }],
    
    "loaders": [{
        "id": "json_output",
        "classname": "my_datasources.jsonFileDS",
        "parameters": {
            "filename": "output.json",
            "path": "data/output/",
            "orient": "records"
        }
    }],
    
    "transformers": [{
        "id": "process",
        "classname": "pipelite.transformers.passthroughTR",
        "inputs": ["json_input"],
        "outputs": ["json_output"]
    }],
    
    "config": {
        "logger": {"level": "INFO", "path": "logs/", "filename": "json.log"}
    }
}
```

---

## Creating a Custom Transformer

### Step 1: Create the Module

Create `my_transformers/filterTR.py`:

```python
"""
Custom Filter Transformer for pipelite
Filters rows based on column conditions
"""

from pipelite.baseobjs.BOTransformer import BOTransformer
from pipelite.plDataset import plDataset
from pipelite.plDatasets import plDatasets
import pipelite.constants as C


class filterTR(BOTransformer):
    """
    A transformer that filters rows based on conditions.
    
    Parameters:
        column: Column to filter on
        operator: Comparison operator (eq, ne, gt, lt, gte, lte, contains)
        value: Value to compare against
    """
    
    def __init__(self, config, log):
        super().__init__(config, log)
        self.column = C.EMPTY
        self.operator = "eq"
        self.value = None
    
    def initialize(self, params) -> bool:
        """
        Initialize with filter parameters.
        
        Args:
            params: Configuration object
            
        Returns:
            True if successful
        """
        try:
            self.column = params.getParameter("column", C.EMPTY)
            self.operator = params.getParameter("operator", "eq")
            self.value = params.getParameter("value", None)
            
            if not self.column:
                raise Exception("column parameter is required")
            if self.value is None:
                raise Exception("value parameter is required")
                
            return True
            
        except Exception as e:
            self.log.error(f"Initialization error: {e}")
            return False
    
    def process(self, dsTransformerInputs) -> plDatasets:
        """
        Filter the input dataset.
        
        Args:
            dsTransformerInputs: Input datasets collection
            
        Returns:
            Filtered dataset collection
        """
        try:
            self.log.info(f"Filtering on {self.column} {self.operator} {self.value}")
            
            # Get first input dataset
            input_ds = dsTransformerInputs[0]
            df = input_ds.get().copy()
            
            # Apply filter based on operator
            if self.operator == "eq":
                mask = df[self.column] == self.value
            elif self.operator == "ne":
                mask = df[self.column] != self.value
            elif self.operator == "gt":
                mask = df[self.column] > self.value
            elif self.operator == "lt":
                mask = df[self.column] < self.value
            elif self.operator == "gte":
                mask = df[self.column] >= self.value
            elif self.operator == "lte":
                mask = df[self.column] <= self.value
            elif self.operator == "contains":
                mask = df[self.column].str.contains(str(self.value), na=False)
            else:
                raise Exception(f"Unknown operator: {self.operator}")
            
            # Apply mask
            filtered_df = df[mask]
            
            self.log.info(f"Filtered from {len(df)} to {len(filtered_df)} rows")
            
            # Create output dataset
            output = plDataset(self.config, self.log)
            output.set(filtered_df)
            output.id = self.dsOutputs[0]
            
            # Return as collection
            outputs = plDatasets()
            outputs.add(output)
            return outputs
            
        except Exception as e:
            self.log.error(f"Filter error: {e}")
            return dsTransformerInputs
```

### Step 2: Use in Pipeline

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "data",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "sales.csv",
            "path": "data/input/",
            "encoding": "utf-8"
        }
    }],
    
    "loaders": [{
        "id": "filtered_data",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "filtered_sales.csv",
            "path": "data/output/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [{
        "id": "filter_high_value",
        "classname": "my_transformers.filterTR",
        "inputs": ["data"],
        "outputs": ["filtered_data"],
        "parameters": {
            "column": "amount",
            "operator": "gte",
            "value": 1000
        }
    }],
    
    "config": {
        "logger": {"level": "INFO", "path": "logs/", "filename": "filter.log"}
    }
}
```

---

## Adding Parameter Validation

Create a JSON Schema for your component's parameters.

### Schema File (`filter_schema.json`)

```json
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["column", "operator", "value"],
    "properties": {
        "column": {
            "type": "string",
            "description": "Column name to filter on"
        },
        "operator": {
            "type": "string",
            "enum": ["eq", "ne", "gt", "lt", "gte", "lte", "contains"],
            "description": "Comparison operator"
        },
        "value": {
            "description": "Value to compare against"
        }
    }
}
```

### Reference in Transformer

```python
@property
def parametersValidationFile(self):
    return "path/to/filter_schema.json"
```

Or in the pipeline configuration:

```json
{
    "id": "filter",
    "classname": "my_transformers.filterTR",
    "validation": "schemas/filter_schema.json",
    "parameters": {...}
}
```

---

## Best Practices

### 1. Logging

Always use the provided logger:

```python
self.log.debug("Detailed debug information")
self.log.info("Important operational messages")
self.log.warning("Warning conditions")
self.log.error("Error conditions")
```

### 2. Error Handling

Handle errors gracefully and provide meaningful messages:

```python
try:
    # Operation
except SpecificException as e:
    self.log.error(f"Specific error: {e}")
    return False
except Exception as e:
    self.log.error(f"Unexpected error: {e}")
    raise
```

### 3. Documentation

Document your components with docstrings:

```python
class myComponent(BODataSource):
    """
    Brief description of the component.
    
    Parameters:
        param1: Description of param1
        param2: Description of param2
        
    Example:
        {
            "classname": "mypackage.myComponent",
            "parameters": {
                "param1": "value1"
            }
        }
    """
```

### 4. Testing

Create unit tests for your components:

```python
import unittest
from my_transformers.filterTR import filterTR

class TestFilterTR(unittest.TestCase):
    def test_filter_eq(self):
        # Test equality filter
        pass
    
    def test_filter_gt(self):
        # Test greater than filter
        pass
```

---

## Complete Example: API Data Source

A data source that reads from a REST API:

```python
"""
REST API Data Source for pipelite
"""

from pipelite.baseobjs.BODataSource import BODataSource
from pipelite.plDataset import plDataset
import pipelite.constants as C
import requests
import pandas as pd


class apiDS(BODataSource):
    """
    Read data from a REST API endpoint.
    
    Parameters:
        url: API endpoint URL
        method: HTTP method (GET, POST)
        headers: Optional headers dict
        params: Optional query parameters
    """
    
    def __init__(self, config, log):
        super().__init__(config, log)
        self.url = C.EMPTY
        self.method = "GET"
        self.headers = {}
        self.params = {}
    
    def initialize(self, cfg) -> bool:
        try:
            self.url = cfg.getParameter("url", C.EMPTY)
            self.method = cfg.getParameter("method", "GET")
            self.headers = cfg.getParameter("headers", {})
            self.params = cfg.getParameter("params", {})
            
            if not self.url:
                raise Exception("url parameter is required")
            
            return True
        except Exception as e:
            self.log.error(f"Init error: {e}")
            return False
    
    def read(self) -> plDataset:
        try:
            self.log.info(f"Calling API: {self.url}")
            
            # Make request
            if self.method.upper() == "GET":
                response = requests.get(
                    self.url, 
                    headers=self.headers,
                    params=self.params
                )
            else:
                response = requests.post(
                    self.url,
                    headers=self.headers,
                    json=self.params
                )
            
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Handle different response structures
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict) and 'data' in data:
                df = pd.DataFrame(data['data'])
            else:
                df = pd.DataFrame([data])
            
            dataset = plDataset(self.config, self.log)
            dataset.set(df)
            
            self.log.info(f"Retrieved {dataset.count} rows")
            return dataset
            
        except Exception as e:
            self.log.error(f"API error: {e}")
            return super().read()
    
    def write(self, dataset) -> bool:
        self.log.error("Write not supported for API data source")
        return False
```

---

## Next Steps

- [Technical Architecture](../technical/architecture.md)
- [API Reference](../technical/api-reference.md)
- [Contributing Guide](../technical/contributing.md)

---

[← Back to How-To Index](index.md) | [Technical Documentation →](../technical/index.md)

