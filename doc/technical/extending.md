# Extending pipelite

Technical guide for creating custom pipelite components.

---

## Table of Contents

1. [Overview](#overview)
2. [Creating Data Sources](#creating-data-sources)
3. [Creating Transformers](#creating-transformers)
4. [Creating Pipelines](#creating-pipelines)
5. [Parameter Validation](#parameter-validation)
6. [Testing Components](#testing-components)
7. [Packaging](#packaging)

---

## Overview

pipelite is designed for extensibility. You can create:

| Component | Base Class | Purpose |
|-----------|------------|---------|
| Data Source | `BODataSource` | Read/write data |
| Transformer | `BOTransformer` | Process data |
| Pipeline | `BOPipeline` | Orchestrate execution |

### Extension Architecture

```
Your Custom Class
       │
       ▼
┌──────────────┐
│ Base Class   │ ──▶ BODataSource, BOTransformer, BOPipeline
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ plBaseObject │ ──▶ Common functionality
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  plObject    │ ──▶ Config and logging
└──────────────┘
```

---

## Creating Data Sources

### Minimal Implementation

```python
"""my_datasource.py - Custom data source example"""

from pipelite.baseobjs.BODataSource import BODataSource
from pipelite.plDataset import plDataset
import pipelite.constants as C


class myDataSource(BODataSource):
    """Custom data source implementation."""
    
    def __init__(self, config, log):
        super().__init__(config, log)
        # Initialize instance variables
        self.my_param = None
    
    def initialize(self, cfg) -> bool:
        """Initialize from configuration.
        
        Args:
            cfg: Configuration object with getParameter method
            
        Returns:
            True if initialization successful
        """
        try:
            self.my_param = cfg.getParameter("my_param", "default")
            return True
        except Exception as e:
            self.log.error(f"Init failed: {e}")
            return False
    
    def read(self) -> plDataset:
        """Read data into a plDataset.
        
        Returns:
            plDataset containing the data
        """
        try:
            self.log.info("Reading data...")
            
            # Create dataset
            dataset = plDataset(self.config, self.log)
            
            # Load your data (example)
            import pandas as pd
            df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
            dataset.set(df)
            
            return dataset
        except Exception as e:
            self.log.error(f"Read failed: {e}")
            return super().read()
    
    def write(self, dataset) -> bool:
        """Write plDataset to destination.
        
        Args:
            dataset: plDataset to write
            
        Returns:
            True if successful
        """
        try:
            self.log.info("Writing data...")
            
            # Get DataFrame
            df = dataset.get()
            
            # Write your data (example)
            # df.to_csv("output.csv")
            
            return True
        except Exception as e:
            self.log.error(f"Write failed: {e}")
            return False
```

### Full Implementation Example: REST API Source

```python
"""restApiDS.py - REST API data source"""

from pipelite.baseobjs.BODataSource import BODataSource
from pipelite.plDataset import plDataset
import pipelite.constants as C
import requests
import pandas as pd


class restApiDS(BODataSource):
    """
    Data source for REST APIs.
    
    Parameters:
        url (str): API endpoint URL
        method (str): HTTP method (GET, POST)
        headers (dict): Request headers
        params (dict): Query parameters or body
        data_path (str): JSONPath to data in response
        timeout (int): Request timeout in seconds
    """
    
    def __init__(self, config, log):
        super().__init__(config, log)
        self.url = C.EMPTY
        self.method = "GET"
        self.headers = {}
        self.params = {}
        self.data_path = None
        self.timeout = 30
    
    @property
    def parametersValidationFile(self):
        """Return path to validation schema."""
        # Return empty if no schema, or path to JSON schema
        return C.EMPTY
    
    def initialize(self, cfg) -> bool:
        """Initialize with API configuration."""
        try:
            self.url = cfg.getParameter("url", C.EMPTY)
            self.method = cfg.getParameter("method", "GET").upper()
            self.headers = cfg.getParameter("headers", {})
            self.params = cfg.getParameter("params", {})
            self.data_path = cfg.getParameter("data_path", None)
            self.timeout = cfg.getParameter("timeout", 30)
            
            # Validation
            if not self.url:
                raise ValueError("url parameter is required")
            
            if self.method not in ["GET", "POST", "PUT", "DELETE"]:
                raise ValueError(f"Invalid method: {self.method}")
            
            self.log.debug(f"API configured: {self.method} {self.url}")
            return True
            
        except Exception as e:
            self.log.error(f"Initialization failed: {e}")
            return False
    
    def read(self) -> plDataset:
        """Fetch data from API."""
        try:
            self.log.info(f"Calling API: {self.method} {self.url}")
            
            # Make request
            response = self._make_request()
            response.raise_for_status()
            
            # Parse response
            data = response.json()
            
            # Extract data using path if specified
            if self.data_path:
                data = self._extract_data(data, self.data_path)
            
            # Convert to DataFrame
            df = self._to_dataframe(data)
            
            # Create dataset
            dataset = plDataset(self.config, self.log)
            dataset.set(df)
            
            self.log.info(f"Retrieved {dataset.count} rows")
            return dataset
            
        except requests.RequestException as e:
            self.log.error(f"API request failed: {e}")
            return super().read()
        except Exception as e:
            self.log.error(f"Read failed: {e}")
            return super().read()
    
    def write(self, dataset) -> bool:
        """POST data to API."""
        try:
            self.log.info(f"Posting data to API: {self.url}")
            
            # Convert to JSON
            df = dataset.get()
            data = df.to_dict(orient='records')
            
            # Make POST request
            response = requests.post(
                self.url,
                json=data,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            self.log.info(f"Posted {len(data)} records")
            return True
            
        except Exception as e:
            self.log.error(f"Write failed: {e}")
            return False
    
    def _make_request(self):
        """Make HTTP request based on method."""
        if self.method == "GET":
            return requests.get(
                self.url,
                headers=self.headers,
                params=self.params,
                timeout=self.timeout
            )
        elif self.method == "POST":
            return requests.post(
                self.url,
                headers=self.headers,
                json=self.params,
                timeout=self.timeout
            )
        else:
            raise ValueError(f"Unsupported method: {self.method}")
    
    def _extract_data(self, data, path):
        """Extract nested data using dot notation path."""
        for key in path.split('.'):
            if isinstance(data, dict):
                data = data.get(key, data)
            elif isinstance(data, list) and key.isdigit():
                data = data[int(key)]
        return data
    
    def _to_dataframe(self, data):
        """Convert data to DataFrame."""
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        else:
            return pd.DataFrame({'value': [data]})
```

---

## Creating Transformers

### Minimal Implementation

```python
"""my_transformer.py - Custom transformer example"""

from pipelite.baseobjs.BOTransformer import BOTransformer
from pipelite.plDataset import plDataset
from pipelite.plDatasets import plDatasets


class myTransformer(BOTransformer):
    """Custom transformer implementation."""
    
    def __init__(self, config, log):
        super().__init__(config, log)
        self.my_param = None
    
    def initialize(self, params) -> bool:
        """Initialize transformer parameters."""
        try:
            self.my_param = params.getParameter("my_param", "default")
            return True
        except Exception as e:
            self.log.error(f"Init failed: {e}")
            return False
    
    def process(self, dsTransformerInputs) -> plDatasets:
        """Process input datasets.
        
        Args:
            dsTransformerInputs: plDatasets collection
            
        Returns:
            plDatasets collection with outputs
        """
        try:
            self.log.info("Processing data...")
            
            # Get first input (modify as needed)
            input_ds = dsTransformerInputs[0]
            df = input_ds.get().copy()
            
            # Apply transformation
            # ... modify df ...
            
            # Create output dataset
            output = plDataset(self.config, self.log)
            output.set(df)
            output.id = self.dsOutputs[0]  # Set output ID
            
            # Return as collection
            outputs = plDatasets()
            outputs.add(output)
            return outputs
            
        except Exception as e:
            self.log.error(f"Process failed: {e}")
            return dsTransformerInputs
```

### Full Implementation Example: Filter Transformer

```python
"""filterTR.py - Row filter transformer"""

from pipelite.baseobjs.BOTransformer import BOTransformer
from pipelite.plDataset import plDataset
from pipelite.plDatasets import plDatasets
import pipelite.constants as C


class filterTR(BOTransformer):
    """
    Filter rows based on column conditions.
    
    Parameters:
        column (str): Column to filter on
        operator (str): Comparison operator
        value: Value to compare against
        keep_nulls (bool): Whether to keep null values
    
    Operators:
        eq: Equal to
        ne: Not equal to
        gt: Greater than
        lt: Less than
        gte: Greater than or equal
        lte: Less than or equal
        contains: String contains
        startswith: String starts with
        endswith: String ends with
        isnull: Value is null
        notnull: Value is not null
    """
    
    OPERATORS = {
        'eq': lambda col, val: col == val,
        'ne': lambda col, val: col != val,
        'gt': lambda col, val: col > val,
        'lt': lambda col, val: col < val,
        'gte': lambda col, val: col >= val,
        'lte': lambda col, val: col <= val,
        'contains': lambda col, val: col.str.contains(str(val), na=False),
        'startswith': lambda col, val: col.str.startswith(str(val), na=False),
        'endswith': lambda col, val: col.str.endswith(str(val), na=False),
        'isnull': lambda col, val: col.isnull(),
        'notnull': lambda col, val: col.notnull(),
    }
    
    def __init__(self, config, log):
        super().__init__(config, log)
        self.column = C.EMPTY
        self.operator = "eq"
        self.value = None
        self.keep_nulls = False
    
    @property
    def parametersValidationFile(self):
        return C.EMPTY  # Add schema path if available
    
    def initialize(self, params) -> bool:
        """Initialize filter parameters."""
        try:
            self.column = params.getParameter("column", C.EMPTY)
            self.operator = params.getParameter("operator", "eq")
            self.value = params.getParameter("value", None)
            self.keep_nulls = params.getParameter("keep_nulls", False)
            
            # Validate
            if not self.column:
                raise ValueError("column parameter is required")
            
            if self.operator not in self.OPERATORS:
                raise ValueError(f"Unknown operator: {self.operator}. "
                               f"Valid: {list(self.OPERATORS.keys())}")
            
            if self.operator not in ['isnull', 'notnull'] and self.value is None:
                raise ValueError("value parameter required for this operator")
            
            self.log.debug(f"Filter: {self.column} {self.operator} {self.value}")
            return True
            
        except Exception as e:
            self.log.error(f"Initialization failed: {e}")
            return False
    
    def process(self, dsTransformerInputs) -> plDatasets:
        """Apply filter to input dataset."""
        try:
            # Get input
            input_ds = dsTransformerInputs[0]
            df = input_ds.get().copy()
            
            original_count = len(df)
            self.log.info(f"Filtering {original_count} rows: "
                         f"{self.column} {self.operator} {self.value}")
            
            # Validate column exists
            if self.column not in df.columns:
                raise ValueError(f"Column not found: {self.column}")
            
            # Get operator function
            op_func = self.OPERATORS[self.operator]
            
            # Apply filter
            mask = op_func(df[self.column], self.value)
            
            # Handle nulls
            if self.keep_nulls and self.operator not in ['isnull', 'notnull']:
                null_mask = df[self.column].isnull()
                mask = mask | null_mask
            
            # Filter DataFrame
            filtered_df = df[mask]
            
            filtered_count = len(filtered_df)
            self.log.info(f"Filtered from {original_count} to {filtered_count} rows "
                         f"({original_count - filtered_count} removed)")
            
            # Create output
            output = plDataset(self.config, self.log)
            output.set(filtered_df)
            output.id = self.dsOutputs[0]
            
            outputs = plDatasets()
            outputs.add(output)
            return outputs
            
        except Exception as e:
            self.log.error(f"Filter failed: {e}")
            return dsTransformerInputs
```

---

## Creating Pipelines

### Custom Pipeline Example

```python
"""parallelPL.py - Hypothetical parallel pipeline"""

from pipelite.baseobjs.BOPipeline import BOPipeline
from pipelite.utils.plReports import plReports
from concurrent.futures import ThreadPoolExecutor


class parallelPL(BOPipeline):
    """Pipeline with parallel extraction."""
    
    def __init__(self, config, log):
        super().__init__(config, log)
        self.max_workers = 4
    
    def prepare(self) -> bool:
        """Prepare execution plan."""
        try:
            # Custom preparation logic
            return True
        except Exception as e:
            self.log.error(f"Prepare failed: {e}")
            return False
    
    def execute(self) -> plReports:
        """Execute with parallel extraction."""
        try:
            # Parallel extraction
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(e.read) for e in self.extractors]
                datasets = [f.result() for f in futures]
            
            # Sequential transformation and loading
            # ... implement as needed ...
            
            return self.report
        except Exception as e:
            self.log.error(f"Execute failed: {e}")
            return plReports()
    
    def terminate(self) -> bool:
        """Cleanup."""
        self.log.info("Pipeline completed")
        return True
    
    def beforeProcess(self) -> bool:
        return True
    
    def afterProcess(self) -> bool:
        return True
```

---

## Parameter Validation

### JSON Schema Validation

Create a schema file `schemas/filter_schema.json`:

```json
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Filter Transformer Parameters",
    "type": "object",
    "required": ["column", "operator"],
    "properties": {
        "column": {
            "type": "string",
            "description": "Column name to filter on",
            "minLength": 1
        },
        "operator": {
            "type": "string",
            "enum": ["eq", "ne", "gt", "lt", "gte", "lte", "contains", "startswith", "endswith", "isnull", "notnull"],
            "description": "Comparison operator"
        },
        "value": {
            "description": "Value to compare against"
        },
        "keep_nulls": {
            "type": "boolean",
            "default": false,
            "description": "Keep rows with null values"
        }
    }
}
```

Reference in your component:

```python
@property
def parametersValidationFile(self):
    return "schemas/filter_schema.json"
```

---

## Testing Components

### Unit Test Example

```python
"""test_filter.py - Tests for filter transformer"""

import unittest
import pandas as pd
from my_transformers.filterTR import filterTR
from pipelite.plDataset import plDataset
from pipelite.plDatasets import plDatasets


class MockConfig:
    def __init__(self, params):
        self.params = params
    def getParameter(self, name, default=None):
        return self.params.get(name, default)


class MockLog:
    def info(self, *msg): pass
    def debug(self, *msg): pass
    def error(self, *msg): print("ERROR:", *msg)
    def warning(self, *msg): pass


class TestFilterTR(unittest.TestCase):
    
    def setUp(self):
        self.log = MockLog()
        self.config = MockConfig({})
    
    def create_dataset(self, data):
        ds = plDataset(self.config, self.log)
        ds.set(pd.DataFrame(data))
        ds.id = "test"
        return ds
    
    def test_filter_eq(self):
        # Setup
        tr = filterTR(self.config, self.log)
        tr.dsOutputs = ["output"]
        params = MockConfig({"column": "status", "operator": "eq", "value": "active"})
        tr.initialize(params)
        
        # Input
        inputs = plDatasets()
        inputs.add(self.create_dataset({
            "id": [1, 2, 3],
            "status": ["active", "inactive", "active"]
        }))
        
        # Execute
        outputs = tr.process(inputs)
        
        # Assert
        self.assertEqual(outputs[0].count, 2)
    
    def test_filter_gt(self):
        tr = filterTR(self.config, self.log)
        tr.dsOutputs = ["output"]
        params = MockConfig({"column": "value", "operator": "gt", "value": 50})
        tr.initialize(params)
        
        inputs = plDatasets()
        inputs.add(self.create_dataset({
            "id": [1, 2, 3],
            "value": [25, 75, 100]
        }))
        
        outputs = tr.process(inputs)
        
        self.assertEqual(outputs[0].count, 2)


if __name__ == '__main__':
    unittest.main()
```

---

## Packaging

### Project Structure

```
my_pipelite_extensions/
├── setup.py
├── README.md
├── my_datasources/
│   ├── __init__.py
│   └── restApiDS.py
├── my_transformers/
│   ├── __init__.py
│   └── filterTR.py
├── schemas/
│   └── filter_schema.json
└── tests/
    ├── __init__.py
    └── test_filter.py
```

### setup.py

```python
from setuptools import setup, find_packages

setup(
    name="my-pipelite-extensions",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pipelite>=0.1.9",
        "requests>=2.28.0",
    ],
    package_data={
        "": ["schemas/*.json"],
    },
)
```

### Install and Use

```bash
pip install -e .
```

```json
{
    "classname": "my_datasources.restApiDS",
    "parameters": {...}
}
```

---

## Next Steps

- [API Reference](api-reference.md)
- [Contributing](contributing.md)
- [Architecture](architecture.md)

---

[← Back to Technical Index](index.md) | [Contributing →](contributing.md)

