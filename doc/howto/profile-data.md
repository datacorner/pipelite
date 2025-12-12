# How To: Profile Your Data

This guide shows you how to use pipelite's built-in data profiling capabilities to understand your datasets.

---

## Overview

Data profiling generates statistics and insights about your data, including:
- Column data types and counts
- Null/missing value analysis
- Value distributions
- Basic statistics (min, max, mean, etc.)

By the end of this guide, you will:
- Generate JSON profile reports
- Create HTML profile dashboards
- Analyze multiple datasets

---

## Prerequisites

- pipelite installed
- Sample data to profile

---

## Step 1: Basic Data Profiling

Create a profile of a single dataset.

### Sample Data (`data.csv`)

```csv
id,name,email,age,salary,department,hire_date
1,Alice Smith,alice@example.com,28,75000,Engineering,2020-03-15
2,Bob Jones,bob@example.com,35,85000,Marketing,2019-06-20
3,Charlie Brown,,42,95000,Engineering,2018-01-10
4,Diana Lee,diana@example.com,31,72000,Sales,2021-09-01
5,Eve Wilson,eve@example.com,29,,Marketing,2020-11-15
```

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "employees",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "data.csv",
            "path": "data/input/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [{
        "id": "profile",
        "classname": "pipelite.transformers.profileTR",
        "inputs": ["employees"],
        "outputs": [],
        "parameters": {
            "output_file": "data/output/employees.profile",
            "max_value_counts": 10
        }
    }],
    
    "loaders": [],
    
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "profile.log"
        }
    }
}
```

### Run the Pipeline

```bash
pipelite -cfg profile_pipeline.json
```

---

## Step 2: Understanding the Profile Output

The profiler generates a JSON file with detailed statistics.

### Sample Profile Output

```json
{
    "dataset": "employees",
    "row_count": 5,
    "column_count": 7,
    "columns": [
        {
            "name": "id",
            "dtype": "int64",
            "count": 5,
            "null_count": 0,
            "null_percent": 0.0,
            "unique_count": 5,
            "min": 1,
            "max": 5,
            "mean": 3.0
        },
        {
            "name": "name",
            "dtype": "object",
            "count": 5,
            "null_count": 0,
            "null_percent": 0.0,
            "unique_count": 5,
            "value_counts": {
                "Alice Smith": 1,
                "Bob Jones": 1,
                "Charlie Brown": 1,
                "Diana Lee": 1,
                "Eve Wilson": 1
            }
        },
        {
            "name": "email",
            "dtype": "object",
            "count": 5,
            "null_count": 1,
            "null_percent": 20.0,
            "unique_count": 4
        },
        {
            "name": "salary",
            "dtype": "float64",
            "count": 5,
            "null_count": 1,
            "null_percent": 20.0,
            "unique_count": 4,
            "min": 72000.0,
            "max": 95000.0,
            "mean": 81750.0
        }
    ]
}
```

---

## Step 3: Generate HTML Reports

Create visual profile reports using the HTML template.

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "data",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "data.csv",
            "path": "data/input/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [{
        "id": "profile",
        "classname": "pipelite.transformers.profileTR",
        "inputs": ["data"],
        "outputs": [],
        "parameters": {
            "output_file": "data/output/profile.json",
            "html_output": "data/output/profile.html",
            "max_value_counts": 20
        }
    }],
    
    "loaders": [],
    
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "profile_html.log"
        }
    }
}
```

---

## Step 4: Profile Multiple Datasets

Profile multiple datasets in a single pipeline.

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "customers",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "customers.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        },
        {
            "id": "orders",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "orders.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "transformers": [
        {
            "id": "profile_customers",
            "classname": "pipelite.transformers.profileTR",
            "inputs": ["customers"],
            "outputs": [],
            "parameters": {
                "output_file": "data/output/customers.profile"
            }
        },
        {
            "id": "profile_orders",
            "classname": "pipelite.transformers.profileTR",
            "inputs": ["orders"],
            "outputs": [],
            "parameters": {
                "output_file": "data/output/orders.profile"
            }
        }
    ],
    
    "loaders": [],
    
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "multi_profile.log"
        }
    }
}
```

---

## Step 5: Profile and Transform

Combine profiling with other transformations.

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "raw_data",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "raw_data.csv",
            "path": "data/input/",
            "encoding": "utf-8"
        }
    }],
    
    "loaders": [{
        "id": "clean_data",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "clean_data.csv",
            "path": "data/output/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [
        {
            "id": "profile_before",
            "classname": "pipelite.transformers.profileTR",
            "inputs": ["raw_data"],
            "outputs": ["profiled_raw"],
            "parameters": {
                "output_file": "data/output/before_profile.json"
            }
        },
        {
            "id": "clean",
            "classname": "pipelite.transformers.renamecolTR",
            "inputs": ["profiled_raw"],
            "outputs": ["cleaned"],
            "parameters": {
                "columns": [
                    {"oldname": "old_name", "newname": "new_name"}
                ]
            }
        },
        {
            "id": "profile_after",
            "classname": "pipelite.transformers.profileTR",
            "inputs": ["cleaned"],
            "outputs": ["clean_data"],
            "parameters": {
                "output_file": "data/output/after_profile.json"
            }
        }
    ],
    
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "profile_transform.log"
        }
    }
}
```

---

## Profile Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `output_file` | string | Path for JSON profile output | Required |
| `html_output` | string | Path for HTML report | None |
| `max_value_counts` | int | Max distinct values to show | 10 |

---

## Understanding Profile Metrics

### Per-Column Metrics

| Metric | Description |
|--------|-------------|
| `dtype` | Data type (int64, float64, object, datetime64, etc.) |
| `count` | Total number of values |
| `null_count` | Number of null/missing values |
| `null_percent` | Percentage of null values |
| `unique_count` | Number of distinct values |
| `min` / `max` | Minimum and maximum values (numeric) |
| `mean` | Average value (numeric) |
| `value_counts` | Distribution of values |

---

## Complete Example

Profile a dataset and generate both JSON and HTML reports:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "sales",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "sales_2024.csv",
            "path": "data/input/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [{
        "id": "profile",
        "classname": "pipelite.transformers.profileTR",
        "inputs": ["sales"],
        "outputs": [],
        "parameters": {
            "output_file": "reports/sales_profile.json",
            "html_output": "reports/sales_profile.html",
            "max_value_counts": 25
        }
    }],
    
    "loaders": [],
    
    "config": {
        "logger": {
            "level": "DEBUG",
            "path": "logs/",
            "filename": "sales_profile.log"
        }
    }
}
```

---

## Troubleshooting

### Profile File Not Created

**Solution**: 
- Ensure the output directory exists
- Check file permissions
- Verify the path is correct

### Memory Issues with Large Datasets

**Solution**:
- Reduce `max_value_counts` parameter
- Profile a sample of data instead of the full dataset

---

## Next Steps

- [Transform Data](transform-data.md)
- [Extend pipelite](extend-pipelite.md)
- [Technical Documentation](../technical/index.md)

---

[← Back to How-To Index](index.md) | [Extend pipelite →](extend-pipelite.md)

