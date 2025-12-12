# Quick Start Guide

Get up and running with pipelite in just a few minutes. This guide will walk you through installing pipelite and creating your first data pipeline.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Your First Pipeline](#your-first-pipeline)
4. [Running the Pipeline](#running-the-pipeline)
5. [Understanding the Output](#understanding-the-output)
6. [Next Steps](#next-steps)

---

## Prerequisites

Before you begin, ensure you have:

- **Python 3.10 or higher** installed on your system
- **pip** package manager (usually comes with Python)
- Basic familiarity with JSON syntax
- A terminal or command prompt

To check your Python version:

```bash
python --version
# or
python3 --version
```

---

## Installation

### Option 1: Install from PyPI (Recommended)

```bash
pip install pipelite
```

### Option 2: Install from Source

```bash
git clone https://github.com/datacorner/pipelite.git
cd pipelite
pip install -e .
```

### Verify Installation

```bash
pipelite --help
```

You should see the help message with available options.

---

## Your First Pipeline

Let's create a simple pipeline that reads a CSV file and writes it to another location.

### Step 1: Create Sample Data

Create a folder structure and sample data file:

```bash
mkdir -p data/input data/output logs
```

Create `data/input/employees.csv`:

```csv
id,name,department,salary
1,Alice,Engineering,75000
2,Bob,Marketing,65000
3,Charlie,Engineering,80000
4,Diana,Sales,70000
5,Eve,Marketing,68000
```

### Step 2: Create the Pipeline Configuration

Create `my_first_pipeline.json`:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    "extractors": [
        {
            "id": "employees_source",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "employees.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        }
    ],
    "loaders": [
        {
            "id": "employees_target",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "employees_copy.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    "transformers": [
        {
            "id": "passthrough",
            "classname": "pipelite.transformers.passthroughTR",
            "inputs": ["employees_source"],
            "outputs": ["employees_target"]
        }
    ],
    "config": {
        "logger": {
            "level": "INFO",
            "format": "%(asctime)s|%(name)s|%(levelname)s|%(message)s",
            "path": "logs/",
            "filename": "my_first_pipeline.log",
            "maxbytes": 1000000
        }
    }
}
```

---

## Running the Pipeline

Execute your pipeline from the command line:

```bash
pipelite -cfg my_first_pipeline.json
```

---

## Understanding the Output

### Console Output

You'll see output similar to:

```
Log file: logs/my_first_pipeline.log
2024-01-15 10:30:00 INFO|pipelite initialisation ...
2024-01-15 10:30:00 INFO|Class instantiated successfully
2024-01-15 10:30:00 INFO|Pipeline prepared successfully. Flow -> [employees_source -> passthrough -> employees_target]
2024-01-15 10:30:00 INFO|Extract from dataset: employees_source
2024-01-15 10:30:00 INFO|[ EXTRACT employees_source - Rows: 5 - Columns: Index(['id', 'name', 'department', 'salary'], dtype='object') ]
2024-01-15 10:30:00 INFO|Apply Transformation: passthrough
2024-01-15 10:30:00 INFO|[ TRANSFORM passthrough - Rows: 5 ]
2024-01-15 10:30:00 INFO|Load from dataset: employees_target
2024-01-15 10:30:00 INFO|[ LOAD employees_target - Rows: 5 ]
2024-01-15 10:30:00 INFO|*** End of Job treatment ***
```

### Pipeline Report

At the end of execution, you'll see a summary report:

```
--- PIPELITE REPORT ---
                   Type                              Description  Rows Processed
id                                                                              
employees_source   Extractor  csvFileDS -> Output: [employees_source]           5
employees_target   Loader     csvFileDS -> Input: [employees_target]            5
passthrough        Transformer  passthroughTR -> Inputs/Outputs                  5

Total Duration 0.002 sec
```

### Output File

Check `data/output/employees_copy.csv` - it should contain the same data as your input file.

---

## Next Steps

Congratulations! You've created and run your first pipelite pipeline. Here's what to explore next:

### Learn More

- **[How-To Guides](howto/index.md)**: Step-by-step guides for common tasks
- **[Complete Guide](guide/index.md)**: In-depth documentation
- **[Configuration Reference](guide/configuration.md)**: All configuration options

### Try These Examples

1. **Transform Data**: Add a [column rename transformer](guide/transformers.md#rename-column)
2. **Join Data**: Combine multiple CSV files with [join transformer](guide/transformers.md#join)
3. **Profile Data**: Generate a [data profile report](guide/transformers.md#profile)
4. **Connect to Database**: Read from an [ODBC data source](guide/datasources.md#odbc)

### Example: Adding a Transformation

Modify your pipeline to rename columns:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    "extractors": [
        {
            "id": "source",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "employees.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        }
    ],
    "loaders": [
        {
            "id": "target",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "employees_renamed.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    "transformers": [
        {
            "id": "rename",
            "classname": "pipelite.transformers.renamecolTR",
            "inputs": ["source"],
            "outputs": ["target"],
            "parameters": {
                "columns": [
                    {"oldname": "name", "newname": "employee_name"},
                    {"oldname": "salary", "newname": "annual_salary"}
                ]
            }
        }
    ],
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "rename_pipeline.log"
        }
    }
}
```

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| `FileNotFoundError` | Check that input file paths are correct relative to where you run the command |
| `No module named 'pipelite'` | Ensure pipelite is installed: `pip install pipelite` |
| `Permission denied` for log file | Ensure the logs directory exists and is writable |

### Getting Help

If you encounter issues:

1. Check the log file for detailed error messages
2. Verify your JSON configuration is valid
3. Visit [GitHub Issues](https://github.com/datacorner/pipelite/issues) for known issues
4. Ask questions in [GitHub Discussions](https://github.com/datacorner/pipelite/discussions)

---

[← Back to Documentation Index](index.md) | [How-To Guides →](howto/index.md)

