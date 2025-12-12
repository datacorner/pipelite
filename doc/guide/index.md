# Complete Guide

Welcome to the pipelite Complete Guide. This comprehensive documentation covers all aspects of the framework in detail.

---

## Guide Contents

### Getting Started

| Chapter | Description |
|---------|-------------|
| [Installation](installation.md) | Installing pipelite and its dependencies |
| [Configuration](configuration.md) | Complete configuration reference |

### Core Components

| Chapter | Description |
|---------|-------------|
| [Data Sources](datasources.md) | All available data sources and their parameters |
| [Transformers](transformers.md) | All available transformers and usage examples |
| [Pipelines](pipelines.md) | Pipeline types and execution strategies |

### Operations

| Chapter | Description |
|---------|-------------|
| [Logging](logging.md) | Logging configuration and monitoring |
| [Error Handling](error-handling.md) | Understanding and handling errors |
| [Best Practices](best-practices.md) | Recommended patterns and practices |

---

## Quick Reference

### Pipeline Structure

Every pipelite pipeline follows this structure:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    "extractors": [...],
    "loaders": [...],
    "transformers": [...],
    "parameters": {...},
    "config": {
        "logger": {...}
    }
}
```

### Data Flow

```
Extractors → Transformers → Loaders
   (Read)     (Process)     (Write)
```

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Extractor** | Reads data from a source into a dataset |
| **Transformer** | Processes and modifies datasets |
| **Loader** | Writes datasets to a destination |
| **Dataset** | In-memory data container (wraps pandas DataFrame) |
| **Pipeline** | Orchestrates execution order |

---

## Configuration Quick Reference

### Extractor/Loader Structure

```json
{
    "id": "unique_identifier",
    "classname": "module.path.ClassName",
    "parameters": {
        "param1": "value1"
    }
}
```

### Transformer Structure

```json
{
    "id": "unique_identifier",
    "classname": "module.path.ClassName",
    "inputs": ["input_dataset_id"],
    "outputs": ["output_dataset_id"],
    "parameters": {
        "param1": "value1"
    }
}
```

### Logger Configuration

```json
{
    "config": {
        "logger": {
            "level": "DEBUG|INFO|WARNING|ERROR",
            "format": "%(asctime)s|%(name)s|%(levelname)s|%(message)s",
            "path": "./logs/",
            "filename": "pipeline.log",
            "maxbytes": 1000000
        }
    }
}
```

---

## Available Components

### Data Sources

| Class | Read | Write | Description |
|-------|:----:|:-----:|-------------|
| `csvFileDS` | ✅ | ✅ | CSV files |
| `excelFileDS` | ✅ | ❌ | Excel spreadsheets |
| `parquetFileDS` | ✅ | ✅ | Parquet files |
| `odbcDS` | ✅ | ❌ | ODBC databases |
| `xesFileDS` | ✅ | ❌ | XES event logs |
| `sapRfcTableDS` | ✅ | ❌ | SAP tables via RFC |
| `piDS` | ❌ | ✅ | ABBYY Timeline |

### Transformers

| Class | Description |
|-------|-------------|
| `passthroughTR` | Pass data through (rename datasets) |
| `concatTR` | Concatenate datasets |
| `joinTR` | Join datasets on columns |
| `lookupTR` | Lookup and enrich data |
| `extractstrTR` | Extract substrings |
| `renamecolTR` | Rename columns |
| `jinjaTR` | Jinja2 template transforms |
| `profileTR` | Data profiling |

---

## Navigation

- **[Installation →](installation.md)**: Start here to set up pipelite
- **[Configuration →](configuration.md)**: Learn the configuration format
- **[Data Sources →](datasources.md)**: Explore available data sources
- **[Transformers →](transformers.md)**: Discover transformation options

---

[← Back to Documentation Index](../index.md) | [Installation →](installation.md)

