# Configuration Reference

Complete reference for pipelite configuration options.

---

## Table of Contents

1. [Configuration Overview](#configuration-overview)
2. [Pipeline Configuration](#pipeline-configuration)
3. [Extractor Configuration](#extractor-configuration)
4. [Loader Configuration](#loader-configuration)
5. [Transformer Configuration](#transformer-configuration)
6. [Logger Configuration](#logger-configuration)
7. [Validation](#validation)
8. [Best Practices](#best-practices)

---

## Configuration Overview

pipelite uses JSON configuration files to define data pipelines. A configuration file specifies:

- **Pipeline type**: How the pipeline executes
- **Extractors**: Data sources to read from
- **Loaders**: Destinations to write to
- **Transformers**: Data processing steps
- **Logger**: Logging configuration

### Basic Structure

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

---

## Pipeline Configuration

### Top-Level Properties

| Property | Type | Required | Description |
|----------|------|:--------:|-------------|
| `classname` | string | ✅ | Full path to pipeline class |
| `extractors` | array | ✅ | List of data sources to read |
| `loaders` | array | ✅ | List of data destinations |
| `transformers` | array | ✅ | List of transformations |
| `parameters` | object | ❌ | Pipeline-level parameters |
| `config` | object | ❌ | Configuration settings |

### Available Pipeline Classes

| Class | Description |
|-------|-------------|
| `pipelite.pipelines.sequentialPL` | Sequential execution based on dependencies |

### Example

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    "extractors": [],
    "loaders": [],
    "transformers": [],
    "parameters": {
        "mode": "full"
    },
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "pipeline.log"
        }
    }
}
```

---

## Extractor Configuration

Extractors read data from sources. They share the same configuration structure as loaders for the same data source type.

### Properties

| Property | Type | Required | Description |
|----------|------|:--------:|-------------|
| `id` | string | ✅ | Unique identifier (used for data flow) |
| `classname` | string | ✅ | Full path to data source class |
| `parameters` | object | ✅ | Data source specific parameters |
| `validation` | string | ❌ | Path to JSON schema for validation |

### Example

```json
{
    "extractors": [
        {
            "id": "source_data",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "data.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        }
    ]
}
```

### Available Data Sources

See [Data Sources](datasources.md) for complete list and parameters.

---

## Loader Configuration

Loaders write data to destinations. They use the same classes as extractors.

### Properties

| Property | Type | Required | Description |
|----------|------|:--------:|-------------|
| `id` | string | ✅ | Unique identifier (matches transformer output) |
| `classname` | string | ✅ | Full path to data source class |
| `parameters` | object | ✅ | Data source specific parameters |
| `validation` | string | ❌ | Path to JSON schema for validation |

### Example

```json
{
    "loaders": [
        {
            "id": "output_data",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ";",
                "filename": "output.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ]
}
```

---

## Transformer Configuration

Transformers process data between extractors and loaders.

### Properties

| Property | Type | Required | Description |
|----------|------|:--------:|-------------|
| `id` | string | ✅ | Unique identifier |
| `classname` | string | ✅ | Full path to transformer class |
| `inputs` | array | ✅ | List of input dataset IDs |
| `outputs` | array | ✅ | List of output dataset IDs |
| `parameters` | object | ❌ | Transformer specific parameters |
| `validation` | string | ❌ | Path to JSON schema for validation |

### Example

```json
{
    "transformers": [
        {
            "id": "transform_step",
            "classname": "pipelite.transformers.renamecolTR",
            "inputs": ["source_data"],
            "outputs": ["output_data"],
            "parameters": {
                "columns": [
                    {"oldname": "col1", "newname": "column1"}
                ]
            }
        }
    ]
}
```

### Data Flow

The `inputs` and `outputs` arrays define data flow:

```
extractors[id="A"] 
        ↓
transformers[inputs=["A"], outputs=["B"]]
        ↓
loaders[id="B"]
```

### Chaining Transformers

```json
{
    "transformers": [
        {
            "id": "step1",
            "inputs": ["source"],
            "outputs": ["intermediate"]
        },
        {
            "id": "step2",
            "inputs": ["intermediate"],
            "outputs": ["final"]
        }
    ]
}
```

See [Transformers](transformers.md) for complete list and parameters.

---

## Logger Configuration

Configure logging behavior for pipeline execution.

### Properties

| Property | Type | Required | Default | Description |
|----------|------|:--------:|---------|-------------|
| `level` | string | ❌ | `DEBUG` | Log level |
| `format` | string | ❌ | See below | Log message format |
| `path` | string | ❌ | `./` | Directory for log file |
| `filename` | string | ❌ | `pipelite.log` | Log file name |
| `maxbytes` | int | ❌ | `1000000` | Max log file size (rotation) |

### Log Levels

| Level | Description |
|-------|-------------|
| `DEBUG` | Detailed debug information |
| `INFO` | General operational messages |
| `WARNING` | Warning conditions |
| `ERROR` | Error conditions only |

### Default Format

```
%(asctime)s|%(name)s|%(levelname)s|%(message)s
```

### Format Placeholders

| Placeholder | Description |
|-------------|-------------|
| `%(asctime)s` | Timestamp |
| `%(name)s` | Logger name |
| `%(levelname)s` | Log level |
| `%(message)s` | Log message |

### Example

```json
{
    "config": {
        "logger": {
            "level": "INFO",
            "format": "%(asctime)s|%(levelname)s|%(message)s",
            "path": "/var/log/pipelite/",
            "filename": "etl_pipeline.log",
            "maxbytes": 5000000
        }
    }
}
```

---

## Validation

pipelite validates configurations using JSON Schema.

### Built-in Validation

Every extractor, loader, and transformer can have a validation schema.

### Custom Validation Schema

Specify a validation schema in the configuration:

```json
{
    "id": "my_component",
    "classname": "my.custom.Component",
    "validation": "schemas/my_component_schema.json",
    "parameters": {...}
}
```

### Schema Example

```json
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["filename", "path"],
    "properties": {
        "filename": {
            "type": "string",
            "description": "Name of the file"
        },
        "path": {
            "type": "string",
            "description": "Directory path"
        },
        "encoding": {
            "type": "string",
            "default": "utf-8"
        }
    }
}
```

---

## Best Practices

### 1. Use Descriptive IDs

```json
// Good
"id": "customer_orders_2024"

// Bad
"id": "data1"
```

### 2. Organize Configuration Files

```
project/
├── config/
│   ├── pipelines/
│   │   ├── daily_etl.json
│   │   └── weekly_report.json
│   └── schemas/
│       └── custom_schema.json
├── data/
│   ├── input/
│   └── output/
└── logs/
```

### 3. Use Relative Paths

```json
// Good - portable
"path": "data/input/"

// Bad - system specific
"path": "/home/user/project/data/input/"
```

### 4. Configure Appropriate Logging

- **Development**: `DEBUG` level
- **Production**: `INFO` or `WARNING` level

### 5. Validate JSON Before Running

Use a JSON validator or:

```bash
python -m json.tool pipeline.json > /dev/null && echo "Valid JSON"
```

### 6. Comment Complex Configurations

JSON doesn't support comments, but you can use a documentation field:

```json
{
    "_description": "This pipeline processes daily sales data",
    "classname": "pipelite.pipelines.sequentialPL",
    ...
}
```

---

## Complete Example

```json
{
    "_description": "Daily sales ETL pipeline",
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "raw_sales",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "sales_{{date}}.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        },
        {
            "id": "product_catalog",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "products.csv",
                "path": "data/reference/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "loaders": [
        {
            "id": "enriched_sales",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "sales_enriched.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "transformers": [
        {
            "id": "join_products",
            "classname": "pipelite.transformers.joinTR",
            "inputs": ["raw_sales", "product_catalog"],
            "outputs": ["enriched_sales"],
            "parameters": {
                "on": "product_id",
                "how": "left"
            }
        }
    ],
    
    "parameters": {
        "mode": "incremental",
        "date": "2024-01-15"
    },
    
    "config": {
        "logger": {
            "level": "INFO",
            "format": "%(asctime)s|%(levelname)s|%(message)s",
            "path": "logs/",
            "filename": "daily_etl.log",
            "maxbytes": 10000000
        }
    }
}
```

---

## Next Steps

- [Data Sources](datasources.md)
- [Transformers](transformers.md)
- [Logging](logging.md)

---

[← Back to Guide Index](index.md) | [Data Sources →](datasources.md)

