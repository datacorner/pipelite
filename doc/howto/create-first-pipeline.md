# How To: Create Your First Pipeline

This guide walks you through creating a complete data pipeline from scratch, including reading data, applying transformations, and writing results.

---

## Overview

By the end of this guide, you will:
- Understand the structure of a pipelite configuration
- Create a pipeline that reads, transforms, and writes data
- Run and monitor your pipeline execution

---

## Prerequisites

- pipelite installed (`pip install pipelite`)
- A text editor for creating JSON configuration files
- Sample data files (we'll create these)

---

## Step 1: Understand Pipeline Structure

A pipelite pipeline consists of three main components:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  EXTRACTORS │────▶│ TRANSFORMERS│────▶│   LOADERS   │
│  (Read)     │     │  (Process)  │     │  (Write)    │
└─────────────┘     └─────────────┘     └─────────────┘
```

- **Extractors**: Read data from sources (files, databases, APIs)
- **Transformers**: Process and modify the data
- **Loaders**: Write data to destinations

---

## Step 2: Prepare Your Environment

Create a project structure:

```bash
mkdir my_pipeline_project
cd my_pipeline_project
mkdir -p data/input data/output logs
```

Create sample data `data/input/sales.csv`:

```csv
order_id,customer_name,product,quantity,unit_price,order_date
1001,John Smith,Widget A,5,10.99,2024-01-15
1002,Jane Doe,Widget B,3,15.99,2024-01-15
1003,Bob Wilson,Widget A,2,10.99,2024-01-16
1004,Alice Brown,Widget C,1,25.99,2024-01-16
1005,Charlie Davis,Widget B,4,15.99,2024-01-17
```

---

## Step 3: Create a Basic Pipeline

Create `pipeline_basic.json`:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "sales_data",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "sales.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "loaders": [
        {
            "id": "sales_output",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "sales_processed.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "transformers": [
        {
            "id": "process_sales",
            "classname": "pipelite.transformers.passthroughTR",
            "inputs": ["sales_data"],
            "outputs": ["sales_output"]
        }
    ],
    
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

## Step 4: Run the Pipeline

Execute from the command line:

```bash
pipelite -cfg pipeline_basic.json
```

Expected output:

```
Log file: logs/pipeline.log
2024-01-15 10:00:00 INFO|pipelite initialisation ...
2024-01-15 10:00:00 INFO|Pipeline prepared successfully. Flow -> [sales_data -> process_sales -> sales_output]
2024-01-15 10:00:00 INFO|Extract from dataset: sales_data
2024-01-15 10:00:00 INFO|[ EXTRACT sales_data - Rows: 5 ]
2024-01-15 10:00:00 INFO|Apply Transformation: process_sales
2024-01-15 10:00:00 INFO|[ TRANSFORM process_sales - Rows: 5 ]
2024-01-15 10:00:00 INFO|Load from dataset: sales_output
2024-01-15 10:00:00 INFO|[ LOAD sales_output - Rows: 5 ]
2024-01-15 10:00:00 INFO|*** End of Job treatment ***
```

---

## Step 5: Add a Real Transformation

Let's enhance the pipeline to rename columns. Create `pipeline_transform.json`:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "sales_data",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "sales.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "loaders": [
        {
            "id": "sales_output",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "sales_renamed.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "transformers": [
        {
            "id": "rename_columns",
            "classname": "pipelite.transformers.renamecolTR",
            "inputs": ["sales_data"],
            "outputs": ["sales_output"],
            "parameters": {
                "columns": [
                    {"oldname": "order_id", "newname": "OrderID"},
                    {"oldname": "customer_name", "newname": "CustomerName"},
                    {"oldname": "unit_price", "newname": "UnitPrice"},
                    {"oldname": "order_date", "newname": "OrderDate"}
                ]
            }
        }
    ],
    
    "config": {
        "logger": {
            "level": "DEBUG",
            "path": "logs/",
            "filename": "pipeline_transform.log"
        }
    }
}
```

Run it:

```bash
pipelite -cfg pipeline_transform.json
```

Check `data/output/sales_renamed.csv` to see the renamed columns.

---

## Step 6: Chain Multiple Transformations

Create a more complex pipeline with multiple steps. Create `pipeline_chain.json`:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "sales_data",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "sales.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "loaders": [
        {
            "id": "final_output",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ";",
                "filename": "sales_final.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "transformers": [
        {
            "id": "step1_rename",
            "classname": "pipelite.transformers.renamecolTR",
            "inputs": ["sales_data"],
            "outputs": ["intermediate"],
            "parameters": {
                "columns": [
                    {"oldname": "customer_name", "newname": "Customer"}
                ]
            }
        },
        {
            "id": "step2_extract",
            "classname": "pipelite.transformers.extractstrTR",
            "inputs": ["intermediate"],
            "outputs": ["final_output"],
            "parameters": {
                "column": "order_date",
                "start": 0,
                "length": 7
            }
        }
    ],
    
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "pipeline_chain.log"
        }
    }
}
```

The data flows:
1. `sales_data` → `step1_rename` → `intermediate`
2. `intermediate` → `step2_extract` → `final_output`

---

## Complete Example

Here's a complete pipeline that:
1. Reads sales data
2. Renames columns
3. Writes to output with different separator

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "raw_sales",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "sales.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "loaders": [
        {
            "id": "processed_sales",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ";",
                "filename": "sales_processed.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "transformers": [
        {
            "id": "transform",
            "classname": "pipelite.transformers.renamecolTR",
            "inputs": ["raw_sales"],
            "outputs": ["processed_sales"],
            "parameters": {
                "columns": [
                    {"oldname": "order_id", "newname": "ID"},
                    {"oldname": "customer_name", "newname": "Customer"},
                    {"oldname": "product", "newname": "Product"},
                    {"oldname": "quantity", "newname": "Qty"},
                    {"oldname": "unit_price", "newname": "Price"},
                    {"oldname": "order_date", "newname": "Date"}
                ]
            }
        }
    ],
    
    "config": {
        "logger": {
            "level": "INFO",
            "format": "%(asctime)s|%(levelname)s|%(message)s",
            "path": "logs/",
            "filename": "complete_pipeline.log",
            "maxbytes": 5000000
        }
    }
}
```

---

## Troubleshooting

### File Not Found Error

```
ERROR: The file data/input/sales.csv does not exist
```

**Solution**: Check that:
- The file exists at the specified path
- The path is relative to where you run the command
- File permissions allow reading

### Invalid JSON Configuration

```
ERROR: Expecting property name enclosed in double quotes
```

**Solution**: Validate your JSON at [jsonlint.com](https://jsonlint.com/)

### Transformer Input Not Found

```
WARNING: The Transformer X has no input, by pass it!
```

**Solution**: Ensure the input dataset ID matches an extractor ID or a previous transformer's output ID.

---

## Next Steps

- [Connect to a Database](connect-to-database.md)
- [Transform Data](transform-data.md)
- [Profile Your Data](profile-data.md)

---

[← Back to How-To Index](index.md) | [Connect to Database →](connect-to-database.md)

