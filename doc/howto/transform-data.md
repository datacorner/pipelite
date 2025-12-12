# How To: Transform Data

This guide covers common data transformation patterns using pipelite's built-in transformers.

---

## Overview

By the end of this guide, you will:
- Understand available transformers and their use cases
- Chain multiple transformations together
- Handle common data transformation scenarios

---

## Prerequisites

- pipelite installed
- Sample data files to work with

---

## Available Transformers

| Transformer | Use Case |
|-------------|----------|
| `passthroughTR` | Rename dataset IDs without modification |
| `renamecolTR` | Rename column headers |
| `extractstrTR` | Extract substrings from column values |
| `concatTR` | Combine multiple datasets vertically |
| `joinTR` | Join datasets on common columns |
| `lookupTR` | Enrich data with lookup table values |
| `jinjaTR` | Transform columns using Jinja2 templates |
| `profileTR` | Generate data profiling reports |

---

## Pattern 1: Rename Columns

Standardize column names across datasets.

### Input Data (`sales.csv`)

```csv
cust_id,prod_name,qty,price
1,Widget,5,10.99
2,Gadget,3,25.99
```

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "sales",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "sales.csv",
            "path": "data/input/",
            "encoding": "utf-8"
        }
    }],
    
    "loaders": [{
        "id": "sales_clean",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "sales_clean.csv",
            "path": "data/output/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [{
        "id": "rename",
        "classname": "pipelite.transformers.renamecolTR",
        "inputs": ["sales"],
        "outputs": ["sales_clean"],
        "parameters": {
            "columns": [
                {"oldname": "cust_id", "newname": "customer_id"},
                {"oldname": "prod_name", "newname": "product_name"},
                {"oldname": "qty", "newname": "quantity"},
                {"oldname": "price", "newname": "unit_price"}
            ]
        }
    }],
    
    "config": {
        "logger": {"level": "INFO", "path": "logs/", "filename": "rename.log"}
    }
}
```

### Output

```csv
customer_id,product_name,quantity,unit_price
1,Widget,5,10.99
2,Gadget,3,25.99
```

---

## Pattern 2: Extract Substrings

Extract parts of string values (e.g., year from date, code from ID).

### Input Data (`orders.csv`)

```csv
order_id,order_date,status
ORD-2024-001,2024-01-15,COMPLETED
ORD-2024-002,2024-02-20,PENDING
ORD-2024-003,2024-03-10,COMPLETED
```

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "orders",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "orders.csv",
            "path": "data/input/",
            "encoding": "utf-8"
        }
    }],
    
    "loaders": [{
        "id": "orders_processed",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "orders_processed.csv",
            "path": "data/output/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [{
        "id": "extract_year",
        "classname": "pipelite.transformers.extractstrTR",
        "inputs": ["orders"],
        "outputs": ["orders_processed"],
        "parameters": {
            "column": "order_date",
            "start": 0,
            "length": 4
        }
    }],
    
    "config": {
        "logger": {"level": "INFO", "path": "logs/", "filename": "extract.log"}
    }
}
```

### Output

The `order_date` column now contains only the year:

```csv
order_id,order_date,status
ORD-2024-001,2024,COMPLETED
ORD-2024-002,2024,PENDING
ORD-2024-003,2024,COMPLETED
```

---

## Pattern 3: Concatenate Datasets

Combine multiple datasets with the same structure.

### Input Data

**file1.csv**
```csv
id,name,value
1,Alpha,100
2,Beta,200
```

**file2.csv**
```csv
id,name,value
3,Gamma,300
4,Delta,400
```

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "data1",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "file1.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        },
        {
            "id": "data2",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "file2.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "loaders": [{
        "id": "combined",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "combined.csv",
            "path": "data/output/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [{
        "id": "concat",
        "classname": "pipelite.transformers.concatTR",
        "inputs": ["data1", "data2"],
        "outputs": ["combined"]
    }],
    
    "config": {
        "logger": {"level": "INFO", "path": "logs/", "filename": "concat.log"}
    }
}
```

### Output

```csv
id,name,value
1,Alpha,100
2,Beta,200
3,Gamma,300
4,Delta,400
```

---

## Pattern 4: Join Datasets

Combine datasets based on common columns.

### Input Data

**orders.csv**
```csv
order_id,customer_id,amount
1001,C001,150.00
1002,C002,275.50
1003,C001,89.99
```

**customers.csv**
```csv
customer_id,name,region
C001,Acme Corp,North
C002,Tech Inc,South
C003,Data Ltd,East
```

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "orders",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "orders.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        },
        {
            "id": "customers",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "customers.csv",
                "path": "data/input/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "loaders": [{
        "id": "enriched_orders",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "enriched_orders.csv",
            "path": "data/output/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [{
        "id": "join",
        "classname": "pipelite.transformers.joinTR",
        "inputs": ["orders", "customers"],
        "outputs": ["enriched_orders"],
        "parameters": {
            "on": "customer_id",
            "how": "left"
        }
    }],
    
    "config": {
        "logger": {"level": "INFO", "path": "logs/", "filename": "join.log"}
    }
}
```

### Output

```csv
order_id,customer_id,amount,name,region
1001,C001,150.00,Acme Corp,North
1002,C002,275.50,Tech Inc,South
1003,C001,89.99,Acme Corp,North
```

### Join Types

| Type | Description |
|------|-------------|
| `inner` | Only matching rows from both datasets |
| `left` | All rows from first dataset, matching from second |
| `right` | All rows from second dataset, matching from first |
| `outer` | All rows from both datasets |

---

## Pattern 5: Chain Transformations

Apply multiple transformations in sequence.

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "raw_data",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "raw.csv",
            "path": "data/input/",
            "encoding": "utf-8"
        }
    }],
    
    "loaders": [{
        "id": "final_output",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "final.csv",
            "path": "data/output/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [
        {
            "id": "step1_rename",
            "classname": "pipelite.transformers.renamecolTR",
            "inputs": ["raw_data"],
            "outputs": ["step1_output"],
            "parameters": {
                "columns": [
                    {"oldname": "old_col", "newname": "new_col"}
                ]
            }
        },
        {
            "id": "step2_extract",
            "classname": "pipelite.transformers.extractstrTR",
            "inputs": ["step1_output"],
            "outputs": ["final_output"],
            "parameters": {
                "column": "date_field",
                "start": 0,
                "length": 10
            }
        }
    ],
    
    "config": {
        "logger": {"level": "INFO", "path": "logs/", "filename": "chain.log"}
    }
}
```

The flow is automatically resolved:
```
raw_data → step1_rename → step1_output → step2_extract → final_output
```

---

## Pattern 6: Jinja2 Template Transformation

Transform column values using Jinja2 templates.

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "users",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "users.csv",
            "path": "data/input/",
            "encoding": "utf-8"
        }
    }],
    
    "loaders": [{
        "id": "users_processed",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "separator": ",",
            "filename": "users_processed.csv",
            "path": "data/output/",
            "encoding": "utf-8"
        }
    }],
    
    "transformers": [{
        "id": "format_names",
        "classname": "pipelite.transformers.jinjaTR",
        "inputs": ["users"],
        "outputs": ["users_processed"],
        "parameters": {
            "column": "full_name",
            "template": "{{ first_name }} {{ last_name | upper }}"
        }
    }],
    
    "config": {
        "logger": {"level": "INFO", "path": "logs/", "filename": "jinja.log"}
    }
}
```

---

## Troubleshooting

### Column Not Found

```
KeyError: 'column_name'
```

**Solution**: Verify column names match exactly (case-sensitive)

### Dataset Not Found

```
WARNING: The Transformer X has no input
```

**Solution**: Check that input IDs match extractor or previous transformer output IDs

### Invalid Transformer Configuration

**Solution**: Check the transformer's parameter documentation in the [Complete Guide](../guide/transformers.md)

---

## Next Steps

- [Profile Your Data](profile-data.md)
- [Extend pipelite](extend-pipelite.md)
- [Complete Transformer Reference](../guide/transformers.md)

---

[← Back to How-To Index](index.md) | [Profile Data →](profile-data.md)

