# Transformers Reference

Complete reference for all pipelite transformers.

---

## Table of Contents

1. [Overview](#overview)
2. [Pass Through](#pass-through)
3. [Rename Column](#rename-column)
4. [Extract String](#extract-string)
5. [Concatenate](#concatenate)
6. [Join](#join)
7. [Lookup](#lookup)
8. [Jinja Transform](#jinja-transform)
9. [Profile](#profile)

---

## Overview

Transformers process data between extractors and loaders. They can:
- Modify column names or values
- Combine multiple datasets
- Filter or enrich data
- Generate reports

### Common Properties

| Property | Type | Required | Description |
|----------|------|:--------:|-------------|
| `id` | string | ✅ | Unique identifier |
| `classname` | string | ✅ | Full path to transformer class |
| `inputs` | array | ✅ | List of input dataset IDs |
| `outputs` | array | ✅ | List of output dataset IDs |
| `parameters` | object | ❌ | Transformer specific settings |

### Data Flow

Transformers connect datasets via `inputs` and `outputs`:

```
extractor[id="A"] → transformer[inputs=["A"], outputs=["B"]] → loader[id="B"]
```

---

## Pass Through

Rename dataset identifiers without modifying data.

### Class

```
pipelite.transformers.passthroughTR
```

### Parameters

None required.

### Example

```json
{
    "id": "rename_dataset",
    "classname": "pipelite.transformers.passthroughTR",
    "inputs": ["source_data"],
    "outputs": ["target_data"]
}
```

### Use Cases

- Routing data from extractors to loaders
- Creating multiple output copies
- Simple dataset renaming

---

## Rename Column

Rename column headers in a dataset.

### Class

```
pipelite.transformers.renamecolTR
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|:--------:|-------------|
| `columns` | array | ✅ | List of rename operations |

Each rename operation:

| Property | Type | Description |
|----------|------|-------------|
| `oldname` | string | Current column name |
| `newname` | string | New column name |

### Example

```json
{
    "id": "rename_columns",
    "classname": "pipelite.transformers.renamecolTR",
    "inputs": ["source"],
    "outputs": ["target"],
    "parameters": {
        "columns": [
            {"oldname": "cust_id", "newname": "customer_id"},
            {"oldname": "prod_name", "newname": "product_name"},
            {"oldname": "qty", "newname": "quantity"}
        ]
    }
}
```

### Before/After

| Before | After |
|--------|-------|
| `cust_id` | `customer_id` |
| `prod_name` | `product_name` |
| `qty` | `quantity` |

---

## Extract String

Extract a substring from column values.

### Class

```
pipelite.transformers.extractstrTR
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|:--------:|-------------|
| `column` | string | ✅ | Column to extract from |
| `start` | int | ✅ | Start index (0-based) |
| `length` | int | ✅ | Number of characters |

### Example

Extract year from date string:

```json
{
    "id": "extract_year",
    "classname": "pipelite.transformers.extractstrTR",
    "inputs": ["data"],
    "outputs": ["result"],
    "parameters": {
        "column": "order_date",
        "start": 0,
        "length": 4
    }
}
```

### Before/After

| order_date (before) | order_date (after) |
|---------------------|-------------------|
| `2024-01-15` | `2024` |
| `2024-02-20` | `2024` |

---

## Concatenate

Combine multiple datasets vertically (stack rows).

### Class

```
pipelite.transformers.concatTR
```

### Parameters

None required. Uses all input datasets.

### Example

```json
{
    "id": "combine_files",
    "classname": "pipelite.transformers.concatTR",
    "inputs": ["file1", "file2", "file3"],
    "outputs": ["combined"]
}
```

### Requirements

- All input datasets should have the same column structure
- Columns with same names are aligned

### Before/After

**file1**:
| id | name |
|----|------|
| 1 | Alice |

**file2**:
| id | name |
|----|------|
| 2 | Bob |

**combined**:
| id | name |
|----|------|
| 1 | Alice |
| 2 | Bob |

---

## Join

Join two datasets on common columns.

### Class

```
pipelite.transformers.joinTR
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `on` | string | ✅ | - | Column name to join on |
| `how` | string | ❌ | `inner` | Join type |

### Join Types

| Type | Description |
|------|-------------|
| `inner` | Only matching rows from both datasets |
| `left` | All rows from first input, matching from second |
| `right` | All rows from second input, matching from first |
| `outer` | All rows from both datasets |

### Example

```json
{
    "id": "join_orders",
    "classname": "pipelite.transformers.joinTR",
    "inputs": ["orders", "customers"],
    "outputs": ["enriched_orders"],
    "parameters": {
        "on": "customer_id",
        "how": "left"
    }
}
```

### Before/After

**orders**:
| order_id | customer_id | amount |
|----------|-------------|--------|
| 1 | C001 | 100 |
| 2 | C002 | 200 |

**customers**:
| customer_id | name |
|-------------|------|
| C001 | Alice |
| C002 | Bob |

**enriched_orders** (left join):
| order_id | customer_id | amount | name |
|----------|-------------|--------|------|
| 1 | C001 | 100 | Alice |
| 2 | C002 | 200 | Bob |

---

## Lookup

Enrich data using a lookup table, dropping non-matching rows.

### Class

```
pipelite.transformers.lookupTR
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|:--------:|-------------|
| `on` | string | ✅ | Column name for lookup |

### Example

```json
{
    "id": "enrich_data",
    "classname": "pipelite.transformers.lookupTR",
    "inputs": ["transactions", "product_catalog"],
    "outputs": ["enriched_transactions"],
    "parameters": {
        "on": "product_id"
    }
}
```

### Difference from Join

- **Join**: Keeps rows based on join type (left, right, inner, outer)
- **Lookup**: Similar to inner join, filters to only matching rows

---

## Jinja Transform

Transform column values using Jinja2 templates.

### Class

```
pipelite.transformers.jinjaTR
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|:--------:|-------------|
| `column` | string | ✅ | Target column for result |
| `template` | string | ✅ | Jinja2 template string |

### Template Variables

All columns from the dataset are available as variables.

### Example

Create a full name column:

```json
{
    "id": "create_fullname",
    "classname": "pipelite.transformers.jinjaTR",
    "inputs": ["users"],
    "outputs": ["users_processed"],
    "parameters": {
        "column": "full_name",
        "template": "{{ first_name }} {{ last_name }}"
    }
}
```

### Before/After

**users**:
| first_name | last_name |
|------------|-----------|
| Alice | Smith |
| Bob | Jones |

**users_processed**:
| first_name | last_name | full_name |
|------------|-----------|-----------|
| Alice | Smith | Alice Smith |
| Bob | Jones | Bob Jones |

### Advanced Templates

**Uppercase**:
```json
"template": "{{ name | upper }}"
```

**Conditional**:
```json
"template": "{% if status == 'active' %}Active{% else %}Inactive{% endif %}"
```

**Formatting**:
```json
"template": "${{ '%.2f' | format(price) }}"
```

---

## Profile

Generate data profiling reports.

### Class

```
pipelite.transformers.profileTR
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `output_file` | string | ✅ | - | Path for JSON output |
| `html_output` | string | ❌ | - | Path for HTML report |
| `max_value_counts` | int | ❌ | `10` | Max distinct values to show |

### Example

```json
{
    "id": "profile_data",
    "classname": "pipelite.transformers.profileTR",
    "inputs": ["data"],
    "outputs": [],
    "parameters": {
        "output_file": "reports/data_profile.json",
        "html_output": "reports/data_profile.html",
        "max_value_counts": 20
    }
}
```

### Profile Output

The JSON profile includes for each column:
- Data type
- Count of values
- Null count and percentage
- Unique value count
- Min/Max (numeric)
- Mean (numeric)
- Value distribution

---

## Chaining Transformers

Transformers can be chained by using intermediate output IDs:

```json
{
    "transformers": [
        {
            "id": "step1",
            "classname": "pipelite.transformers.renamecolTR",
            "inputs": ["raw_data"],
            "outputs": ["step1_output"],
            "parameters": {...}
        },
        {
            "id": "step2",
            "classname": "pipelite.transformers.extractstrTR",
            "inputs": ["step1_output"],
            "outputs": ["step2_output"],
            "parameters": {...}
        },
        {
            "id": "step3",
            "classname": "pipelite.transformers.passthroughTR",
            "inputs": ["step2_output"],
            "outputs": ["final_output"]
        }
    ]
}
```

pipelite automatically determines the correct execution order based on dependencies.

---

## Custom Transformers

See [How To: Extend pipelite](../howto/extend-pipelite.md) for creating custom transformers.

---

## Next Steps

- [Pipelines](pipelines.md)
- [How-To: Transform Data](../howto/transform-data.md)
- [Configuration](configuration.md)

---

[← Back to Guide Index](index.md) | [Pipelines →](pipelines.md)

