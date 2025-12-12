# Data Sources Reference

Complete reference for all pipelite data sources.

---

## Table of Contents

1. [Overview](#overview)
2. [CSV Files](#csv-files)
3. [Excel Files](#excel-files)
4. [Parquet Files](#parquet-files)
5. [ODBC Databases](#odbc-databases)
6. [XES Files](#xes-files)
7. [SAP RFC](#sap-rfc)
8. [ABBYY Timeline](#abbyy-timeline)

---

## Overview

Data sources in pipelite can be used as:
- **Extractors**: Read data into the pipeline
- **Loaders**: Write data from the pipeline

### Common Properties

All data sources share these configuration properties:

| Property | Type | Required | Description |
|----------|------|:--------:|-------------|
| `id` | string | ✅ | Unique identifier |
| `classname` | string | ✅ | Full path to data source class |
| `parameters` | object | ✅ | Data source specific settings |
| `validation` | string | ❌ | Path to validation schema |

---

## CSV Files

Read and write comma-separated value files.

### Class

```
pipelite.datasources.csvFileDS
```

### Capabilities

| Operation | Supported |
|-----------|:---------:|
| Read (Extract) | ✅ |
| Write (Load) | ✅ |

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `filename` | string | ✅ | - | Name of the CSV file |
| `path` | string | ✅ | - | Directory path |
| `separator` | string | ❌ | `,` | Column delimiter |
| `encoding` | string | ❌ | `utf-8` | File encoding |

### Examples

**Extractor (Read)**

```json
{
    "id": "sales_data",
    "classname": "pipelite.datasources.csvFileDS",
    "parameters": {
        "filename": "sales.csv",
        "path": "data/input/",
        "separator": ",",
        "encoding": "utf-8"
    }
}
```

**Loader (Write)**

```json
{
    "id": "output",
    "classname": "pipelite.datasources.csvFileDS",
    "parameters": {
        "filename": "result.csv",
        "path": "data/output/",
        "separator": ";",
        "encoding": "utf-8"
    }
}
```

### Supported Separators

| Separator | Character |
|-----------|-----------|
| Comma | `,` |
| Semicolon | `;` |
| Tab | `\t` |
| Pipe | `\|` |

### Supported Encodings

- `utf-8` (recommended)
- `latin-1`
- `iso-8859-1`
- `cp1252` (Windows)
- `ascii`

---

## Excel Files

Read data from Excel spreadsheets.

### Class

```
pipelite.datasources.excelFileDS
```

### Capabilities

| Operation | Supported |
|-----------|:---------:|
| Read (Extract) | ✅ |
| Write (Load) | ❌ |

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `filename` | string | ✅ | - | Name of the Excel file |
| `path` | string | ✅ | - | Directory path |
| `sheet` | string/int | ❌ | `0` | Sheet name or index |

### Supported Formats

- `.xlsx` (Excel 2007+)
- `.xls` (Excel 97-2003)
- `.xlsm` (Macro-enabled)
- `.xlsb` (Binary)
- `.odf`, `.ods`, `.odt` (OpenDocument)

### Examples

**Read First Sheet**

```json
{
    "id": "excel_data",
    "classname": "pipelite.datasources.excelFileDS",
    "parameters": {
        "filename": "report.xlsx",
        "path": "data/input/"
    }
}
```

**Read Specific Sheet by Name**

```json
{
    "id": "excel_data",
    "classname": "pipelite.datasources.excelFileDS",
    "parameters": {
        "filename": "report.xlsx",
        "path": "data/input/",
        "sheet": "Sales Data"
    }
}
```

**Read by Sheet Index**

```json
{
    "id": "excel_data",
    "classname": "pipelite.datasources.excelFileDS",
    "parameters": {
        "filename": "report.xlsx",
        "path": "data/input/",
        "sheet": 2
    }
}
```

---

## Parquet Files

Read and write Apache Parquet columnar files.

### Class

```
pipelite.datasources.parquetFileDS
```

### Capabilities

| Operation | Supported |
|-----------|:---------:|
| Read (Extract) | ✅ |
| Write (Load) | ✅ |

### Requirements

```bash
pip install pyarrow
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `filename` | string | ✅ | - | Name of the Parquet file |
| `path` | string | ✅ | - | Directory path |
| `compression` | string | ❌ | `gzip` | Compression codec |

### Compression Options

- `gzip` (recommended)
- `snappy`
- `brotli`
- `lz4`
- `zstd`
- `none`

### Examples

**Read Parquet**

```json
{
    "id": "parquet_data",
    "classname": "pipelite.datasources.parquetFileDS",
    "parameters": {
        "filename": "data.parquet",
        "path": "data/input/"
    }
}
```

**Write Parquet**

```json
{
    "id": "output",
    "classname": "pipelite.datasources.parquetFileDS",
    "parameters": {
        "filename": "output.parquet",
        "path": "data/output/",
        "compression": "snappy"
    }
}
```

---

## ODBC Databases

Read data from any ODBC-compatible database.

### Class

```
pipelite.datasources.odbcDS
```

### Capabilities

| Operation | Supported |
|-----------|:---------:|
| Read (Extract) | ✅ |
| Write (Load) | ❌ |

### Requirements

```bash
pip install pyodbc
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|:--------:|-------------|
| `connection` | string | ✅ | ODBC connection string |
| `query` | string | ✅* | SQL query to execute |
| `queryfile` | string | ✅* | Path to SQL file |

*Either `query` or `queryfile` is required.

### Connection Strings by Database

**SQL Server**

```json
{
    "connection": "Driver={SQL Server};Server=myserver;Database=mydb;Trusted_Connection=yes;"
}
```

**SQL Server with Credentials**

```json
{
    "connection": "Driver={SQL Server};Server=myserver;Database=mydb;UID=user;PWD=password;"
}
```

**PostgreSQL**

```json
{
    "connection": "Driver={PostgreSQL};Server=localhost;Port=5432;Database=mydb;Uid=user;Pwd=password;"
}
```

**MySQL**

```json
{
    "connection": "Driver={MySQL ODBC 8.0 Driver};Server=localhost;Database=mydb;User=user;Password=password;"
}
```

**SQLite**

```json
{
    "connection": "Driver={SQLite3 ODBC Driver};Database=/path/to/database.sqlite;"
}
```

### Examples

**Inline Query**

```json
{
    "id": "db_data",
    "classname": "pipelite.datasources.odbcDS",
    "parameters": {
        "connection": "Driver={SQL Server};Server=localhost;Database=Sales;Trusted_Connection=yes;",
        "query": "SELECT * FROM customers WHERE active = 1"
    }
}
```

**External SQL File**

```json
{
    "id": "db_data",
    "classname": "pipelite.datasources.odbcDS",
    "parameters": {
        "connection": "Driver={SQL Server};Server=localhost;Database=Sales;Trusted_Connection=yes;",
        "queryfile": "queries/customers.sql"
    }
}
```

---

## XES Files

Read XES (eXtensible Event Stream) process mining event logs.

### Class

```
pipelite.datasources.xesFileDS
```

### Capabilities

| Operation | Supported |
|-----------|:---------:|
| Read (Extract) | ✅ |
| Write (Load) | ❌ |

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|:--------:|-------------|
| `filename` | string | ✅ | Name of the XES file |
| `path` | string | ✅ | Directory path |

### Example

```json
{
    "id": "event_log",
    "classname": "pipelite.datasources.xesFileDS",
    "parameters": {
        "filename": "process_log.xes",
        "path": "data/input/"
    }
}
```

### Output Columns

XES files are flattened to include standard event log columns:
- `case:concept:name` - Case identifier
- `concept:name` - Activity name
- `time:timestamp` - Event timestamp
- `org:resource` - Resource (if available)
- Plus any additional attributes

---

## SAP RFC

Read data from SAP tables via RFC (Remote Function Call).

### Class

```
pipelite.datasources.sapRfcTableDS
```

### Capabilities

| Operation | Supported |
|-----------|:---------:|
| Read (Extract) | ✅ |
| Write (Load) | ❌ |

### Requirements

```bash
pip install pyrfc
```

Also requires SAP NW RFC SDK installed.

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|:--------:|-------------|
| `ashost` | string | ✅ | SAP application server |
| `sysnr` | string | ✅ | System number |
| `client` | string | ✅ | Client number |
| `user` | string | ✅ | SAP username |
| `passwd` | string | ✅ | SAP password |
| `table` | string | ✅ | SAP table name |
| `fields` | array | ❌ | Fields to select |
| `options` | array | ❌ | WHERE conditions |

### Example

```json
{
    "id": "sap_customers",
    "classname": "pipelite.datasources.sapRfcTableDS",
    "parameters": {
        "ashost": "sap.example.com",
        "sysnr": "00",
        "client": "100",
        "user": "SAPUSER",
        "passwd": "password",
        "table": "KNA1",
        "fields": ["KUNNR", "NAME1", "LAND1"],
        "options": ["LAND1 EQ 'US'"]
    }
}
```

---

## ABBYY Timeline

Write data to ABBYY Timeline Process Intelligence.

### Class

```
pipelite.datasources.piDS
```

### Capabilities

| Operation | Supported |
|-----------|:---------:|
| Read (Extract) | ❌ |
| Write (Load) | ✅ |

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|:--------:|-------------|
| `url` | string | ✅ | Timeline API URL |
| `repository` | string | ✅ | Repository name |
| `apikey` | string | ✅ | API key |

### Example

```json
{
    "id": "timeline_load",
    "classname": "pipelite.datasources.piDS",
    "parameters": {
        "url": "https://timeline.example.com/api",
        "repository": "ProcessData",
        "apikey": "your-api-key-here"
    }
}
```

---

## Custom Data Sources

See [How To: Extend pipelite](../howto/extend-pipelite.md) for creating custom data sources.

---

## Next Steps

- [Transformers](transformers.md)
- [Configuration](configuration.md)
- [How-To: Connect to Database](../howto/connect-to-database.md)

---

[← Back to Guide Index](index.md) | [Transformers →](transformers.md)

