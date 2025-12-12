# How To: Connect to a Database

This guide shows you how to read data from SQL databases using ODBC connections in pipelite.

---

## Overview

By the end of this guide, you will:
- Configure ODBC connections for various databases
- Read data using SQL queries
- Store queries in external files for maintainability

---

## Prerequisites

- pipelite installed
- `pyodbc` package installed: `pip install pyodbc`
- ODBC driver for your database installed
- Database credentials and connection details

---

## Step 1: Install Database Driver

### SQL Server

```bash
# Windows - usually pre-installed
# macOS
brew install microsoft/mssql-release/msodbcsql17

# Linux (Ubuntu/Debian)
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
apt-get update && apt-get install -y msodbcsql17
```

### SQLite

```bash
# Usually pre-installed with Python
# No additional driver needed
```

### PostgreSQL

```bash
# Windows
# Download from https://www.postgresql.org/ftp/odbc/versions/

# macOS
brew install psqlodbc

# Linux
apt-get install odbc-postgresql
```

### MySQL

```bash
# Download MySQL ODBC Connector from:
# https://dev.mysql.com/downloads/connector/odbc/
```

---

## Step 2: Basic Database Connection

Create `db_pipeline.json`:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "customers",
            "classname": "pipelite.datasources.odbcDS",
            "parameters": {
                "connection": "Driver={SQL Server};Server=localhost;Database=SalesDB;Trusted_Connection=yes;",
                "query": "SELECT customer_id, name, email, created_date FROM customers WHERE active = 1"
            }
        }
    ],
    
    "loaders": [
        {
            "id": "customers_export",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "customers_export.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "transformers": [
        {
            "id": "process",
            "classname": "pipelite.transformers.passthroughTR",
            "inputs": ["customers"],
            "outputs": ["customers_export"]
        }
    ],
    
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "db_pipeline.log"
        }
    }
}
```

---

## Step 3: Connection String Examples

### SQL Server (Windows Authentication)

```json
{
    "connection": "Driver={SQL Server};Server=myserver;Database=mydb;Trusted_Connection=yes;"
}
```

### SQL Server (SQL Authentication)

```json
{
    "connection": "Driver={SQL Server};Server=myserver;Database=mydb;UID=myuser;PWD=mypassword;"
}
```

### SQLite

```json
{
    "connection": "Driver={SQLite3 ODBC Driver};Database=/path/to/database.sqlite;"
}
```

### PostgreSQL

```json
{
    "connection": "Driver={PostgreSQL};Server=localhost;Port=5432;Database=mydb;Uid=myuser;Pwd=mypassword;"
}
```

### MySQL

```json
{
    "connection": "Driver={MySQL ODBC 8.0 Driver};Server=localhost;Database=mydb;User=myuser;Password=mypassword;"
}
```

---

## Step 4: Using External SQL Files

For complex queries, store them in separate files.

Create `queries/get_orders.sql`:

```sql
SELECT 
    o.order_id,
    o.order_date,
    c.customer_name,
    p.product_name,
    od.quantity,
    od.unit_price,
    (od.quantity * od.unit_price) as total_amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_details od ON o.order_id = od.order_id
JOIN products p ON od.product_id = p.product_id
WHERE o.order_date >= '2024-01-01'
ORDER BY o.order_date DESC
```

Reference it in your pipeline:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "orders",
            "classname": "pipelite.datasources.odbcDS",
            "parameters": {
                "connection": "Driver={SQL Server};Server=localhost;Database=SalesDB;Trusted_Connection=yes;",
                "queryfile": "queries/get_orders.sql"
            }
        }
    ],
    
    "loaders": [
        {
            "id": "orders_report",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "orders_report.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "transformers": [
        {
            "id": "export",
            "classname": "pipelite.transformers.passthroughTR",
            "inputs": ["orders"],
            "outputs": ["orders_report"]
        }
    ],
    
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "orders_pipeline.log"
        }
    }
}
```

---

## Step 5: Multiple Database Sources

Read from multiple databases and combine data:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "sales_data",
            "classname": "pipelite.datasources.odbcDS",
            "parameters": {
                "connection": "Driver={SQL Server};Server=sales-server;Database=SalesDB;Trusted_Connection=yes;",
                "query": "SELECT order_id, customer_id, amount FROM orders"
            }
        },
        {
            "id": "customer_data",
            "classname": "pipelite.datasources.odbcDS",
            "parameters": {
                "connection": "Driver={SQL Server};Server=crm-server;Database=CRM;Trusted_Connection=yes;",
                "query": "SELECT customer_id, name, region FROM customers"
            }
        }
    ],
    
    "loaders": [
        {
            "id": "combined_report",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "combined_report.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "transformers": [
        {
            "id": "join_data",
            "classname": "pipelite.transformers.joinTR",
            "inputs": ["sales_data", "customer_data"],
            "outputs": ["combined_report"],
            "parameters": {
                "on": "customer_id",
                "how": "left"
            }
        }
    ],
    
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "combined_pipeline.log"
        }
    }
}
```

---

## Complete Example: SQLite Database

Create a test database and query it:

### Create Test Database

```python
import sqlite3

conn = sqlite3.connect('data/test.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        price REAL,
        stock INTEGER
    )
''')

products = [
    (1, 'Widget A', 10.99, 100),
    (2, 'Widget B', 15.99, 50),
    (3, 'Widget C', 25.99, 25),
]

cursor.executemany('INSERT OR REPLACE INTO products VALUES (?,?,?,?)', products)
conn.commit()
conn.close()
```

### Pipeline Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [
        {
            "id": "products",
            "classname": "pipelite.datasources.odbcDS",
            "parameters": {
                "connection": "Driver={SQLite3 ODBC Driver};Database=data/test.db;",
                "query": "SELECT * FROM products WHERE stock > 0"
            }
        }
    ],
    
    "loaders": [
        {
            "id": "products_csv",
            "classname": "pipelite.datasources.csvFileDS",
            "parameters": {
                "separator": ",",
                "filename": "products.csv",
                "path": "data/output/",
                "encoding": "utf-8"
            }
        }
    ],
    
    "transformers": [
        {
            "id": "export",
            "classname": "pipelite.transformers.passthroughTR",
            "inputs": ["products"],
            "outputs": ["products_csv"]
        }
    ],
    
    "config": {
        "logger": {
            "level": "DEBUG",
            "path": "logs/",
            "filename": "sqlite_pipeline.log"
        }
    }
}
```

---

## Troubleshooting

### ODBC Driver Not Found

```
Error: [IM002] [Microsoft][ODBC Driver Manager] Data source name not found
```

**Solution**: 
- Verify the driver is installed
- Check exact driver name with `odbcinst -q -d` (Linux/Mac)
- Use the exact driver name in quotes

### Connection Failed

```
Error: Login failed for user
```

**Solution**:
- Verify credentials
- Check network connectivity
- Ensure database server allows remote connections

### Query Timeout

**Solution**: Add timeout to connection string:
```json
{
    "connection": "Driver={SQL Server};Server=myserver;Database=mydb;Timeout=300;..."
}
```

---

## Next Steps

- [Transform Data](transform-data.md)
- [Join Multiple Datasets](join-datasets.md)
- [Profile Your Data](profile-data.md)

---

[← Back to How-To Index](index.md) | [Transform Data →](transform-data.md)

