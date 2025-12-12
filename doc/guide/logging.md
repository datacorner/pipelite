# Logging Reference

Complete guide to pipelite logging and monitoring.

---

## Table of Contents

1. [Overview](#overview)
2. [Configuration](#configuration)
3. [Log Levels](#log-levels)
4. [Log Format](#log-format)
5. [Log Rotation](#log-rotation)
6. [Reading Logs](#reading-logs)
7. [Best Practices](#best-practices)

---

## Overview

pipelite provides comprehensive logging for:

- **Pipeline execution**: Start, progress, completion
- **Data operations**: Row counts, column information
- **Errors and warnings**: Failures, issues
- **Debug information**: Detailed execution traces

Logs are written to both:
- **Console**: Real-time output during execution
- **Log file**: Persistent record for analysis

---

## Configuration

### Basic Configuration

```json
{
    "config": {
        "logger": {
            "level": "INFO",
            "path": "logs/",
            "filename": "pipeline.log"
        }
    }
}
```

### Full Configuration

```json
{
    "config": {
        "logger": {
            "level": "DEBUG",
            "format": "%(asctime)s|%(name)s|%(levelname)s|%(message)s",
            "path": "/var/log/pipelite/",
            "filename": "etl_pipeline.log",
            "maxbytes": 10000000
        }
    }
}
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `level` | string | `DEBUG` | Minimum log level |
| `format` | string | See below | Log message format |
| `path` | string | `./` | Directory for log files |
| `filename` | string | `pipelite.log` | Log file name |
| `maxbytes` | int | `1000000` | Max file size before rotation |

---

## Log Levels

### Available Levels

| Level | Priority | Description |
|-------|:--------:|-------------|
| `DEBUG` | 1 | Detailed debugging information |
| `INFO` | 2 | General operational messages |
| `WARNING` | 3 | Warning conditions |
| `ERROR` | 4 | Error conditions |

### Level Configuration

```json
{"level": "DEBUG"}   // All messages
{"level": "INFO"}    // INFO, WARNING, ERROR
{"level": "WARNING"} // WARNING, ERROR
{"level": "ERROR"}   // ERROR only
```

### Message Examples by Level

**DEBUG**
```
DEBUG|pipelite.baseobjs.plBaseObject|Import module -> pipelite.datasources.csvFileDS
DEBUG|pipelite.pipelines.sequentialPL|Pipeline preparation
```

**INFO**
```
INFO|pipelite initialisation ...
INFO|Pipeline prepared successfully. Flow -> [A -> T -> B]
INFO|[ EXTRACT A - Rows: 1000 ]
INFO|*** End of Job treatment ***
```

**WARNING**
```
WARNING|The Transformer T has no input, by pass it!
```

**ERROR**
```
ERROR|The file data.csv does not exist or is not accessible
ERROR|pipelite cannot be initialized: The Data pipeline could not be initialized properly
```

---

## Log Format

### Default Format

```
%(asctime)s|%(name)s|%(levelname)s|%(message)s
```

**Example Output:**
```
2024-01-15 10:30:00,123|pipelite.pipelines.sequentialPL|INFO|Pipeline prepared successfully
```

### Format Placeholders

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `%(asctime)s` | Timestamp | `2024-01-15 10:30:00,123` |
| `%(name)s` | Logger name | `pipelite.pipelines.sequentialPL` |
| `%(levelname)s` | Log level | `INFO` |
| `%(message)s` | Log message | `Pipeline prepared successfully` |
| `%(filename)s` | Source file | `sequentialPL.py` |
| `%(lineno)d` | Line number | `45` |
| `%(funcName)s` | Function name | `execute` |

### Custom Formats

**Simple:**
```json
{"format": "%(asctime)s|%(levelname)s|%(message)s"}
```

**Detailed:**
```json
{"format": "%(asctime)s|%(name)s|%(levelname)s|%(filename)s:%(lineno)d|%(message)s"}
```

**JSON-style:**
```json
{"format": "{\"time\":\"%(asctime)s\",\"level\":\"%(levelname)s\",\"msg\":\"%(message)s\"}"}
```

---

## Log Rotation

pipelite automatically rotates log files when they reach the maximum size.

### Configuration

```json
{
    "config": {
        "logger": {
            "maxbytes": 5000000
        }
    }
}
```

### How It Works

1. Log writes to `pipeline.log`
2. When file reaches `maxbytes`, it's renamed to `pipeline.log.1`
3. A new `pipeline.log` is created
4. Old `.1` file is overwritten on next rotation

### Size Guidelines

| Use Case | Recommended Size |
|----------|-----------------|
| Development | 1 MB (`1000000`) |
| Production | 10 MB (`10000000`) |
| High volume | 50 MB (`50000000`) |

---

## Reading Logs

### Console Output

During execution, logs appear in real-time:

```bash
$ pipelite -cfg pipeline.json
Log file: logs/pipeline.log
2024-01-15 10:30:00 INFO|pipelite initialisation ...
2024-01-15 10:30:00 INFO|Pipeline prepared successfully. Flow -> [A -> T -> B]
...
```

### Log File Analysis

**View recent entries:**
```bash
tail -f logs/pipeline.log
```

**Search for errors:**
```bash
grep "ERROR" logs/pipeline.log
```

**Count by level:**
```bash
grep -c "ERROR" logs/pipeline.log
grep -c "WARNING" logs/pipeline.log
grep -c "INFO" logs/pipeline.log
```

**Filter by component:**
```bash
grep "csvFileDS" logs/pipeline.log
grep "sequentialPL" logs/pipeline.log
```

---

## Best Practices

### 1. Use Appropriate Levels

| Environment | Recommended Level |
|-------------|-------------------|
| Development | `DEBUG` |
| Testing | `DEBUG` or `INFO` |
| Production | `INFO` or `WARNING` |

### 2. Organize Log Files

```
project/
├── logs/
│   ├── dev/
│   │   └── pipeline.log
│   └── prod/
│       └── pipeline.log
```

### 3. Include Context in Filenames

```json
{"filename": "sales_etl_2024.log"}
{"filename": "daily_import.log"}
```

### 4. Monitor Log Files

**Linux/macOS:**
```bash
# Watch in real-time
tail -f logs/pipeline.log

# Check file size
ls -lh logs/pipeline.log
```

**Automated monitoring:**
```bash
# Count errors in last hour
grep "ERROR" logs/pipeline.log | grep "$(date -d '1 hour ago' +'%Y-%m-%d %H')"
```

### 5. Archive Old Logs

```bash
# Compress logs older than 7 days
find logs/ -name "*.log.*" -mtime +7 -exec gzip {} \;
```

### 6. Secure Sensitive Data

Avoid logging sensitive information:
- Passwords
- API keys
- Personal data

---

## Troubleshooting

### Log File Not Created

**Check:**
1. Path exists: `mkdir -p logs/`
2. Permissions: `chmod 755 logs/`
3. Disk space: `df -h`

### Log File Empty

**Check:**
1. Pipeline actually ran
2. Level not set too high (e.g., ERROR when only INFO occurs)
3. Format string is valid

### Missing Console Output

The console always shows output. If missing:
1. Check stderr redirection
2. Verify pipeline is running

---

## Example: Complete Logging Setup

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    
    "extractors": [{
        "id": "data",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "filename": "input.csv",
            "path": "data/",
            "separator": ","
        }
    }],
    
    "loaders": [{
        "id": "output",
        "classname": "pipelite.datasources.csvFileDS",
        "parameters": {
            "filename": "output.csv",
            "path": "data/out/",
            "separator": ","
        }
    }],
    
    "transformers": [{
        "id": "process",
        "classname": "pipelite.transformers.passthroughTR",
        "inputs": ["data"],
        "outputs": ["output"]
    }],
    
    "config": {
        "logger": {
            "level": "INFO",
            "format": "%(asctime)s|%(levelname)s|%(name)s|%(message)s",
            "path": "logs/",
            "filename": "etl_pipeline.log",
            "maxbytes": 10000000
        }
    }
}
```

---

## Next Steps

- [Configuration](configuration.md)
- [Pipelines](pipelines.md)
- [Technical Documentation](../technical/index.md)

---

[← Back to Guide Index](index.md) | [Technical Documentation →](../technical/index.md)

