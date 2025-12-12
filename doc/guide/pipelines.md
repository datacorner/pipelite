# Pipelines Reference

Complete reference for pipelite pipeline types and execution.

---

## Table of Contents

1. [Overview](#overview)
2. [Sequential Pipeline](#sequential-pipeline)
3. [Execution Flow](#execution-flow)
4. [Lifecycle Hooks](#lifecycle-hooks)
5. [Error Handling](#error-handling)
6. [Performance](#performance)

---

## Overview

Pipelines orchestrate the execution of extractors, transformers, and loaders. They determine:

- **Execution order**: Which components run in what sequence
- **Data flow**: How data passes between components
- **Error handling**: What happens when something fails

### Pipeline Classes

| Class | Description |
|-------|-------------|
| `pipelite.pipelines.sequentialPL` | Sequential execution based on dependencies |

---

## Sequential Pipeline

The default pipeline type that executes components in dependency order.

### Class

```
pipelite.pipelines.sequentialPL
```

### Configuration

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    "extractors": [...],
    "loaders": [...],
    "transformers": [...]
}
```

### How It Works

1. **Preparation Phase**
   - Analyzes dependencies between components
   - Builds execution order based on inputs/outputs
   - Validates all components are reachable

2. **Execution Phase**
   - Runs extractors to load data
   - Executes transformers in dependency order
   - Runs loaders to write output

3. **Termination Phase**
   - Generates execution report
   - Logs summary statistics
   - Cleans up resources

---

## Execution Flow

### Automatic Ordering

The pipeline automatically determines execution order based on data dependencies.

**Example Configuration:**

```json
{
    "extractors": [
        {"id": "A", ...},
        {"id": "B", ...}
    ],
    "loaders": [
        {"id": "D", ...}
    ],
    "transformers": [
        {"id": "C", "inputs": ["A", "B"], "outputs": ["D"]}
    ]
}
```

**Resolved Flow:**

```
A ──┐
    ├──▶ C ──▶ D
B ──┘
```

**Execution Order:**
1. Extract A
2. Extract B
3. Transform C
4. Load D

### Complex Flows

**Chained Transformers:**

```json
{
    "transformers": [
        {"id": "T1", "inputs": ["A"], "outputs": ["X"]},
        {"id": "T2", "inputs": ["X"], "outputs": ["Y"]},
        {"id": "T3", "inputs": ["Y"], "outputs": ["B"]}
    ]
}
```

**Flow:**
```
A ──▶ T1 ──▶ X ──▶ T2 ──▶ Y ──▶ T3 ──▶ B
```

**Multiple Outputs:**

```json
{
    "transformers": [
        {"id": "T1", "inputs": ["A"], "outputs": ["X", "Y"]},
        {"id": "T2", "inputs": ["X"], "outputs": ["B"]},
        {"id": "T3", "inputs": ["Y"], "outputs": ["C"]}
    ]
}
```

**Flow:**
```
        ┌──▶ X ──▶ T2 ──▶ B
A ──▶ T1
        └──▶ Y ──▶ T3 ──▶ C
```

---

## Lifecycle Hooks

The pipeline execution follows defined lifecycle phases:

### Phases

| Phase | Method | Description |
|-------|--------|-------------|
| 1 | `initialize()` | Load and validate configuration |
| 2 | `prepare()` | Build execution order |
| 3 | `beforeProcess()` | Pre-execution hook |
| 4 | `execute()` | Run the pipeline |
| 5 | `afterProcess()` | Post-execution hook |
| 6 | `terminate()` | Cleanup and reporting |

### Execution Diagram

```
┌─────────────┐
│ initialize  │ ──▶ Load config, validate
└──────┬──────┘
       │
┌──────▼──────┐
│   prepare   │ ──▶ Build execution order
└──────┬──────┘
       │
┌──────▼──────┐
│beforeProcess│ ──▶ Custom pre-processing
└──────┬──────┘
       │
┌──────▼──────┐
│   execute   │ ──▶ Extract → Transform → Load
└──────┬──────┘
       │
┌──────▼──────┐
│afterProcess │ ──▶ Custom post-processing
└──────┬──────┘
       │
┌──────▼──────┐
│  terminate  │ ──▶ Report, cleanup
└─────────────┘
```

---

## Error Handling

### Default Behavior

By default, errors stop pipeline execution:

```
ERROR: Extract failed for dataset A
ERROR: Pipeline cannot be initialized: The Data pipeline could not be initialized properly
```

### Error Levels

| Level | Description |
|-------|-------------|
| **Warning** | Non-fatal issue, execution continues |
| **Error** | Fatal issue, execution stops |

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| File not found | Input file missing | Check file path |
| Connection failed | Database unreachable | Verify connection string |
| Invalid configuration | JSON syntax error | Validate JSON |
| Duplicate ID | Same ID used twice | Use unique IDs |
| Missing input | Transformer input not found | Check input dataset exists |

### Execution Report

After execution, a report shows:

```
--- PIPELITE REPORT ---
           Type                Description  Rows Processed
id                                                        
A     Extractor  csvFileDS -> Output: [A]            100
B        Loader  csvFileDS -> Input: [B]             100
T   Transformer  joinTR -> Inputs/Outputs             100

Total Duration 0.5 sec

Warnings: 0
Errors: 0
```

---

## Performance

### Memory Management

pipelite loads data into memory using pandas DataFrames. For large datasets:

- **Monitor memory usage**: Watch system memory
- **Process in chunks**: Split large files
- **Use efficient formats**: Parquet over CSV

### Execution Tips

| Tip | Description |
|-----|-------------|
| **Minimize extractors** | Read data once, reuse via transformers |
| **Filter early** | Apply filters close to the source |
| **Use appropriate join types** | Left join keeps more rows than inner |
| **Profile first** | Understand data before complex transforms |

### Logging Levels

For performance troubleshooting:

| Level | Use Case |
|-------|----------|
| `ERROR` | Production, minimum output |
| `WARNING` | Production with alerts |
| `INFO` | Normal operation |
| `DEBUG` | Development, full details |

---

## Pipeline Report

### Console Output

```
--- PIPELITE REPORT ---
           Type                              Description  Start                  End Duration Rows Processed
id                                                                                                          
source  Extractor  csvFileDS -> Output: [source]           2024-01-15 10:00:00.1  2024-01-15 10:00:00.2   0.1s            1000
target     Loader  csvFileDS -> Input: [target]            2024-01-15 10:00:00.3  2024-01-15 10:00:00.4   0.1s            1000
transform Transformer  renamecolTR -> Inputs/Outputs        2024-01-15 10:00:00.2  2024-01-15 10:00:00.3   0.1s            1000

Total Duration 0.3 sec
```

### Log File

The log file contains detailed execution information:

```
2024-01-15 10:00:00|INFO|pipelite initialisation ...
2024-01-15 10:00:00|INFO|Pipeline prepared successfully. Flow -> [source -> transform -> target]
2024-01-15 10:00:00|INFO|Extract from dataset: source
2024-01-15 10:00:00|INFO|[ EXTRACT source - Rows: 1000 ]
2024-01-15 10:00:00|INFO|Apply Transformation: transform
2024-01-15 10:00:00|INFO|[ TRANSFORM transform - Rows: 1000 ]
2024-01-15 10:00:00|INFO|Load from dataset: target
2024-01-15 10:00:00|INFO|[ LOAD target - Rows: 1000 ]
2024-01-15 10:00:00|INFO|*** End of Job treatment ***
```

---

## Next Steps

- [Logging](logging.md)
- [Configuration](configuration.md)
- [Technical Architecture](../technical/architecture.md)

---

[← Back to Guide Index](index.md) | [Logging →](logging.md)

