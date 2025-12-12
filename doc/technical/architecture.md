# Architecture

Technical overview of pipelite's architecture and design.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Core Components](#core-components)
3. [Data Flow](#data-flow)
4. [Class Hierarchy](#class-hierarchy)
5. [Configuration System](#configuration-system)
6. [Extension Points](#extension-points)

---

## System Overview

pipelite is a configuration-driven ETL framework that processes data through a pipeline of extractors, transformers, and loaders.

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    pipelite CLI                         │
│                   pipelite -cfg X                       │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│                  pipelineProcess                        │
│              (Orchestration Layer)                      │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│                    BOPipeline                           │
│              (Pipeline Execution)                       │
│  ┌──────────┐  ┌──────────────┐  ┌─────────────┐       │
│  │Extractors│─▶│ Transformers │─▶│   Loaders   │       │
│  └──────────┘  └──────────────┘  └─────────────┘       │
└─────────────────────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│                    plDataset(s)                         │
│              (Data Container Layer)                     │
│              ┌─────────────────┐                        │
│              │ pandas DataFrame│                        │
│              └─────────────────┘                        │
└─────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. Entry Point (`pipelite/__init__.py`)

```python
def main() -> int:
    config = cmdLineConfig.readConfig(argparse.ArgumentParser())
    log = pipelineProcess.getLogger(config)
    pipelineProcess(config, log).process()
    return log.errorCounts
```

### 2. Pipeline Process (`pipelineProcess.py`)

Orchestrates pipeline execution:

```python
class pipelineProcess(plObject):
    def process(self) -> plReports:
        pl = self.create()      # Instantiate pipeline
        pl.initialize()         # Load configuration
        pl.prepare()            # Build execution order
        pl.beforeProcess()      # Pre-processing hook
        reports = pl.execute()  # Run pipeline
        pl.afterProcess()       # Post-processing hook
        pl.terminate()          # Cleanup
        return reports
```

### 3. Pipeline (`BOPipeline` / `sequentialPL`)

Manages ETL object lifecycle:

```python
class BOPipeline(plBaseObject):
    etlObjects: list      # All extractors, transformers, loaders
    
    def initialize() -> bool    # Load and validate objects
    def prepare() -> bool       # Build execution order
    def execute() -> plReports  # Run the pipeline
    def terminate() -> bool     # Cleanup and report
```

### 4. Data Sources (`BODataSource`)

Abstract base for data sources:

```python
class BODataSource(plBaseObject):
    @abstractmethod
    def read(self) -> plDataset:
        """Read data from source"""
        
    @abstractmethod  
    def write(self, dataset) -> bool:
        """Write data to destination"""
```

### 5. Transformers (`BOTransformer`)

Abstract base for transformers:

```python
class BOTransformer(plBaseObject):
    dsInputs: list   # Input dataset IDs
    dsOutputs: list  # Output dataset IDs
    
    @abstractmethod
    def process(self, inputs: plDatasets) -> plDatasets:
        """Transform input datasets"""
```

### 6. Dataset (`plDataset`)

Wraps pandas DataFrame:

```python
class plDataset(plObject):
    __content: pd.DataFrame  # Internal data storage
    id: str                  # Dataset identifier
    
    def get() -> pd.DataFrame     # Access DataFrame
    def set(value)                # Set DataFrame
    def read_csv(...)             # Read from CSV
    def write_csv(...)            # Write to CSV
```

---

## Data Flow

### Execution Sequence

```
1. CONFIGURATION
   ┌─────────────────┐
   │ JSON Config File│
   └────────┬────────┘
            │
2. INITIALIZATION
   ┌────────▼────────┐
   │ Parse Config    │
   │ Validate Schema │
   │ Instantiate Obj │
   └────────┬────────┘
            │
3. PREPARATION  
   ┌────────▼────────┐
   │ Analyze Deps    │
   │ Build Order     │
   │ Resolve Flow    │
   └────────┬────────┘
            │
4. EXECUTION
   ┌────────▼────────┐
   │ Run Extractors  │──▶ plDataset A
   │ Run Transformers│──▶ plDataset B
   │ Run Loaders     │──▶ Output Files
   └────────┬────────┘
            │
5. TERMINATION
   ┌────────▼────────┐
   │ Generate Report │
   │ Cleanup         │
   └─────────────────┘
```

### Dataset Pool

Datasets are managed in a pool during execution:

```python
class sequentialPL(BOPipeline):
    dsPool: plDatasets  # All active datasets
    
    def extract(obj):
        dataset = obj.read()
        dsPool.add(dataset)
    
    def transform(obj):
        inputs = [dsPool.getFromId(id) for id in obj.dsInputs]
        outputs = obj.process(inputs)
        dsPool.merge(outputs)
    
    def load(obj):
        dataset = dsPool.getFromId(obj.id)
        obj.write(dataset)
```

---

## Class Hierarchy

### Inheritance Structure

```
plObject
├── plBaseObject
│   ├── BODataSource
│   │   ├── csvFileDS
│   │   ├── excelFileDS
│   │   ├── parquetFileDS
│   │   ├── odbcDS
│   │   ├── xesFileDS
│   │   ├── sapRfcTableDS
│   │   └── piDS
│   │
│   ├── BOTransformer
│   │   ├── passthroughTR
│   │   ├── renamecolTR
│   │   ├── extractstrTR
│   │   ├── concatTR
│   │   ├── joinTR
│   │   ├── lookupTR
│   │   ├── jinjaTR
│   │   └── profileTR
│   │
│   └── BOPipeline
│       └── sequentialPL
│
├── plDataset
│
└── pipelineProcess
```

### Key Relationships

```
┌──────────────┐      ┌──────────────┐
│pipelineProcess│─────▶│  BOPipeline  │
└──────────────┘      └──────┬───────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────▼───────┐   ┌────────▼────────┐   ┌──────▼───────┐
│ BODataSource  │   │  BOTransformer  │   │ BODataSource │
│  (Extractor)  │   │                 │   │   (Loader)   │
└───────┬───────┘   └────────┬────────┘   └──────┬───────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                      ┌──────▼──────┐
                      │  plDataset  │
                      └─────────────┘
```

---

## Configuration System

### Configuration Loading

```
JSON File
    │
    ▼
plConfig.load()
    │
    ▼
JSONPath Queries
    │
    ▼
Component Initialization
```

### JSONPath Access

```python
config.getParameter("$.extractors")      # Get all extractors
config.getParameter("$.config.logger.level")  # Get log level
```

### Validation Flow

```
JSON Config
    │
    ▼
Schema Validation (jsonschema)
    │
    ▼
Component Validation
    │
    ▼
Runtime Validation
```

---

## Extension Points

### 1. Custom Data Sources

Extend `BODataSource`:

```python
class myDataSource(BODataSource):
    def initialize(self, cfg) -> bool:
        # Setup
        return True
    
    def read(self) -> plDataset:
        # Read data
        return dataset
    
    def write(self, dataset) -> bool:
        # Write data
        return True
```

### 2. Custom Transformers

Extend `BOTransformer`:

```python
class myTransformer(BOTransformer):
    def initialize(self, params) -> bool:
        # Setup
        return True
    
    def process(self, inputs) -> plDatasets:
        # Transform
        return outputs
```

### 3. Custom Pipelines

Extend `BOPipeline`:

```python
class myPipeline(BOPipeline):
    def prepare(self) -> bool:
        # Custom ordering logic
        return True
    
    def execute(self) -> plReports:
        # Custom execution logic
        return reports
```

---

## Threading Model

pipelite uses a **single-threaded** execution model:

- All operations run sequentially
- Dataset pool is not thread-safe
- Suitable for I/O-bound workloads

For parallel processing, consider:
- Running multiple pipeline instances
- Using external orchestration (Airflow, etc.)

---

## Memory Management

### Dataset Lifecycle

```
1. Extractor reads → DataFrame created → Added to pool
2. Transformer processes → New DataFrame → Added to pool
3. Loader writes → DataFrame persists in pool
4. Pipeline terminates → Pool garbage collected
```

### Memory Considerations

- All data held in memory during execution
- Large datasets may require chunking
- No automatic memory management/spilling

---

## Next Steps

- [API Reference](api-reference.md)
- [Extending pipelite](extending.md)
- [Contributing](contributing.md)

---

[← Back to Technical Index](index.md) | [API Reference →](api-reference.md)

