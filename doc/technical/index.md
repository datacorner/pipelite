# Technical Documentation

In-depth technical documentation for developers working with or extending pipelite.

---

## Documentation Contents

### Architecture & Design

| Document | Description |
|----------|-------------|
| [Architecture](architecture.md) | System architecture and design patterns |
| [API Reference](api-reference.md) | Complete API documentation |

### Development

| Document | Description |
|----------|-------------|
| [Extending pipelite](extending.md) | Creating custom components |
| [Contributing](contributing.md) | Contribution guidelines |

---

## Quick Links

### Key Classes

| Class | Module | Description |
|-------|--------|-------------|
| `pipelineProcess` | `pipelite.pipelineProcess` | Main entry point |
| `BODataSource` | `pipelite.baseobjs` | Base data source class |
| `BOTransformer` | `pipelite.baseobjs` | Base transformer class |
| `BOPipeline` | `pipelite.baseobjs` | Base pipeline class |
| `plDataset` | `pipelite.plDataset` | Dataset wrapper |
| `plDatasets` | `pipelite.plDatasets` | Dataset collection |

### Module Structure

```
pipelite/
├── __init__.py           # Package entry point, main()
├── pipelineProcess.py    # Pipeline execution
├── plObject.py           # Base object class
├── plDataset.py          # Dataset wrapper
├── plDatasets.py         # Dataset collection
├── plConfig.py           # Object configuration
├── constants.py          # Constants and defaults
│
├── baseobjs/             # Abstract base classes
│   ├── plBaseObject.py   # Base for all objects
│   ├── BODataSource.py   # Base data source
│   ├── BOTransformer.py  # Base transformer
│   └── BOPipeline.py     # Base pipeline
│
├── config/               # Configuration management
│   ├── plConfig.py       # Pipeline config loader
│   └── cmdLineConfig.py  # Command line parser
│
├── datasources/          # Data source implementations
│   ├── csvFileDS.py
│   ├── excelFileDS.py
│   ├── parquetFileDS.py
│   ├── odbcDS.py
│   ├── xesFileDS.py
│   ├── sapRfcTableDS.py
│   └── piDS.py
│
├── transformers/         # Transformer implementations
│   ├── passthroughTR.py
│   ├── renamecolTR.py
│   ├── extractstrTR.py
│   ├── concatTR.py
│   ├── joinTR.py
│   ├── lookupTR.py
│   ├── jinjaTR.py
│   └── profileTR.py
│
├── pipelines/            # Pipeline implementations
│   └── sequentialPL.py
│
└── utils/                # Utilities
    ├── log.py            # Logging
    ├── plReport.py       # Single report
    └── plReports.py      # Report collection
```

---

## Getting Started with Development

### 1. Clone and Setup

```bash
git clone https://github.com/datacorner/pipelite.git
cd pipelite
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2. Run Tests

```bash
python -m pytest tests/ -v
```

### 3. Create Custom Components

See [Extending pipelite](extending.md) for detailed guides.

---

## Design Principles

pipelite follows these design principles:

1. **Configuration over Code**: Pipelines defined in JSON, not Python
2. **Extensibility**: Easy to add new data sources and transformers
3. **Simplicity**: Minimal dependencies, straightforward architecture
4. **Transparency**: Comprehensive logging and reporting

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.10+ |
| Data Processing | pandas |
| Configuration | JSON with JSONPath |
| Validation | JSON Schema |
| Templating | Jinja2 |
| Logging | Python logging |

---

[← Back to Documentation Index](../index.md) | [Architecture →](architecture.md)

