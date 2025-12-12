# API Reference

Complete API documentation for pipelite classes and methods.

---

## Table of Contents

1. [Core Classes](#core-classes)
2. [Base Objects](#base-objects)
3. [Data Containers](#data-containers)
4. [Configuration](#configuration)
5. [Utilities](#utilities)

---

## Core Classes

### pipelineProcess

Main entry point for pipeline execution.

**Module**: `pipelite.pipelineProcess`

```python
class pipelineProcess(plObject):
    """Orchestrates pipeline execution."""
    
    @staticmethod
    def getLogger(config) -> log:
        """Create logger from configuration.
        
        Args:
            config: Pipeline configuration object
            
        Returns:
            Configured logger instance
            
        Raises:
            Exception: If configuration is None
        """
    
    def process(self) -> plReports:
        """Execute the pipeline.
        
        Returns:
            Execution reports
        """
    
    def create(self) -> BOPipeline:
        """Instantiate the pipeline from configuration.
        
        Returns:
            Pipeline instance or None on error
        """
```

**Example:**

```python
from pipelite.pipelineProcess import pipelineProcess
from pipelite.config.cmdLineConfig import cmdLineConfig

config = cmdLineConfig.set_config(cfg="pipeline.json")
log = pipelineProcess.getLogger(config)
reports = pipelineProcess(config, log).process()
```

---

## Base Objects

### plObject

Base class for all pipelite objects.

**Module**: `pipelite.plObject`

```python
class plObject:
    """Base class providing config and logging."""
    
    def __init__(self, config, log):
        """Initialize with configuration and logger.
        
        Args:
            config: Configuration object
            log: Logger instance
        """
        self.config = config
        self.log = log
```

### plBaseObject

Base class for instantiable objects.

**Module**: `pipelite.baseobjs.plBaseObject`

```python
class plBaseObject(plObject):
    """Base class for ETL objects."""
    
    id: str          # Object identifier
    objtype: str     # Object type (extractor/transformer/loader)
    executed: bool   # Execution flag
    
    @property
    def parametersValidationFile(self) -> str:
        """Path to JSON schema for parameter validation."""
    
    def validateParametersCfg(self, schemaFile, params) -> bool:
        """Validate parameters against JSON schema.
        
        Args:
            schemaFile: Path to validation schema
            params: Parameters to validate
            
        Returns:
            True if valid
        """
    
    def initialize(self, params) -> bool:
        """Initialize the object.
        
        Args:
            params: Configuration parameters
            
        Returns:
            True if successful
        """
    
    @staticmethod
    def instantiate(fullClassPath, config, log):
        """Dynamically instantiate a class.
        
        Args:
            fullClassPath: Full module.class path
            config: Configuration object
            log: Logger instance
            
        Returns:
            Instantiated object or None
        """
```

### BODataSource

Abstract base class for data sources.

**Module**: `pipelite.baseobjs.BODataSource`

```python
class BODataSource(plBaseObject):
    """Base class for extractors and loaders."""
    
    @abstractmethod
    def initialize(self, cfg) -> bool:
        """Initialize data source.
        
        Args:
            cfg: Configuration object
            
        Returns:
            True if successful
        """
    
    @abstractmethod
    def read(self) -> plDataset:
        """Read data from source.
        
        Returns:
            Dataset containing the data
        """
    
    @abstractmethod
    def write(self, dataset) -> bool:
        """Write data to destination.
        
        Args:
            dataset: Dataset to write
            
        Returns:
            True if successful
        """
```

### BOTransformer

Abstract base class for transformers.

**Module**: `pipelite.baseobjs.BOTransformer`

```python
class BOTransformer(plBaseObject):
    """Base class for data transformers."""
    
    dsInputs: list   # Input dataset IDs
    dsOutputs: list  # Output dataset IDs
    
    @abstractmethod
    def initialize(self, params) -> bool:
        """Initialize transformer.
        
        Args:
            params: Configuration parameters
            
        Returns:
            True if successful
        """
    
    @abstractmethod
    def process(self, dsTransformerInputs) -> plDatasets:
        """Process input datasets.
        
        Args:
            dsTransformerInputs: Input dataset collection
            
        Returns:
            Output dataset collection
        """
```

### BOPipeline

Abstract base class for pipelines.

**Module**: `pipelite.baseobjs.BOPipeline`

```python
class BOPipeline(plBaseObject):
    """Base class for pipeline execution."""
    
    etlObjects: list  # All ETL objects
    
    @property
    def extractors(self) -> list:
        """Get all extractor objects."""
    
    @property
    def loaders(self) -> list:
        """Get all loader objects."""
    
    @property
    def transformers(self) -> list:
        """Get all transformer objects."""
    
    def getObjectFromId(self, id) -> plBaseObject:
        """Get object by ID.
        
        Args:
            id: Object identifier
            
        Returns:
            Matching object or None
        """
    
    def initialize(self) -> bool:
        """Initialize all ETL objects.
        
        Returns:
            True if all objects initialized successfully
        """
    
    @abstractmethod
    def prepare(self) -> bool:
        """Prepare pipeline for execution.
        
        Returns:
            True if successful
        """
    
    @abstractmethod
    def execute(self) -> plReports:
        """Execute the pipeline.
        
        Returns:
            Execution reports
        """
    
    @abstractmethod
    def terminate(self) -> bool:
        """Cleanup after execution.
        
        Returns:
            True if successful
        """
    
    @abstractmethod
    def beforeProcess(self) -> bool:
        """Pre-execution hook.
        
        Returns:
            True to continue, False to abort
        """
    
    @abstractmethod
    def afterProcess(self) -> bool:
        """Post-execution hook.
        
        Returns:
            True if successful
        """
```

---

## Data Containers

### plDataset

Dataset wrapper around pandas DataFrame.

**Module**: `pipelite.plDataset`

```python
class plDataset(plObject):
    """Encapsulates dataset management."""
    
    id: str  # Dataset identifier
    
    @property
    def columns(self) -> pd.Index:
        """Get column names."""
    
    @property
    def count(self) -> int:
        """Get row count."""
    
    def reset(self):
        """Reset to empty DataFrame."""
    
    def get(self) -> pd.DataFrame:
        """Get underlying DataFrame.
        
        Returns:
            pandas DataFrame
        """
    
    def set(self, value, defaultype=None):
        """Set data from list or DataFrame.
        
        Args:
            value: List or DataFrame
            defaultype: Default type for list
        """
    
    def copy(self) -> plDataset:
        """Create deep copy.
        
        Returns:
            New plDataset instance
        """
    
    def read_csv(self, filename, separator, encoding):
        """Read from CSV file.
        
        Args:
            filename: Path to file
            separator: Column delimiter
            encoding: File encoding
        """
    
    def write_csv(self, filename, separator, encoding):
        """Write to CSV file.
        
        Args:
            filename: Path to file
            separator: Column delimiter
            encoding: File encoding
        """
    
    def read_excel(self, filename, sheet=0):
        """Read from Excel file.
        
        Args:
            filename: Path to file
            sheet: Sheet name or index
        """
    
    def write_parquet(self, filename, compression='gzip'):
        """Write to Parquet file.
        
        Args:
            filename: Path to file
            compression: Compression codec
        """
    
    def concatWith(self, other, keys=None):
        """Concatenate with another dataset.
        
        Args:
            other: Dataset to concatenate
            keys: Optional keys for concat
        """
    
    def joinWith(self, ds, on, how='inner'):
        """Join with another dataset.
        
        Args:
            ds: Dataset to join
            on: Column name for join
            how: Join type (inner, left, right, outer)
        """
    
    def renameColumn(self, oldName, newName):
        """Rename a column.
        
        Args:
            oldName: Current column name
            newName: New column name
        """
    
    def dropColumn(self, column):
        """Remove a column.
        
        Args:
            column: Column name to drop
        """
    
    def subString(self, columnName, start, length):
        """Extract substring from column.
        
        Args:
            columnName: Column to modify
            start: Start index
            length: Number of characters
        """
    
    def profile(self, maxvaluecounts=10) -> dict:
        """Generate data profile.
        
        Args:
            maxvaluecounts: Max distinct values
            
        Returns:
            Profile dictionary
        """
```

### plDatasets

Collection of plDataset objects.

**Module**: `pipelite.plDatasets`

```python
class plDatasets:
    """Manages collection of datasets."""
    
    @property
    def count(self) -> int:
        """Number of datasets."""
    
    @property
    def totalRowCount(self) -> int:
        """Total rows across all datasets."""
    
    @property
    def empty(self) -> bool:
        """True if no datasets."""
    
    @property
    def names(self) -> list:
        """List of dataset IDs."""
    
    def add(self, dataset):
        """Add a dataset.
        
        Args:
            dataset: plDataset to add
        """
    
    def merge(self, other):
        """Merge another collection.
        
        Args:
            other: plDatasets to merge
        """
    
    def getFromId(self, id) -> plDataset:
        """Get dataset by ID.
        
        Args:
            id: Dataset identifier
            
        Returns:
            Matching dataset or None
        """
    
    def isInside(self, id) -> bool:
        """Check if dataset exists.
        
        Args:
            id: Dataset identifier
            
        Returns:
            True if exists
        """
```

---

## Configuration

### plConfig (Pipeline Config)

Main configuration loader.

**Module**: `pipelite.config.plConfig`

```python
class plConfig:
    """Pipeline configuration manager."""
    
    def load(self, filename) -> bool:
        """Load configuration from JSON file.
        
        Args:
            filename: Path to JSON file
            
        Returns:
            True if successful
        """
    
    def getParameter(self, paramPath, default=None):
        """Get parameter using JSONPath.
        
        Args:
            paramPath: JSONPath expression
            default: Default value
            
        Returns:
            Parameter value or default
        """
    
    def addParameter(self, name, value):
        """Add dynamic parameter.
        
        Args:
            name: Parameter name
            value: Parameter value
        """
```

### plConfig (Object Config)

Object-specific configuration wrapper.

**Module**: `pipelite.plConfig`

```python
class plConfig:
    """Object configuration wrapper."""
    
    className: str      # Class path
    parameters: dict    # Object parameters
    inputs: list        # Input IDs (transformers)
    outputs: list       # Output IDs (transformers)
    validation: str     # Validation schema path
    id: str            # Object ID
    
    def validate(self) -> bool:
        """Validate configuration.
        
        Returns:
            True if valid
        """
    
    def initialize(self) -> bool:
        """Initialize from config.
        
        Returns:
            True if successful
        """
    
    def getParameter(self, name, default=None):
        """Get specific parameter.
        
        Args:
            name: Parameter name
            default: Default value
            
        Returns:
            Parameter value or default
        """
```

### cmdLineConfig

Command line configuration parser.

**Module**: `pipelite.config.cmdLineConfig`

```python
class cmdLineConfig:
    """Command line configuration."""
    
    @staticmethod
    def readConfig(parser) -> plConfig:
        """Parse command line arguments.
        
        Args:
            parser: ArgumentParser instance
            
        Returns:
            Configuration object
        """
    
    @staticmethod
    def set_config(cfg) -> plConfig:
        """Load configuration from file path.
        
        Args:
            cfg: Path to configuration file
            
        Returns:
            Configuration object
        """
```

---

## Utilities

### log

Logging wrapper.

**Module**: `pipelite.utils.log`

```python
class log:
    """Logging manager."""
    
    @property
    def warningCounts(self) -> int:
        """Number of warnings logged."""
    
    @property
    def errorCounts(self) -> int:
        """Number of errors logged."""
    
    def info(self, *message):
        """Log info message."""
    
    def debug(self, *message):
        """Log debug message."""
    
    def warning(self, *message):
        """Log warning message."""
    
    def error(self, *message):
        """Log error message."""
```

### plReports

Execution report collection.

**Module**: `pipelite.utils.plReports`

```python
class plReports:
    """Pipeline execution reports."""
    
    def addEntry(self, id, objtype):
        """Add report entry.
        
        Args:
            id: Object ID
            objtype: Object type
        """
    
    def getFromId(self, id) -> plReport:
        """Get report by ID.
        
        Args:
            id: Object ID
            
        Returns:
            Report entry
        """
    
    def getFullSTRReport(self) -> str:
        """Get formatted string report.
        
        Returns:
            Report string
        """
    
    def getFullJSONReport(self) -> dict:
        """Get JSON report.
        
        Returns:
            Report dictionary
        """
```

---

## Constants

**Module**: `pipelite.constants`

```python
# Encoding
ENCODING = "utf-8"
DEFCSVSEP = ","

# Log levels
LOG_LEVEL_DEBUG = "DEBUG"
LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_WARNING = "WARNING"
LOG_LEVEL_ERROR = "ERROR"

# Configuration paths
PLJSONCFG_EXTRACTOR = "$.extractors"
PLJSONCFG_LOADER = "$.loaders"
PLJSONCFG_TRANSFORMER = "$.transformers"

# Logger defaults
TRACE_DEFAULT_LEVEL = logging.DEBUG
TRACE_DEFAULT_FILENAME = "pipelite.log"
TRACE_DEFAULT_MAXBYTES = 1000000
```

---

## Next Steps

- [Extending pipelite](extending.md)
- [Contributing](contributing.md)
- [Architecture](architecture.md)

---

[← Back to Technical Index](index.md) | [Extending pipelite →](extending.md)

