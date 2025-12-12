# Installation Guide

This guide covers all installation methods and options for pipelite.

---

## Table of Contents

1. [Requirements](#requirements)
2. [Quick Installation](#quick-installation)
3. [Installation Options](#installation-options)
4. [Optional Dependencies](#optional-dependencies)
5. [Verifying Installation](#verifying-installation)
6. [Upgrading](#upgrading)
7. [Troubleshooting](#troubleshooting)

---

## Requirements

### System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| Python | 3.10 | 3.11+ |
| Memory | 512 MB | 2 GB+ |
| Disk Space | 100 MB | 500 MB |

### Operating Systems

- **Linux**: Ubuntu 18.04+, CentOS 7+, Debian 10+
- **macOS**: 10.15 (Catalina) or later
- **Windows**: Windows 10 or later

---

## Quick Installation

The fastest way to install pipelite:

```bash
pip install pipelite
```

---

## Installation Options

### Option 1: Install from PyPI (Recommended)

```bash
# Basic installation
pip install pipelite

# Install with specific version
pip install pipelite==0.1.9.2

# Upgrade to latest version
pip install --upgrade pipelite
```

### Option 2: Install from Source

```bash
# Clone the repository
git clone https://github.com/datacorner/pipelite.git
cd pipelite

# Install in development mode
pip install -e .

# Or install normally
pip install .
```

### Option 3: Install in Virtual Environment (Recommended)

```bash
# Create virtual environment
python -m venv pipelite-env

# Activate (Linux/macOS)
source pipelite-env/bin/activate

# Activate (Windows)
pipelite-env\Scripts\activate

# Install pipelite
pip install pipelite
```

### Option 4: Install with Conda

```bash
# Create conda environment
conda create -n pipelite python=3.11

# Activate environment
conda activate pipelite

# Install via pip (pipelite not on conda-forge yet)
pip install pipelite
```

---

## Optional Dependencies

pipelite has optional dependencies for specific features:

### Database Connectivity (ODBC)

```bash
pip install pyodbc
```

**Additional requirements by database:**

| Database | Driver Installation |
|----------|---------------------|
| SQL Server | Windows: Built-in<br>macOS: `brew install microsoft/mssql-release/msodbcsql17`<br>Linux: [Microsoft Docs](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/) |
| PostgreSQL | `apt install odbc-postgresql` or download from postgresql.org |
| MySQL | Download from dev.mysql.com |
| SQLite | Usually included with Python |

### SAP Connectivity

```bash
pip install pyrfc
```

**Note**: Requires SAP NW RFC SDK. See [pyrfc documentation](https://sap.github.io/PyRFC/).

### Parquet File Support

```bash
pip install pyarrow
```

### All Optional Dependencies

```bash
pip install pipelite[all]
# or
pip install pyodbc pyrfc pyarrow
```

---

## Verifying Installation

### Check Version

```bash
python -c "import pipelite; print('pipelite installed successfully')"
```

### Test Command Line

```bash
pipelite --help
```

Expected output:

```
usage: pipelite [-h] -cfg CFG

options:
  -h, --help  show this help message and exit
  -cfg CFG    Pipeline Configuration file with all configuration details (JSON)
```

### Run a Test Pipeline

Create `test_pipeline.json`:

```json
{
    "classname": "pipelite.pipelines.sequentialPL",
    "extractors": [],
    "loaders": [],
    "transformers": [],
    "config": {
        "logger": {
            "level": "INFO",
            "path": "./",
            "filename": "test.log"
        }
    }
}
```

Run:

```bash
pipelite -cfg test_pipeline.json
```

---

## Upgrading

### Upgrade to Latest Version

```bash
pip install --upgrade pipelite
```

### Upgrade to Specific Version

```bash
pip install pipelite==X.Y.Z
```

### Check Current Version

```bash
pip show pipelite
```

---

## Troubleshooting

### Common Installation Issues

#### Issue: `pip: command not found`

**Solution**: Use `pip3` or `python -m pip`:

```bash
python -m pip install pipelite
```

#### Issue: Permission denied

**Solution**: Use `--user` flag or virtual environment:

```bash
pip install --user pipelite
```

#### Issue: `No matching distribution found`

**Solution**: Upgrade pip and try again:

```bash
pip install --upgrade pip
pip install pipelite
```

#### Issue: Python version not supported

**Solution**: Install Python 3.10 or higher:

```bash
# Ubuntu/Debian
sudo apt install python3.11

# macOS
brew install python@3.11

# Windows
# Download from python.org
```

### Dependency Conflicts

If you encounter dependency conflicts:

```bash
# Create a fresh virtual environment
python -m venv fresh-env
source fresh-env/bin/activate

# Install only pipelite
pip install pipelite
```

### Missing System Libraries

#### Linux

```bash
# For pyodbc
sudo apt install unixodbc-dev

# For general build tools
sudo apt install build-essential python3-dev
```

#### macOS

```bash
# Install Xcode command line tools
xcode-select --install

# For ODBC
brew install unixodbc
```

---

## Uninstalling

```bash
pip uninstall pipelite
```

---

## Next Steps

- [Configuration Guide](configuration.md)
- [Quick Start](../quickstart.md)
- [Create Your First Pipeline](../howto/create-first-pipeline.md)

---

[← Back to Guide Index](index.md) | [Configuration →](configuration.md)

