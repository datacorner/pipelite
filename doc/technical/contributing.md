# Contributing to pipelite

Guidelines for contributing to the pipelite project.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Development Setup](#development-setup)
3. [Code Style](#code-style)
4. [Testing](#testing)
5. [Submitting Changes](#submitting-changes)
6. [Documentation](#documentation)
7. [Release Process](#release-process)

---

## Getting Started

### Ways to Contribute

| Type | Description |
|------|-------------|
| **Bug Reports** | Report issues you encounter |
| **Feature Requests** | Suggest new functionality |
| **Documentation** | Improve docs, fix typos |
| **Code** | Fix bugs, add features |
| **Testing** | Add test cases |

### Before Contributing

1. Check [existing issues](https://github.com/datacorner/pipelite/issues)
2. Read this guide completely
3. Understand the [architecture](architecture.md)

---

## Development Setup

### 1. Fork and Clone

```bash
# Fork on GitHub first, then:
git clone https://github.com/YOUR_USERNAME/pipelite.git
cd pipelite
```

### 2. Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate     # Windows
```

### 3. Install in Development Mode

```bash
pip install -e .
pip install pytest  # For testing
```

### 4. Create Feature Branch

```bash
git checkout -b feature/your-feature-name
```

---

## Code Style

### Python Style Guide

Follow [PEP 8](https://peps.python.org/pep-0008/) with these specifics:

```python
# Good
class MyTransformer(BOTransformer):
    """Short description.
    
    Longer description if needed.
    """
    
    def __init__(self, config, log):
        super().__init__(config, log)
        self.my_param = None
    
    def initialize(self, params) -> bool:
        """Initialize the transformer.
        
        Args:
            params: Configuration parameters
            
        Returns:
            True if successful
        """
        try:
            self.my_param = params.getParameter("param", "default")
            return True
        except Exception as e:
            self.log.error(f"Init failed: {e}")
            return False
```

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Classes | PascalCase | `MyDataSource` |
| Functions/Methods | snake_case | `get_data()` |
| Variables | snake_case | `my_variable` |
| Constants | UPPER_SNAKE | `MAX_RETRIES` |
| Files | camelCase + suffix | `myDataDS.py`, `filterTR.py` |

### Type Hints

Use type hints for public methods:

```python
from typing import Optional, List

def process(self, inputs: plDatasets) -> plDatasets:
    """Process data."""
    pass

def get_value(self, name: str, default: Optional[str] = None) -> str:
    """Get configuration value."""
    pass
```

### Docstrings

Use Google-style docstrings:

```python
def my_function(param1: str, param2: int = 10) -> bool:
    """Short description.
    
    Longer description if needed. Can span multiple lines
    and paragraphs.
    
    Args:
        param1: Description of param1
        param2: Description of param2. Defaults to 10.
        
    Returns:
        Description of return value
        
    Raises:
        ValueError: When param1 is empty
        
    Example:
        >>> my_function("test", 20)
        True
    """
```

### Import Order

```python
# Standard library
import os
import json
from typing import Optional

# Third-party
import pandas as pd
import requests

# Local
import pipelite.constants as C
from pipelite.baseobjs.BODataSource import BODataSource
from pipelite.plDataset import plDataset
```

---

## Testing

### Running Tests

```bash
# All tests
python -m pytest tests/ -v

# Specific test file
python -m pytest tests/test_DS.py -v

# Specific test
python -m pytest tests/test_TR.py::testTransformers::test_csv2csv_concat -v

# With coverage
python -m pytest tests/ --cov=pipelite --cov-report=html
```

### Writing Tests

```python
import unittest
from pipelite.plDataset import plDataset


class TestMyFeature(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_data = [...]
    
    def tearDown(self):
        """Clean up after tests."""
        pass
    
    def test_basic_functionality(self):
        """Test basic functionality works."""
        result = my_function()
        self.assertEqual(result, expected)
    
    def test_edge_case(self):
        """Test edge case handling."""
        with self.assertRaises(ValueError):
            my_function(invalid_input)
    
    def test_error_handling(self):
        """Test error handling."""
        result = my_function(bad_input)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
```

### Test Requirements

- All new features must have tests
- Bug fixes should include regression tests
- Tests should be independent and repeatable
- Use meaningful test method names

---

## Submitting Changes

### Commit Messages

Format:
```
type(scope): Short description

Longer description if needed. Explain what and why,
not how (the code shows how).

Fixes #123
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Code style (no functional change)
- `refactor`: Code refactoring
- `test`: Tests
- `chore`: Build, CI, etc.

Examples:
```
feat(datasource): Add REST API data source

Add new data source for reading from REST APIs with support
for GET and POST methods, custom headers, and JSON responses.

Closes #45
```

```
fix(transform): Fix concat with empty datasets

Concatenation now handles empty datasets correctly without
raising an exception.

Fixes #67
```

### Pull Request Process

1. **Update your branch**
   ```bash
   git fetch origin
   git rebase origin/main
   ```

2. **Run tests**
   ```bash
   python -m pytest tests/ -v
   ```

3. **Push changes**
   ```bash
   git push origin feature/your-feature-name
   ```

4. **Create Pull Request**
   - Use descriptive title
   - Reference related issues
   - Describe changes made
   - Include test results

5. **Code Review**
   - Address feedback promptly
   - Keep discussion focused
   - Update PR as needed

### PR Template

```markdown
## Description
Brief description of changes

## Related Issue
Fixes #(issue number)

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation
- [ ] Refactoring

## Checklist
- [ ] Code follows style guidelines
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] All tests pass
```

---

## Documentation

### Documentation Types

| Type | Location | Purpose |
|------|----------|---------|
| Code docstrings | Source files | API documentation |
| User guides | `doc/guide/` | How to use features |
| How-tos | `doc/howto/` | Task-oriented guides |
| Technical | `doc/technical/` | Architecture, internals |

### Documentation Style

- Use clear, simple language
- Include code examples
- Keep paragraphs short
- Use tables for reference material
- Link related topics

### Building Documentation

Documentation is written in Markdown. Preview with any Markdown viewer.

---

## Release Process

### Version Numbering

pipelite uses [Semantic Versioning](https://semver.org/):

```
MAJOR.MINOR.PATCH

0.1.9
│ │ └── Patch: Bug fixes, minor changes
│ └──── Minor: New features, backward compatible
└────── Major: Breaking changes
```

### Release Checklist

1. [ ] All tests pass
2. [ ] Documentation updated
3. [ ] CHANGELOG updated
4. [ ] Version bumped in `pyproject.toml`
5. [ ] PR approved and merged
6. [ ] Tag created
7. [ ] Package published to PyPI

---

## Code of Conduct

### Our Standards

- Be respectful and inclusive
- Accept constructive criticism gracefully
- Focus on what's best for the community
- Show empathy toward others

### Enforcement

Violations may be reported to project maintainers. All complaints will be reviewed and investigated.

---

## Questions?

- Open a [GitHub Discussion](https://github.com/datacorner/pipelite/discussions)
- Check existing [Issues](https://github.com/datacorner/pipelite/issues)
- Read the [documentation](../index.md)

---

## Acknowledgments

Thank you to all contributors who help make pipelite better!

---

[← Back to Technical Index](index.md) | [Documentation Index →](../index.md)

