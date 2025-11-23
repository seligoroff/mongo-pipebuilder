# Development Guide

This document provides guidelines for developing and contributing to `mongo-pipebuilder`.

## Development Setup

### Prerequisites

- Python 3.8 or higher
- pip
- (Optional) virtual environment

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/seligoroff/mongo-pipebuilder.git
   cd mongo-pipebuilder
   ```

2. **Create and activate a virtual environment** (recommended):
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install the package in editable mode:**
   ```bash
   pip install -e .
   ```
   
   This installs the package in development mode, so changes to the source code are immediately available without reinstalling.

4. **Install development dependencies:**
   ```bash
   pip install -r requirements-dev.txt
   ```
   
   Or use the Makefile:
   ```bash
   make install-dev
   ```

### Running Tests

Once the package is installed in editable mode, you can run tests:

```bash
# Run all tests
pytest -vv

# Run tests with coverage (generates multiple report formats)
pytest -vv  # Coverage is configured in pyproject.toml

# Or use Makefile command
make test-cov
```

**Coverage Reports Generated:**

After running tests, you'll get coverage reports in multiple formats:

1. **Terminal output** - shows coverage summary with missing lines marked
2. **HTML report** - `tests/htmlcov/index.html` - interactive web interface
   - Open in browser: `open tests/htmlcov/index.html` (macOS) or `xdg-open tests/htmlcov/index.html` (Linux)
3. **JSON report** - `coverage.json` - machine-readable format for CI/CD
4. **XML report** - `coverage.xml` - for tools like SonarQube, Codecov

**Example commands:**

```bash
# Run a specific test file
pytest tests/test_builder.py -vv

# Run a specific test
pytest tests/test_builder.py::TestPipelineBuilder::test_match_stage -vv

# Run without coverage (faster)
pytest -vv --no-cov

# Run integration tests (requires MongoDB and pymongo)
pytest tests/integration -m integration

# Run only unit tests (skip integration tests)
pytest -m "not integration"
```

### Quick Verification

After installation, verify everything works:

```bash
python -c "from mongo_pipebuilder import PipelineBuilder; builder = PipelineBuilder(); pipeline = builder.match({'test': 1}).build(); print('✅ Setup successful!', pipeline)"
```

**Note:** If you see `ModuleNotFoundError: No module named 'mongo_pipebuilder'`, make sure you've run `pip install -e .` from the project root directory.

## Design Philosophy

The Builder Pattern provides:

1. **Type safety**: Full type hints for IDE support and static analysis
2. **Maintainability**: Explicit, readable code instead of template-based generation
3. **Testability**: Easy to unit test pipeline construction
4. **Flexibility**: Extensible API for adding new stages

## Current Implementation

The `PipelineBuilder` class provides a fluent interface for building MongoDB aggregation pipelines. It currently supports:

- Basic stages: `match`, `project`, `group`, `sort`, `limit`, `skip`
- Join operations: `lookup` (with optional nested pipelines)
- Array operations: `unwind`
- Field manipulation: `add_fields`
- Custom stages: `add_stage` for advanced use cases

## Architecture Decisions

### Why Builder Pattern?

- **Fluent interface**: Enables readable, chainable method calls
- **Immutability**: Each method returns a new builder instance (via `Self`)
- **Type safety**: Full type hints for IDE support and static analysis
- **Extensibility**: Easy to add new stages without breaking existing code

### Why No Dependencies?

The package intentionally has zero external dependencies to:
- Keep it lightweight
- Avoid version conflicts
- Make it easy to integrate into any project
- Reduce maintenance burden

### Design Principles

1. **Explicit over implicit**: All stages are explicitly added via methods
2. **Fail-safe defaults**: Methods skip empty/null values rather than erroring
3. **Flexibility**: `add_stage()` allows any MongoDB stage for advanced use cases
4. **Pythonic**: Follows Python conventions and type hints

## Extending the Package

### Adding New Stage Methods

To add a new MongoDB aggregation stage:

1. Add a method to `PipelineBuilder` class in `src/mongo_pipebuilder/builder.py`:

```python
def facet(self, facets: Dict[str, List[Dict[str, Any]]]) -> Self:
    """Adds a $facet stage."""
    if facets:
        self._stages.append({"$facet": facets})
    return self
```

2. Add tests in `tests/test_builder.py`
3. Update README.md with documentation
4. Add example usage if applicable

### Testing Strategy

- **Unit tests**: Test each method independently
- **Integration tests**: Test complete pipelines
- **Edge cases**: Empty inputs, None values, etc.
- **Type checking**: Run `mypy` to ensure type safety
- **Coverage**: Aim for >90% code coverage

### Coverage Analysis

Coverage reports are automatically generated in multiple formats:

**1. Terminal Report** (`term-missing`)
- Shows coverage percentage per file
- Highlights missing lines with line numbers
- Quick overview during development

**2. HTML Report** (`tests/htmlcov/index.html`)
- Interactive web interface
- Click on files to see covered/uncovered lines
- Line-by-line highlighting
- Best for detailed analysis

**3. JSON Report** (`coverage.json`)
- Machine-readable format
- Structure: `{meta, files, totals}`
- Useful for CI/CD pipelines and automated analysis
- Contains detailed per-file statistics

**4. XML Report** (`coverage.xml`)
- Standard format for CI/CD tools
- Compatible with SonarQube, Codecov, etc.

**Current Coverage:**
```
TOTAL: 92.41% (59 statements, 2 missing, 20 branches, 4 partial)
```

**Analyzing JSON Coverage Report:**

The JSON report (`coverage.json`) is structured for easy programmatic analysis:

```python
import json

# Load coverage report
with open('coverage.json') as f:
    coverage = json.load(f)

# Overall statistics
totals = coverage['totals']
print(f"Total coverage: {totals['percent_covered']:.2f}%")
print(f"Statements: {totals['num_statements']}")
print(f"Missing lines: {totals['missing_lines']}")

# Per-file analysis
for filepath, file_data in coverage['files'].items():
    summary = file_data['summary']
    if summary['missing_lines'] > 0:
        missing_lines = file_data.get('missing_lines', [])
        print(f"\n{filepath}:")
        print(f"  Coverage: {summary['percent_covered']:.2f}%")
        print(f"  Missing lines: {missing_lines[:10]}")  # First 10
```

**Structure of `coverage.json`:**

```json
{
  "meta": {
    "version": "7.12.0",
    "timestamp": "2025-11-23T10:27:00.000000",
    "branch_coverage": true,
    ...
  },
  "totals": {
    "percent_covered": 92.41,
    "num_statements": 59,
    "missing_lines": 2,
    "num_branches": 20,
    ...
  },
  "files": {
    "src/mongo_pipebuilder/builder.py": {
      "summary": {
        "num_statements": 56,
        "missing_lines": 2,
        "percent_covered": 92.11,
        ...
      },
      "missing_lines": [14, 15],
      "excluded_lines": [],
      ...
    }
  }
}
```

**Improving Coverage:**
- Check `coverage.json` for `missing_lines` per file
- Review HTML report (`tests/htmlcov/index.html`) to see exact uncovered lines with context
- Add tests for edge cases and error paths
- Use `--cov-branch` flag (already configured) to track branch coverage

### Example Test Structure

```python
def test_match_stage():
    builder = PipelineBuilder()
    pipeline = builder.match({"status": "active"}).build()
    assert pipeline == [{"$match": {"status": "active"}}]

def test_empty_match_skipped():
    builder = PipelineBuilder()
    pipeline = builder.match({}).build()
    assert pipeline == []
```

### Integration Tests

The project includes optional integration tests that verify pipelines work with real MongoDB:

**Setup:**
```bash
# Install pymongo for integration tests (optional)
pip install pymongo

# Optional: Install python-dotenv for .env.testing support
pip install python-dotenv

# Ensure MongoDB is running on localhost:27017
# Or use Docker: docker run -d -p 27017:27017 mongo:latest
```

**Configuration:**

MongoDB connection can be configured via:

1. **Environment variables** (highest priority):
   ```bash
   export MONGODB_TEST_URI="mongodb://localhost:27017/"
   export MONGODB_TEST_TIMEOUT=2000
   export MONGODB_TEST_DB_PREFIX="test_pipebuilder"
   ```

2. **`.env.testing` file** (if `python-dotenv` installed):
   ```bash
   # Copy example file
   cp tests/integration/.env.testing.example tests/integration/.env.testing
   
   # Edit tests/integration/.env.testing with your settings
   ```

3. **Defaults** (if nothing configured):
   - URI: `mongodb://localhost:27017/`
   - Timeout: `2000ms`
   - DB prefix: `test_pipebuilder`

**Example `.env.testing`:**
```ini
MONGODB_TEST_URI=mongodb://localhost:27017/
MONGODB_TEST_TIMEOUT=2000
MONGODB_TEST_DB_PREFIX=test_pipebuilder
```

**Running Integration Tests:**
```bash
# Run all integration tests
pytest tests/integration -m integration

# Run only unit tests (skip integration)
pytest -m "not integration"

# Run all tests (unit + integration if MongoDB available)
pytest
```

**Test Data Factories:**
The `tests/integration/factories.py` module provides factories for generating test data:
- `UserFactory` - Create test user documents
- `OrderFactory` - Create test order documents  
- `ProductFactory` - Create test product documents

These factories can be used independently of MongoDB for generating test data structures.

**Note:** Integration tests are optional and skipped automatically if:
- `pymongo` is not installed
- MongoDB is not running or not accessible
- Tests are run in CI/CD without MongoDB configured

This ensures the library remains lightweight while providing optional verification against real MongoDB.

## Future Enhancements

### High Priority

1. **Additional stages**: `$facet`, `$bucket`, `$bucketAuto`, `$merge`, `$replaceRoot`, `$replaceWith`
2. **Pipeline validation**: Validate pipeline structure before execution
3. **Better error messages**: More descriptive errors for invalid inputs

### Medium Priority

1. **Query helpers**: Convenience methods for common patterns
2. **Pipeline optimization**: Detect and optimize common patterns
3. **Documentation**: More examples and use cases

### Low Priority

1. **Async support**: Async/await patterns (if needed)
2. **Performance profiling**: Benchmark against manual pipeline construction
3. **IDE plugins**: Autocomplete suggestions for MongoDB operators

## Code Style

- Follow PEP 8
- Use type hints for all public methods
- Keep methods focused and single-purpose
- Add docstrings for all public methods
- Use `Self` type for fluent interface methods

## Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md` (if exists)
3. Run tests: `pytest`
4. Run type checker: `mypy src/`
5. Build package: `python -m build`
6. Publish to PyPI: `twine upload dist/*`

## Questions to Consider

When extending this package, consider:

1. **Should this be a core method or use `add_stage()`?**
   - If it's a common MongoDB stage → core method
   - If it's specialized → use `add_stage()`

2. **Does this break backward compatibility?**
   - Avoid breaking changes in minor versions
   - Use deprecation warnings for major changes

3. **Is this adding unnecessary complexity?**
   - Keep the API simple and focused
   - Don't over-engineer

## Related Resources

- [MongoDB Aggregation Pipeline Documentation](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/)
- [Python Type Hints](https://docs.python.org/3/library/typing.html)
- [Builder Pattern](https://refactoring.guru/design-patterns/builder)

