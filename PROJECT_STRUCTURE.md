# Project Structure

```
mongo-pipebuilder/
├── src/
│   └── mongo_pipebuilder/
│       ├── __init__.py          # Package exports
│       └── builder.py            # PipelineBuilder class
├── tests/
│   └── test_builder.py           # Test suite
├── examples/
│   └── examples.py               # Usage examples
├── .gitignore                    # Git ignore rules
├── CHANGELOG.md                  # Version history
├── CONTEXT.md                    # Background and context
├── DEVELOPMENT.md                # Development guidelines
├── LICENSE                       # MIT License
├── Makefile                      # Development commands
├── PROJECT_STRUCTURE.md          # This file
├── pyproject.toml                # Package configuration
├── README.md                     # User documentation
└── requirements-dev.txt          # Development dependencies
```

## Key Files

### Source Code
- `src/mongo_pipebuilder/builder.py` - Main PipelineBuilder implementation
- `src/mongo_pipebuilder/__init__.py` - Package initialization and exports

### Documentation
- `README.md` - User-facing documentation with examples
- `DEVELOPMENT.md` - Guidelines for contributors
- `CHANGELOG.md` - Version history

### Testing
- `tests/test_builder.py` - Comprehensive test suite

### Examples
- `examples/examples.py` - Real-world usage examples

### Configuration
- `pyproject.toml` - Package metadata and build configuration
- `requirements-dev.txt` - Development dependencies
- `Makefile` - Convenience commands for development
- `.gitignore` - Git ignore patterns

## Quick Start for New Developers

1. Read `DEVELOPMENT.md` for guidelines
2. Review `src/mongo_pipebuilder/builder.py` for implementation
3. Check `tests/test_builder.py` for test patterns
4. See `examples/examples.py` for usage examples

## Building and Testing

```bash
# Install development dependencies
make install-dev

# Run tests
make test

# Run type checker
make type-check

# Format code
make format

# Build package
make build
```

















