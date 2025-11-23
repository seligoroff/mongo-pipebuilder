# Integration Tests

Integration tests verify that pipelines built with `PipelineBuilder` work correctly with real MongoDB.

## Quick Start

```bash
# Install dependencies
pip install pymongo

# Optional: Install python-dotenv for .env.testing support
pip install python-dotenv

# Run integration tests
pytest tests/integration -m integration
```

## Configuration

### Option 1: Environment Variables (Recommended for CI/CD)

```bash
export MONGODB_TEST_URI="mongodb://localhost:27017/"
export MONGODB_TEST_TIMEOUT=2000
export MONGODB_TEST_DB_PREFIX="test_pipebuilder"
pytest tests/integration -m integration
```

### Option 2: `.env.testing` File (Recommended for Local Development)

```bash
# Copy example file
cp tests/integration/.env.testing.example tests/integration/.env.testing

# Edit tests/integration/.env.testing with your settings
# Then run tests
pytest tests/integration -m integration
```

### Option 3: Defaults

If nothing is configured, tests use:
- URI: `mongodb://localhost:27017/`
- Timeout: `2000ms`
- DB prefix: `test_pipebuilder`

## Configuration Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_TEST_URI` | `mongodb://localhost:27017/` | MongoDB connection string |
| `MONGODB_TEST_TIMEOUT` | `2000` | Connection timeout in milliseconds |
| `MONGODB_TEST_DB_PREFIX` | `test_pipebuilder` | Prefix for temporary test database names (only if `MONGODB_TEST_DB_NAME` not set) |
| `MONGODB_TEST_DB_NAME` | `None` | **Optional:** Fixed database name. If set, uses this database for all tests (database is NOT dropped) |

## Test Database

**By default**, each test creates a temporary database with a unique name:
- Format: `{prefix}_{random_uuid}`
- Example: `test_pipebuilder_a1b2c3d4`
- Automatically dropped after test completes

**To use a fixed database name**, set `MONGODB_TEST_DB_NAME` in `.env.testing`:
```ini
MONGODB_TEST_DB_NAME=my_test_db
```
- Uses the same database for all tests
- Database is **NOT** dropped after tests (you manage it manually)
- Useful for debugging or when you want to inspect test data

## Examples

### Local MongoDB
```bash
pytest tests/integration -m integration
```

### Remote MongoDB
```bash
export MONGODB_TEST_URI="mongodb://user:pass@remote-host:27017/"
pytest tests/integration -m integration
```

### MongoDB with Authentication
```bash
export MONGODB_TEST_URI="mongodb://username:password@localhost:27017/?authSource=admin"
pytest tests/integration -m integration
```

### Docker MongoDB
```bash
# Start MongoDB in Docker
docker run -d -p 27017:27017 --name mongodb-test mongo:latest

# Run tests
pytest tests/integration -m integration

# Stop MongoDB
docker stop mongodb-test && docker rm mongodb-test
```

## Skipping Tests

Tests are automatically skipped if:
- `pymongo` is not installed
- MongoDB is not running or not accessible
- Connection timeout is exceeded

This ensures tests don't fail in environments without MongoDB.

