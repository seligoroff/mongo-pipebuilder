"""
Pytest configuration for integration tests.

These fixtures set up MongoDB connections and test databases.

Configuration:
    - Environment variables: MONGODB_TEST_URI, MONGODB_TEST_TIMEOUT, etc.
    - Optional .env.testing file in tests/integration/ (if python-dotenv installed)
    - Defaults: mongodb://localhost:27017/, timeout=2000ms
"""
import os
import pytest
from typing import Generator, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pymongo import MongoClient

# Try to load .env.testing if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Load .env.testing from tests/integration/ directory
    env_path = os.path.join(os.path.dirname(__file__), ".env.testing")
    if os.path.exists(env_path):
        load_dotenv(env_path)
except ImportError:
    # python-dotenv not installed, skip .env.testing loading
    pass

# Skip all integration tests if MongoDB is not available
try:
    import pymongo
    from pymongo import MongoClient
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    MongoClient = None  # type: ignore


def get_mongodb_config() -> dict:
    """
    Get MongoDB test configuration from environment variables or defaults.
    
    Returns:
        dict with keys: uri, timeout, db_prefix, db_name
    """
    uri = os.getenv("MONGODB_TEST_URI", "mongodb://localhost:27017/")
    
    # Build URI with authentication if username/password provided separately
    username = os.getenv("MONGODB_TEST_USERNAME")
    password = os.getenv("MONGODB_TEST_PASSWORD")
    
    if username and password and "@" not in uri:
        # Add credentials to URI if not already present
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(uri)
        # Reconstruct URI with credentials
        netloc = f"{username}:{password}@{parsed.netloc}" if parsed.netloc else f"{username}:{password}@localhost:27017"
        uri = urlunparse(parsed._replace(netloc=netloc))
    
    return {
        "uri": uri,
        "timeout": int(os.getenv("MONGODB_TEST_TIMEOUT", "2000")),
        "db_prefix": os.getenv("MONGODB_TEST_DB_PREFIX", "test_pipebuilder"),
        "db_name": os.getenv("MONGODB_TEST_DB_NAME", None),  # Optional: fixed DB name
    }


# Register integration marker
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "integration: marks tests as integration tests (requires MongoDB)")


@pytest.fixture(scope="session")
def mongodb_client() -> Generator:
    """
    Create a MongoDB client for testing.
    
    Configuration is read from:
    - Environment variables (MONGODB_TEST_URI, MONGODB_TEST_TIMEOUT)
    - .env.testing file in tests/integration/ (if python-dotenv installed)
    - Defaults: mongodb://localhost:27017/, timeout=2000ms
    
    Skips tests if MongoDB is not available or connection fails.
    """
    if not MONGODB_AVAILABLE:
        pytest.skip("pymongo not installed. Install with: pip install pymongo")
    
    config = get_mongodb_config()
    
    try:
        # Connect to MongoDB using configured URI
        client = MongoClient(
            config["uri"],
            serverSelectionTimeoutMS=config["timeout"]
        )
        # Test connection
        client.server_info()
        yield client
        client.close()
    except Exception as e:
        pytest.skip(f"MongoDB not available at {config['uri']}: {e}")


@pytest.fixture(scope="function")
def test_db(mongodb_client) -> Generator:
    """
    Create a test database for each test.
    
    Database name:
    - If MONGODB_TEST_DB_NAME is set: uses that name (database is NOT dropped)
    - Otherwise: generates temporary name from MONGODB_TEST_DB_PREFIX with random suffix
                 (database is dropped after test completes)
    """
    import uuid
    config = get_mongodb_config()
    
    # Use fixed DB name if provided, otherwise generate temporary name
    if config["db_name"]:
        db_name = config["db_name"]
        drop_db = False  # Don't drop fixed database
    else:
        db_name = f"{config['db_prefix']}_{uuid.uuid4().hex[:8]}"
        drop_db = True  # Drop temporary database
    
    db = mongodb_client[db_name]
    
    yield db
    
    # Cleanup: drop test database only if it was temporary
    if drop_db:
        mongodb_client.drop_database(db_name)


@pytest.fixture
def users_collection(test_db):
    """Create a users collection with test data."""
    from tests.integration.factories import UserFactory
    
    collection = test_db.users
    # Insert test users: mix of active and inactive
    users = UserFactory.create_batch(5, active=True)
    users.extend(UserFactory.create_batch(3, active=False))
    collection.insert_many(users)
    
    return collection


@pytest.fixture
def orders_collection(test_db, users_collection):
    """Create an orders collection with test data linked to users."""
    from tests.integration.factories import OrderFactory
    
    # Get user IDs from users collection
    user_ids = [user["_id"] for user in users_collection.find()]
    
    collection = test_db.orders
    # Create orders linked to actual users
    orders = OrderFactory.create_batch(10, user_ids=user_ids, status="pending")
    orders.extend(OrderFactory.create_batch(5, user_ids=user_ids, status="completed"))
    collection.insert_many(orders)
    
    return collection


@pytest.fixture
def products_collection(test_db):
    """Create a products collection with test data."""
    from tests.integration.factories import ProductFactory
    
    collection = test_db.products
    # Insert test products
    products = ProductFactory.create_batch(10, category="electronics")
    products.extend(ProductFactory.create_batch(5, category="books"))
    collection.insert_many(products)
    
    return collection

