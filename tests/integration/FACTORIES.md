# Test Data Factories

Factories for generating test data for MongoDB integration tests.

## Features

- ✅ **MongoDB ObjectId support** - Automatically uses ObjectId if `bson` is available
- ✅ **Related data** - Create orders linked to users, products with categories, etc.
- ✅ **Batch creation** - Generate multiple documents with variations
- ✅ **Flexible** - Override any field via kwargs
- ✅ **No dependencies** - Works without MongoDB connection (pure Python)

## Usage

### Basic Usage

```python
from tests.integration.factories import UserFactory, OrderFactory, ProductFactory

# Create a single user
user = UserFactory.create(name="John Doe", email="john@example.com")

# Create multiple users
users = UserFactory.create_batch(10, active=True)
```

### UserFactory

```python
# Create a user with custom fields
user = UserFactory.create(
    name="Jane Doe",
    email="jane@example.com",
    active=True,
    role="admin"  # Custom field
)

# Create batch with mix of active/inactive
users = UserFactory.create_batch(10)  # Mix of active/inactive

# Create batch with all active
users = UserFactory.create_batch(10, active=True)

# Create batch with all inactive
users = UserFactory.create_batch(10, active=False)
```

### OrderFactory

```python
# Create order linked to a user
user_id = "user_123"
order = OrderFactory.create(
    user_id=user_id,
    amount=250.50,
    status="pending"
)

# Create orders for multiple users
user_ids = ["user_1", "user_2", "user_3"]
orders = OrderFactory.create_batch(
    10,
    user_ids=user_ids,  # Cycles through user IDs
    status="pending"
)

# Create batch with mixed statuses
orders = OrderFactory.create_batch(10)  # Random statuses
```

### ProductFactory

```python
# Create a product
product = ProductFactory.create(
    name="Laptop",
    category="electronics",
    price=999.99,
    tags=["new", "popular"]
)

# Create batch in specific category
products = ProductFactory.create_batch(10, category="books")

# Create batch with mixed categories
products = ProductFactory.create_batch(10)  # Random categories
```

## Creating Related Data

```python
# Create users first
users = UserFactory.create_batch(5, active=True)
user_ids = [u["_id"] for u in users]

# Create orders for these users
orders = OrderFactory.create_batch(20, user_ids=user_ids)

# All orders are now linked to actual users
assert all(o["userId"] in user_ids for o in orders)
```

## ObjectId Support

Factories automatically use MongoDB ObjectId if `bson` is available:

```python
# With bson installed - uses ObjectId
user = UserFactory.create()
assert isinstance(user["_id"], ObjectId)

# Without bson - uses string IDs
user = UserFactory.create(use_objectid=False)
assert isinstance(user["_id"], str)
```

## Custom Fields

All factories support custom fields via `**kwargs`:

```python
user = UserFactory.create(
    name="Custom User",
    customField="value",
    metadata={"key": "value"}
)
# Result includes: _id, name, email, active, createdAt, customField, metadata
```

## Available Factories

### UserFactory
- Fields: `_id`, `name`, `email`, `active`, `createdAt`
- Batch options: `active` (bool or None for mix)

### OrderFactory
- Fields: `_id`, `userId`, `amount`, `status`, `items`, `createdAt`
- Batch options: `user_ids` (list), `status` (str or None for mix)

### ProductFactory
- Fields: `_id`, `name`, `category`, `price`, `inStock`, `tags`, `createdAt`
- Batch options: `category` (str or None for mix), `in_stock` (bool or None for mix)

## Examples in Tests

```python
def test_user_orders(self, test_db):
    """Example: Create users and their orders."""
    from tests.integration.factories import UserFactory, OrderFactory
    
    # Create users
    users = UserFactory.create_batch(3)
    user_ids = [u["_id"] for u in users]
    test_db.users.insert_many(users)
    
    # Create orders for these users
    orders = OrderFactory.create_batch(10, user_ids=user_ids)
    test_db.orders.insert_many(orders)
    
    # Now test your pipeline
    pipeline = PipelineBuilder().lookup(...).build()
    results = list(test_db.orders.aggregate(pipeline))
```


