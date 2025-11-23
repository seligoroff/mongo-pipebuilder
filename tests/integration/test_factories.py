"""
Test factories to verify they work correctly.
"""
from .factories import UserFactory, OrderFactory, ProductFactory


def test_user_factory():
    """Test UserFactory creates valid user documents."""
    user = UserFactory.create(name="John Doe", email="john@example.com")
    
    assert user["name"] == "John Doe"
    assert user["email"] == "john@example.com"
    assert "_id" in user
    assert "createdAt" in user
    assert user["active"] is True


def test_user_factory_batch():
    """Test UserFactory batch creation."""
    users = UserFactory.create_batch(5, active=True)
    
    assert len(users) == 5
    assert all(u["active"] is True for u in users)
    assert len(set(u["_id"] for u in users)) == 5  # All IDs unique


def test_order_factory_with_user_id():
    """Test OrderFactory with related user_id."""
    user_id = "user_123"
    order = OrderFactory.create(user_id=user_id, amount=250.50)
    
    assert order["userId"] == user_id
    assert order["amount"] == 250.50
    assert "status" in order
    assert "items" in order


def test_order_factory_batch_with_user_ids():
    """Test OrderFactory batch with user IDs."""
    user_ids = ["user_1", "user_2", "user_3"]
    orders = OrderFactory.create_batch(6, user_ids=user_ids, status="pending")
    
    assert len(orders) == 6
    assert all(o["status"] == "pending" for o in orders)
    # Should cycle through user_ids
    assert orders[0]["userId"] in user_ids
    assert orders[3]["userId"] in user_ids


def test_product_factory():
    """Test ProductFactory creates valid product documents."""
    product = ProductFactory.create(category="electronics", price=99.99)
    
    assert product["category"] == "electronics"
    assert product["price"] == 99.99
    assert "tags" in product
    assert isinstance(product["tags"], list)
    assert len(product["tags"]) > 0


def test_product_factory_batch():
    """Test ProductFactory batch creation."""
    products = ProductFactory.create_batch(10, category="books")
    
    assert len(products) == 10
    assert all(p["category"] == "books" for p in products)


def test_factories_with_related_data():
    """Test creating related data (orders for users)."""
    # Create users
    users = UserFactory.create_batch(3, active=True)
    user_ids = [u["_id"] for u in users]
    
    # Create orders for these users
    orders = OrderFactory.create_batch(10, user_ids=user_ids)
    
    assert len(orders) == 10
    assert all(o["userId"] in user_ids for o in orders)

