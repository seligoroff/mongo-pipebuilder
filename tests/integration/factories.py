"""
Factories for generating test data for MongoDB integration tests.

These factories can be used both with and without a real MongoDB connection
to generate test data structures.

Features:
- Support for MongoDB ObjectId (if bson available)
- Related data generation (e.g., orders with valid user_ids)
- Batch creation with variations
- Flexible customization via kwargs
"""
from typing import Any, Dict, List, Optional
from datetime import datetime
import random
import string

# Try to import ObjectId for MongoDB compatibility
try:
    from bson import ObjectId
    OBJECTID_AVAILABLE = True
except ImportError:
    OBJECTID_AVAILABLE = False
    # Fallback: create a simple ObjectId-like string generator
    def ObjectId(value: Optional[str] = None):
        """Fallback ObjectId generator when bson is not available."""
        if value:
            return value
        return ''.join(random.choices(string.hexdigits.lower(), k=24))


class UserFactory:
    """Factory for creating test user documents."""
    
    _counter = 0
    
    @staticmethod
    def create(
        user_id: Optional[Any] = None,
        name: Optional[str] = None,
        email: Optional[str] = None,
        active: bool = True,
        use_objectid: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create a test user document.
        
        Args:
            user_id: Custom user ID (if None, generates ObjectId or string)
            name: User name (default: "Test User {counter}")
            email: User email (default: "test{counter}@example.com")
            active: Whether user is active
            use_objectid: Use ObjectId if bson available (default: True)
            **kwargs: Additional fields to add to document
        
        Returns:
            Dictionary representing a user document
        """
        UserFactory._counter += 1
        counter = UserFactory._counter
        
        if user_id is None:
            _id = ObjectId() if (use_objectid and OBJECTID_AVAILABLE) else f"user_{counter}_{datetime.now().timestamp()}"
        else:
            _id = user_id
        
        return {
            "_id": _id,
            "name": name or f"Test User {counter}",
            "email": email or f"test{counter}@example.com",
            "active": active,
            "createdAt": datetime.now(),
            **kwargs
        }
    
    @staticmethod
    def create_batch(
        count: int,
        active: Optional[bool] = None,
        use_objectid: bool = True,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Create multiple test user documents.
        
        Args:
            count: Number of users to create
            active: If None, creates mix of active/inactive. If bool, all users have this status.
            use_objectid: Use ObjectId if bson available
            **kwargs: Additional fields to add to all documents
        
        Returns:
            List of user documents
        """
        users = []
        for i in range(count):
            user_kwargs = kwargs.copy()
            if active is None:
                # Mix of active and inactive
                user_kwargs["active"] = i % 2 == 0
            else:
                user_kwargs["active"] = active
            
            users.append(UserFactory.create(use_objectid=use_objectid, **user_kwargs))
        return users


class OrderFactory:
    """Factory for creating test order documents."""
    
    _counter = 0
    
    @staticmethod
    def create(
        order_id: Optional[Any] = None,
        user_id: Optional[Any] = None,
        amount: Optional[float] = None,
        status: Optional[str] = None,
        items: Optional[List[Dict[str, Any]]] = None,
        use_objectid: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create a test order document.
        
        Args:
            order_id: Custom order ID (if None, generates ObjectId or string)
            user_id: User ID this order belongs to (required for lookups)
            amount: Order amount (default: random 10-1000)
            status: Order status (default: random from ["pending", "completed", "cancelled"])
            items: List of order items (default: single item)
            use_objectid: Use ObjectId if bson available
            **kwargs: Additional fields to add to document
        
        Returns:
            Dictionary representing an order document
        """
        OrderFactory._counter += 1
        
        if order_id is None:
            _id = ObjectId() if (use_objectid and OBJECTID_AVAILABLE) else f"order_{OrderFactory._counter}_{datetime.now().timestamp()}"
        else:
            _id = order_id
        
        if amount is None:
            amount = round(random.uniform(10.0, 1000.0), 2)
        
        if status is None:
            status = random.choice(["pending", "completed", "cancelled"])
        
        return {
            "_id": _id,
            "userId": user_id or "user_123",
            "amount": amount,
            "status": status,
            "items": items or [{"name": f"Item {OrderFactory._counter}", "quantity": random.randint(1, 5)}],
            "createdAt": datetime.now(),
            **kwargs
        }
    
    @staticmethod
    def create_batch(
        count: int,
        user_ids: Optional[List[Any]] = None,
        status: Optional[str] = None,
        use_objectid: bool = True,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Create multiple test order documents.
        
        Args:
            count: Number of orders to create
            user_ids: List of user IDs to assign orders to (cycles through if provided)
            status: If None, creates mix of statuses. If str, all orders have this status.
            use_objectid: Use ObjectId if bson available
            **kwargs: Additional fields to add to all documents
        
        Returns:
            List of order documents
        """
        orders = []
        for i in range(count):
            order_kwargs = kwargs.copy()
            
            # Assign user_id from list if provided
            if user_ids:
                order_kwargs["user_id"] = user_ids[i % len(user_ids)]
            
            # Set status
            if status is None:
                order_kwargs["status"] = None  # Will be randomized in create()
            else:
                order_kwargs["status"] = status
            
            orders.append(OrderFactory.create(use_objectid=use_objectid, **order_kwargs))
        return orders


class ProductFactory:
    """Factory for creating test product documents."""
    
    _counter = 0
    _categories = ["electronics", "books", "clothing", "food", "toys", "sports"]
    _tags_pool = ["new", "popular", "sale", "featured", "limited", "premium", "basic"]
    
    @staticmethod
    def create(
        product_id: Optional[Any] = None,
        name: Optional[str] = None,
        category: Optional[str] = None,
        price: Optional[float] = None,
        in_stock: Optional[bool] = None,
        tags: Optional[List[str]] = None,
        use_objectid: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create a test product document.
        
        Args:
            product_id: Custom product ID (if None, generates ObjectId or string)
            name: Product name (default: "Product {counter}")
            category: Product category (default: random from categories)
            price: Product price (default: random 5-500)
            in_stock: Whether product is in stock (default: random)
            tags: List of tags (default: random 1-3 tags from pool)
            use_objectid: Use ObjectId if bson available
            **kwargs: Additional fields to add to document
        
        Returns:
            Dictionary representing a product document
        """
        ProductFactory._counter += 1
        
        if product_id is None:
            _id = ObjectId() if (use_objectid and OBJECTID_AVAILABLE) else f"product_{ProductFactory._counter}_{datetime.now().timestamp()}"
        else:
            _id = product_id
        
        if price is None:
            price = round(random.uniform(5.0, 500.0), 2)
        
        if category is None:
            category = random.choice(ProductFactory._categories)
        
        if in_stock is None:
            in_stock = random.choice([True, True, True, False])  # 75% chance in stock
        
        if tags is None:
            num_tags = random.randint(1, 3)
            tags = random.sample(ProductFactory._tags_pool, num_tags)
        
        return {
            "_id": _id,
            "name": name or f"Product {ProductFactory._counter}",
            "category": category,
            "price": price,
            "inStock": in_stock,
            "tags": tags,
            "createdAt": datetime.now(),
            **kwargs
        }
    
    @staticmethod
    def create_batch(
        count: int,
        category: Optional[str] = None,
        in_stock: Optional[bool] = None,
        use_objectid: bool = True,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Create multiple test product documents.
        
        Args:
            count: Number of products to create
            category: If None, creates mix of categories. If str, all products have this category.
            in_stock: If None, creates mix. If bool, all products have this stock status.
            use_objectid: Use ObjectId if bson available
            **kwargs: Additional fields to add to all documents
        
        Returns:
            List of product documents
        """
        products = []
        for i in range(count):
            product_kwargs = kwargs.copy()
            
            if category is None:
                product_kwargs["category"] = None  # Will be randomized in create()
            else:
                product_kwargs["category"] = category
            
            if in_stock is None:
                product_kwargs["in_stock"] = None  # Will be randomized in create()
            else:
                product_kwargs["in_stock"] = in_stock
            
            products.append(ProductFactory.create(use_objectid=use_objectid, **product_kwargs))
        return products

