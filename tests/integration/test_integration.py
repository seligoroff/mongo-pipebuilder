"""
Integration tests for PipelineBuilder with real MongoDB.

These tests verify that pipelines built with PipelineBuilder
actually work correctly when executed against MongoDB.

To run these tests:
    pytest tests/integration -m integration

Requirements:
    - MongoDB running on localhost:27017
    - pymongo installed (pip install pymongo)
"""
import pytest
from mongo_pipebuilder import PipelineBuilder


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests with real MongoDB."""
    
    def test_match_stage_execution(self, users_collection):
        """Test that $match stage works correctly."""
        pipeline = (
            PipelineBuilder()
            .match({"active": True})
            .build()
        )
        
        results = list(users_collection.aggregate(pipeline))
        
        # Should only return active users
        assert len(results) == 5
        assert all(user["active"] is True for user in results)
    
    def test_lookup_stage_execution(self, orders_collection, users_collection):
        """Test that $lookup stage works correctly."""
        # First, ensure we have matching user IDs
        user = users_collection.find_one()
        orders_collection.update_many({}, {"$set": {"userId": user["_id"]}})
        
        pipeline = (
            PipelineBuilder()
            .lookup(
                from_collection="users",
                local_field="userId",
                foreign_field="_id",
                as_field="user"
            )
            .build()
        )
        
        results = list(orders_collection.aggregate(pipeline))
        
        # Should have user data joined
        assert len(results) > 0
        assert "user" in results[0]
        assert len(results[0]["user"]) > 0
    
    def test_group_stage_execution(self, orders_collection):
        """Test that $group stage works correctly."""
        pipeline = (
            PipelineBuilder()
            .group(
                group_by={"status": "$status"},
                accumulators={"total": {"$sum": "$amount"}, "count": {"$sum": 1}}
            )
            .build()
        )
        
        results = list(orders_collection.aggregate(pipeline))
        
        # Should group by status
        assert len(results) > 0
        assert all("status" in r for r in results)
        assert all("total" in r for r in results)
        assert all("count" in r for r in results)
    
    def test_sort_and_limit_execution(self, products_collection):
        """Test that $sort and $limit stages work correctly."""
        pipeline = (
            PipelineBuilder()
            .sort({"price": -1})
            .limit(3)
            .build()
        )
        
        results = list(products_collection.aggregate(pipeline))
        
        # Should return exactly 3 results, sorted by price descending
        assert len(results) == 3
        prices = [r["price"] for r in results]
        assert prices == sorted(prices, reverse=True)
    
    def test_complex_pipeline_execution(self, orders_collection, users_collection):
        """Test a complex pipeline with multiple stages."""
        # Setup: ensure user IDs match
        user = users_collection.find_one()
        orders_collection.update_many({}, {"$set": {"userId": user["_id"]}})
        
        pipeline = (
            PipelineBuilder()
            .match({"status": "pending"})
            .lookup(
                from_collection="users",
                local_field="userId",
                foreign_field="_id",
                as_field="user"
            )
            .add_fields({"userName": "$user.name"})
            .project({"amount": 1, "userName": 1, "_id": 0})
            .sort({"amount": -1})
            .limit(5)
            .build()
        )
        
        results = list(orders_collection.aggregate(pipeline))
        
        # Verify structure
        assert len(results) <= 5
        if results:
            assert "amount" in results[0]
            assert "userName" in results[0]
            assert "_id" not in results[0]
    
    def test_unwind_stage_execution(self, products_collection):
        """Test that $unwind stage works correctly."""
        pipeline = (
            PipelineBuilder()
            .unwind("tags")
            .group(
                group_by={"tag": "$tags"},
                accumulators={"count": {"$sum": 1}}
            )
            .sort({"count": -1})
            .build()
        )
        
        results = list(products_collection.aggregate(pipeline))
        
        # Should unwind tags and group by them
        assert len(results) > 0
        assert all("tag" in r for r in results)
        assert all("count" in r for r in results)



