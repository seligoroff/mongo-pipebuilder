"""
Tests for PipelineBuilder stage insertion methods.

Tests for prepend() and insert_at() methods from Proposal 9.

Author: seligoroff
"""
import pytest
from mongo_pipebuilder import PipelineBuilder


class TestPrepend:
    """Tests for prepend() method."""

    def test_prepend_to_empty_builder(self):
        """Test prepend() on empty builder."""
        builder = PipelineBuilder()
        builder.prepend({"$match": {"deleted": False}})
        assert builder.build() == [{"$match": {"deleted": False}}]

    def test_prepend_single_stage(self):
        """Test prepend() adds stage at the beginning."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        builder.prepend({"$match": {"deleted": False}})
        
        pipeline = builder.build()
        assert len(pipeline) == 2
        assert pipeline[0] == {"$match": {"deleted": False}}
        assert pipeline[1] == {"$match": {"status": "active"}}

    def test_prepend_multiple_stages(self):
        """Test prepend() with multiple existing stages."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10).sort({"name": 1})
        builder.prepend({"$match": {"deleted": False}})
        
        pipeline = builder.build()
        assert len(pipeline) == 4
        assert pipeline[0] == {"$match": {"deleted": False}}
        assert pipeline[1] == {"$match": {"status": "active"}}
        assert pipeline[2] == {"$limit": 10}
        assert pipeline[3] == {"$sort": {"name": 1}}

    def test_prepend_returns_self(self):
        """Test that prepend() returns self for method chaining."""
        builder = PipelineBuilder()
        result = builder.prepend({"$match": {"x": 1}}).match({"y": 2})
        assert result is builder
        assert len(builder) == 2

    def test_prepend_empty_dict_skipped(self):
        """Test that prepend() skips empty dictionary."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        builder.prepend({})
        
        pipeline = builder.build()
        assert len(pipeline) == 1
        assert pipeline[0] == {"$match": {"status": "active"}}

    def test_prepend_none_raises_error(self):
        """Test that prepend(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="stage cannot be None"):
            builder.prepend(None)

    def test_prepend_invalid_type_raises_error(self):
        """Test that prepend() with non-dict raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="stage must be a dict"):
            builder.prepend("not a dict")
        
        with pytest.raises(TypeError, match="stage must be a dict"):
            builder.prepend(123)
        
        with pytest.raises(TypeError, match="stage must be a dict"):
            builder.prepend([])


class TestInsertAt:
    """Tests for insert_at() method."""

    def test_insert_at_beginning(self):
        """Test insert_at(0) inserts at the beginning."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        builder.insert_at(0, {"$match": {"deleted": False}})
        
        pipeline = builder.build()
        assert len(pipeline) == 2
        assert pipeline[0] == {"$match": {"deleted": False}}
        assert pipeline[1] == {"$match": {"status": "active"}}

    def test_insert_at_middle(self):
        """Test insert_at() inserts in the middle."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).group("$category", {"count": {"$sum": 1}})
        builder.insert_at(1, {"$sort": {"name": 1}})
        
        pipeline = builder.build()
        assert len(pipeline) == 3
        assert pipeline[0] == {"$match": {"status": "active"}}
        assert pipeline[1] == {"$sort": {"name": 1}}
        assert "$group" in pipeline[2]

    def test_insert_at_end(self):
        """Test insert_at(len(stages)) inserts at the end."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10)
        builder.insert_at(2, {"$sort": {"name": 1}})
        
        pipeline = builder.build()
        assert len(pipeline) == 3
        assert pipeline[0] == {"$match": {"status": "active"}}
        assert pipeline[1] == {"$limit": 10}
        assert pipeline[2] == {"$sort": {"name": 1}}

    def test_insert_at_returns_self(self):
        """Test that insert_at() returns self for method chaining."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        result = builder.insert_at(1, {"$limit": 10}).sort({"name": 1})
        assert result is builder
        assert len(builder) == 3

    def test_insert_at_empty_dict_skipped(self):
        """Test that insert_at() skips empty dictionary."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10)
        builder.insert_at(1, {})
        
        pipeline = builder.build()
        assert len(pipeline) == 2
        assert pipeline[0] == {"$match": {"status": "active"}}
        assert pipeline[1] == {"$limit": 10}

    def test_insert_at_none_raises_error(self):
        """Test that insert_at(..., None) raises TypeError."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        with pytest.raises(TypeError, match="stage cannot be None"):
            builder.insert_at(1, None)

    def test_insert_at_invalid_type_raises_error(self):
        """Test that insert_at() with non-dict raises TypeError."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        with pytest.raises(TypeError, match="stage must be a dict"):
            builder.insert_at(1, "not a dict")
        
        with pytest.raises(TypeError, match="stage must be a dict"):
            builder.insert_at(1, 123)
        
        with pytest.raises(TypeError, match="stage must be a dict"):
            builder.insert_at(1, [])

    def test_insert_at_negative_position_raises_error(self):
        """Test that insert_at() with negative position raises IndexError."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        with pytest.raises(IndexError, match="Position -1 out of range"):
            builder.insert_at(-1, {"$limit": 10})

    def test_insert_at_position_too_large_raises_error(self):
        """Test that insert_at() with position > len(stages) raises IndexError."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        with pytest.raises(IndexError, match="Position 10 out of range"):
            builder.insert_at(10, {"$limit": 10})

    def test_insert_at_position_equals_length_allowed(self):
        """Test that insert_at(len(stages)) is allowed (inserts at end)."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        builder.insert_at(1, {"$limit": 10})
        
        pipeline = builder.build()
        assert len(pipeline) == 2
        assert pipeline[1] == {"$limit": 10}

    def test_insert_at_complex_pipeline(self):
        """Test insert_at() with complex pipeline."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        builder.lookup("users", "userId", "_id", "user")
        builder.unwind("user")
        builder.group("$category", {"count": {"$sum": 1}})
        
        # Insert $addFields before $group
        builder.insert_at(3, {"$addFields": {"categoryUpper": {"$toUpper": "$category"}}})
        
        pipeline = builder.build()
        assert len(pipeline) == 5
        assert "$match" in pipeline[0]
        assert "$lookup" in pipeline[1]
        assert "$unwind" in pipeline[2]
        assert "$addFields" in pipeline[3]
        assert "$group" in pipeline[4]


class TestPrependAndInsertAtIntegration:
    """Integration tests for prepend() and insert_at() together."""

    def test_prepend_then_insert_at(self):
        """Test using prepend() and insert_at() together."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10)
        builder.prepend({"$match": {"deleted": False}})
        builder.insert_at(2, {"$sort": {"name": 1}})
        
        pipeline = builder.build()
        assert len(pipeline) == 4
        assert pipeline[0] == {"$match": {"deleted": False}}
        assert pipeline[1] == {"$match": {"status": "active"}}
        assert pipeline[2] == {"$sort": {"name": 1}}
        assert pipeline[3] == {"$limit": 10}

    def test_insert_at_with_get_stage_types(self):
        """Test insert_at() combined with get_stage_types() for finding position."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        builder.lookup("users", "userId", "_id", "user")
        builder.group("$category", {"count": {"$sum": 1}})
        
        # Find position of $group and insert before it
        stage_types = builder.get_stage_types()
        group_index = stage_types.index("$group")
        builder.insert_at(group_index, {"$addFields": {"x": 1}})
        
        pipeline = builder.build()
        assert len(pipeline) == 4
        assert "$match" in pipeline[0]
        assert "$lookup" in pipeline[1]
        assert "$addFields" in pipeline[2]
        assert "$group" in pipeline[3]

    def test_fluent_interface_with_insertion(self):
        """Test fluent interface works with insertion methods."""
        builder = PipelineBuilder()
        pipeline = (
            builder
            .match({"status": "active"})
            .prepend({"$match": {"deleted": False}})
            .insert_at(2, {"$sort": {"name": 1}})
            .limit(10)
            .build()
        )
        
        assert len(pipeline) == 4
        assert pipeline[0] == {"$match": {"deleted": False}}
        assert pipeline[1] == {"$match": {"status": "active"}}
        assert pipeline[2] == {"$sort": {"name": 1}}
        assert pipeline[3] == {"$limit": 10}



