"""
Additional validation tests for PipelineBuilder.

These tests check MongoDB syntax validity and edge cases that could cause
runtime errors when executing pipelines on a real MongoDB instance.

Author: seligoroff
"""
import pytest
from mongo_pipebuilder import PipelineBuilder


class TestPipelineValidation:
    """Tests for MongoDB pipeline syntax validation."""

    def test_pipeline_is_valid_json_serializable(self):
        """Test that built pipeline is JSON serializable."""
        import json
        
        builder = PipelineBuilder()
        pipeline = (
            builder
            .match({"status": "active"})
            .sort({"name": 1})
            .limit(10)
            .build()
        )
        
        # Should not raise exception
        json_str = json.dumps(pipeline)
        assert json_str is not None
        # Should be able to deserialize
        deserialized = json.loads(json_str)
        assert deserialized == pipeline

    def test_lookup_stage_has_required_fields(self):
        """Test that $lookup stage has all required MongoDB fields."""
        builder = PipelineBuilder()
        pipeline = builder.lookup(
            from_collection="users",
            local_field="userId",
            foreign_field="_id",
            as_field="user"
        ).build()
        
        lookup_stage = pipeline[0]["$lookup"]
        # MongoDB requires: from, localField, foreignField, as
        assert "from" in lookup_stage
        assert "localField" in lookup_stage
        assert "foreignField" in lookup_stage
        assert "as" in lookup_stage
        assert isinstance(lookup_stage["from"], str)
        assert isinstance(lookup_stage["localField"], str)
        assert isinstance(lookup_stage["foreignField"], str)
        assert isinstance(lookup_stage["as"], str)

    def test_group_stage_has_id(self):
        """Test that $group stage has required _id field."""
        builder = PipelineBuilder()
        pipeline = builder.group(
            group_by={"category": "$category"},
            accumulators={"total": {"$sum": "$amount"}}
        ).build()
        
        group_stage = pipeline[0]["$group"]
        assert "_id" in group_stage
        assert group_stage["_id"] == {"category": "$category"}

    def test_unwind_stage_has_path(self):
        """Test that $unwind stage has required path field."""
        builder = PipelineBuilder()
        pipeline = builder.unwind("tags").build()
        
        unwind_stage = pipeline[0]["$unwind"]
        assert "path" in unwind_stage
        assert isinstance(unwind_stage["path"], str)
        assert unwind_stage["path"] == "tags"

    def test_sort_stage_values_are_valid(self):
        """Test that $sort stage has valid sort directions (1 or -1)."""
        builder = PipelineBuilder()
        pipeline = builder.sort({"createdAt": -1, "name": 1}).build()
        
        sort_stage = pipeline[0]["$sort"]
        for field, direction in sort_stage.items():
            assert direction in (1, -1), f"Sort direction must be 1 or -1, got {direction}"

    def test_limit_skip_are_positive_integers(self):
        """Test that $limit and $skip accept only positive integers."""
        # Valid cases - use separate builders for limit and skip
        builder_limit = PipelineBuilder()
        pipeline_limit = builder_limit.limit(10).build()
        assert pipeline_limit[0]["$limit"] == 10
        assert isinstance(pipeline_limit[0]["$limit"], int)
        
        builder_skip = PipelineBuilder()
        pipeline_skip = builder_skip.skip(20).build()
        assert pipeline_skip[0]["$skip"] == 20
        assert isinstance(pipeline_skip[0]["$skip"], int)

    def test_nested_pipeline_in_lookup_is_valid(self):
        """Test that nested pipeline in $lookup is a valid list."""
        builder = PipelineBuilder()
        nested_pipeline = [{"$match": {"active": True}}, {"$limit": 10}]
        
        pipeline = builder.lookup(
            from_collection="users",
            local_field="userId",
            foreign_field="_id",
            as_field="user",
            pipeline=nested_pipeline
        ).build()
        
        lookup_stage = pipeline[0]["$lookup"]
        assert "pipeline" in lookup_stage
        assert isinstance(lookup_stage["pipeline"], list)
        assert len(lookup_stage["pipeline"]) == 2

    def test_pipeline_stages_order(self):
        """Test that pipeline stages are in correct order."""
        builder = PipelineBuilder()
        pipeline = (
            builder
            .match({"status": "active"})
            .sort({"name": 1})
            .limit(10)
            .build()
        )
        
        # Verify order: match -> sort -> limit
        assert "$match" in pipeline[0]
        assert "$sort" in pipeline[1]
        assert "$limit" in pipeline[2]

    def test_empty_pipeline_is_valid(self):
        """Test that empty pipeline is valid (though not useful)."""
        builder = PipelineBuilder()
        pipeline = builder.build()
        
        assert isinstance(pipeline, list)
        assert len(pipeline) == 0
        # Empty pipeline is valid in MongoDB (just returns all documents)

    def test_complex_pipeline_structure(self):
        """Test that complex pipeline maintains correct structure."""
        builder = PipelineBuilder()
        pipeline = (
            builder
            .match({"status": "published"})
            .lookup(
                from_collection="authors",
                local_field="authorId",
                foreign_field="_id",
                as_field="author"
            )
            .unwind("author", preserve_null_and_empty_arrays=True)
            .add_fields({"authorName": "$author.name"})
            .project({"title": 1, "authorName": 1, "_id": 0})
            .sort({"title": 1})
            .limit(20)
            .build()
        )
        
        # Verify all stages are present and in correct order
        assert len(pipeline) == 7
        assert "$match" in pipeline[0]
        assert "$lookup" in pipeline[1]
        assert "$unwind" in pipeline[2]
        assert "$addFields" in pipeline[3]
        assert "$project" in pipeline[4]
        assert "$sort" in pipeline[5]
        assert "$limit" in pipeline[6]

















