"""
Tests for PipelineBuilder.

Author: seligoroff
"""
import pytest
from mongo_pipebuilder import PipelineBuilder


class TestPipelineBuilder:
    """Tests for basic PipelineBuilder functionality."""

    def test_empty_builder(self):
        """Test empty builder."""
        builder = PipelineBuilder()
        assert builder.build() == []

    def test_match_stage(self):
        """Test adding $match stage."""
        builder = PipelineBuilder()
        pipeline = builder.match({"status": "active"}).build()
        assert pipeline == [{"$match": {"status": "active"}}]

    def test_empty_match_skipped(self):
        """Test skipping empty $match."""
        builder = PipelineBuilder()
        pipeline = builder.match({}).build()
        assert pipeline == []

    def test_lookup_stage(self):
        """Test adding $lookup stage."""
        builder = PipelineBuilder()
        pipeline = builder.lookup(
            from_collection="users",
            local_field="userId",
            foreign_field="_id",
            as_field="user"
        ).build()
        
        assert len(pipeline) == 1
        assert "$lookup" in pipeline[0]
        assert pipeline[0]["$lookup"]["from"] == "users"
        assert pipeline[0]["$lookup"]["localField"] == "userId"
        assert pipeline[0]["$lookup"]["foreignField"] == "_id"
        assert pipeline[0]["$lookup"]["as"] == "user"

    def test_lookup_with_pipeline(self):
        """Test $lookup with nested pipeline."""
        builder = PipelineBuilder()
        nested_pipeline = [{"$match": {"active": True}}]
        pipeline = builder.lookup(
            from_collection="users",
            local_field="userId",
            foreign_field="_id",
            as_field="user",
            pipeline=nested_pipeline
        ).build()
        
        assert pipeline[0]["$lookup"]["pipeline"] == nested_pipeline

    def test_add_fields_stage(self):
        """Test adding $addFields stage."""
        builder = PipelineBuilder()
        pipeline = builder.add_fields({
            "fullName": {"$concat": ["$firstName", " ", "$lastName"]}
        }).build()
        
        assert pipeline == [{
            "$addFields": {
                "fullName": {"$concat": ["$firstName", " ", "$lastName"]}
            }
        }]

    def test_project_stage(self):
        """Test adding $project stage."""
        builder = PipelineBuilder()
        pipeline = builder.project({"name": 1, "email": 1, "_id": 0}).build()
        assert pipeline == [{"$project": {"name": 1, "email": 1, "_id": 0}}]

    def test_group_stage(self):
        """Test adding $group stage with dict."""
        builder = PipelineBuilder()
        pipeline = builder.group(
            group_by={"category": "$category"},
            accumulators={"total": {"$sum": "$amount"}}
        ).build()
        
        assert pipeline == [{
            "$group": {
                "_id": {"category": "$category"},
                "total": {"$sum": "$amount"}
            }
        }]

    def test_group_stage_with_string(self):
        """Test adding $group stage with string field path."""
        builder = PipelineBuilder()
        pipeline = builder.group(
            group_by="$categoryType",
            accumulators={"total": {"$sum": "$amount"}}
        ).build()
        
        assert pipeline == [{
            "$group": {
                "_id": "$categoryType",
                "total": {"$sum": "$amount"}
            }
        }]

    def test_unwind_stage(self):
        """Test adding $unwind stage."""
        builder = PipelineBuilder()
        pipeline = builder.unwind("tags").build()
        assert pipeline == [{"$unwind": {"path": "tags"}}]

    def test_unwind_with_options(self):
        """Test $unwind with options."""
        builder = PipelineBuilder()
        pipeline = builder.unwind(
            "items",
            preserve_null_and_empty_arrays=True,
            include_array_index="itemIndex"
        ).build()
        
        assert pipeline == [{
            "$unwind": {
                "path": "items",
                "preserveNullAndEmptyArrays": True,
                "includeArrayIndex": "itemIndex"
            }
        }]

    def test_sort_stage(self):
        """Test adding $sort stage."""
        builder = PipelineBuilder()
        pipeline = builder.sort({"createdAt": -1, "name": 1}).build()
        assert pipeline == [{"$sort": {"createdAt": -1, "name": 1}}]

    def test_limit_stage(self):
        """Test adding $limit stage."""
        builder = PipelineBuilder()
        pipeline = builder.limit(10).build()
        assert pipeline == [{"$limit": 10}]

    def test_limit_zero_skipped(self):
        """Test skipping $limit with zero value."""
        builder = PipelineBuilder()
        pipeline = builder.limit(0).build()
        assert pipeline == []

    def test_skip_stage(self):
        """Test adding $skip stage."""
        builder = PipelineBuilder()
        pipeline = builder.skip(20).build()
        assert pipeline == [{"$skip": 20}]

    def test_skip_zero_skipped(self):
        """Test skipping $skip with zero value."""
        builder = PipelineBuilder()
        pipeline = builder.skip(0).build()
        assert pipeline == []

    def test_add_stage(self):
        """Test adding arbitrary stage."""
        builder = PipelineBuilder()
        custom_stage = {"$facet": {"categories": [{"$group": {"_id": "$category"}}]}}
        pipeline = builder.add_stage(custom_stage).build()
        assert pipeline == [custom_stage]

    def test_fluent_interface(self):
        """Test method chaining (fluent interface)."""
        builder = PipelineBuilder()
        pipeline = (
            builder
            .match({"status": "active"})
            .sort({"name": 1})
            .limit(10)
            .build()
        )
        
        assert len(pipeline) == 3
        assert pipeline[0] == {"$match": {"status": "active"}}
        assert pipeline[1] == {"$sort": {"name": 1}}
        assert pipeline[2] == {"$limit": 10}

    def test_complex_pipeline(self):
        """Test complex pipeline with multiple stages."""
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
        
        assert len(pipeline) == 7
        assert pipeline[0]["$match"]["status"] == "published"
        assert "$lookup" in pipeline[1]
        assert "$unwind" in pipeline[2]
        assert "$addFields" in pipeline[3]
        assert "$project" in pipeline[4]
        assert "$sort" in pipeline[5]
        assert "$limit" in pipeline[6]

    def test_build_returns_copy(self):
        """Test that build() returns a copy, not a reference."""
        builder = PipelineBuilder()
        builder.match({"test": 1})
        pipeline1 = builder.build()
        pipeline2 = builder.build()
        
        assert pipeline1 == pipeline2
        assert pipeline1 is not pipeline2  # Different objects
        pipeline1.append({"$test": "should not affect builder"})
        assert len(builder.build()) == 1  # Builder unchanged

    def test_unset_single_field(self):
        """Test $unset with single field."""
        builder = PipelineBuilder()
        pipeline = builder.unset("temp_field").build()
        assert pipeline == [{"$unset": "temp_field"}]

    def test_unset_multiple_fields(self):
        """Test $unset with multiple fields."""
        builder = PipelineBuilder()
        pipeline = builder.unset(["field1", "field2", "field3"]).build()
        assert pipeline == [{"$unset": ["field1", "field2", "field3"]}]

    def test_unset_single_field_as_list(self):
        """Test $unset with single field in list (should be converted to string)."""
        builder = PipelineBuilder()
        pipeline = builder.unset(["single_field"]).build()
        assert pipeline == [{"$unset": "single_field"}]

    def test_replace_root(self):
        """Test $replaceRoot stage."""
        builder = PipelineBuilder()
        pipeline = builder.replace_root({"newRoot": "$embedded"}).build()
        assert pipeline == [{"$replaceRoot": {"newRoot": "$embedded"}}]

    def test_replace_with(self):
        """Test $replaceWith stage (alias for $replaceRoot)."""
        builder = PipelineBuilder()
        pipeline = builder.replace_with("$embedded").build()
        assert pipeline == [{"$replaceWith": "$embedded"}]

    def test_facet(self):
        """Test $facet stage."""
        builder = PipelineBuilder()
        facets = {
            "items": [{"$skip": 10}, {"$limit": 20}],
            "meta": [{"$count": "total"}]
        }
        pipeline = builder.facet(facets).build()
        assert pipeline == [{"$facet": facets}]

    def test_count(self):
        """Test $count stage."""
        builder = PipelineBuilder()
        pipeline = builder.count("total_count").build()
        assert pipeline == [{"$count": "total_count"}]

    def test_count_default_name(self):
        """Test $count stage with default field name."""
        builder = PipelineBuilder()
        pipeline = builder.count().build()
        assert pipeline == [{"$count": "count"}]

    def test_set_field(self):
        """Test $set stage (alias for $addFields)."""
        builder = PipelineBuilder()
        pipeline = builder.set_field({"status": "active", "updatedAt": "$$NOW"}).build()
        assert pipeline == [{"$set": {"status": "active", "updatedAt": "$$NOW"}}]

    def test_set_field_empty_skipped(self):
        """Test that empty $set is skipped."""
        builder = PipelineBuilder()
        pipeline = builder.set_field({}).build()
        assert pipeline == []

    def test_fluent_interface_with_new_stages(self):
        """Test fluent interface with new stages."""
        builder = PipelineBuilder()
        pipeline = (
            builder
            .match({"status": "active"})
            .unset("temp_field")
            .set_field({"processed": True})
            .replace_root({"newRoot": "$data"})
            .count("total")
            .build()
        )
        assert len(pipeline) == 5
        assert "$match" in pipeline[0]
        assert "$unset" in pipeline[1]
        assert "$set" in pipeline[2]
        assert "$replaceRoot" in pipeline[3]
        assert "$count" in pipeline[4]

