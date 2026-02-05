"""
Tests for PipelineBuilder.union_with ($unionWith stage).

Author: seligoroff
"""
import pytest

from mongo_pipebuilder import PipelineBuilder


class TestUnionWith:
    """Tests for union_with stage."""

    def test_union_with_basic_without_pipeline(self):
        """union_with(coll) adds one $unionWith stage with coll and empty pipeline."""
        builder = PipelineBuilder()
        pipeline = builder.union_with("other_coll").build()

        assert len(pipeline) == 1
        assert pipeline[0] == {"$unionWith": {"coll": "other_coll", "pipeline": []}}

    def test_union_with_with_pipeline_list(self):
        """union_with(coll, pipeline_list) adds stage with that pipeline."""
        builder = PipelineBuilder()
        sub = [{"$match": {"x": 1}}, {"$project": {"a": 1}}]
        pipeline = builder.union_with("logs", sub).build()

        assert len(pipeline) == 1
        assert pipeline[0]["$unionWith"]["coll"] == "logs"
        assert pipeline[0]["$unionWith"]["pipeline"] == sub

    def test_union_with_with_pipeline_builder(self):
        """union_with(coll, PipelineBuilder) uses built list as pipeline; builder unchanged."""
        sub = PipelineBuilder().match({"source": "individual"}).project({"name": 1})
        builder = PipelineBuilder()
        pipeline = builder.union_with("sso_individual_statistics", sub).build()

        assert len(pipeline) == 1
        assert pipeline[0]["$unionWith"]["coll"] == "sso_individual_statistics"
        assert pipeline[0]["$unionWith"]["pipeline"] == [
            {"$match": {"source": "individual"}},
            {"$project": {"name": 1}},
        ]
        assert len(sub) == 2

    def test_union_with_coll_empty_raises(self):
        """union_with('') raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="coll must be a non-empty string"):
            builder.union_with("")

    def test_union_with_coll_not_string_raises(self):
        """union_with(coll not str) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="coll must be a string"):
            builder.union_with(123)

    def test_union_with_pipeline_not_list_or_builder_raises(self):
        """union_with(..., pipeline not list and not PipelineBuilder) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="pipeline must be a list or PipelineBuilder"):
            builder.union_with("c", "not a pipeline")

    def test_union_with_pipeline_list_with_non_dict_raises(self):
        """union_with(..., pipeline list with non-dict) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="All pipeline stages must be dictionaries"):
            builder.union_with("c", [{"$match": {}}, "not a stage"])

    def test_union_with_chaining(self):
        """union_with returns self; can chain with match, limit, etc."""
        builder = PipelineBuilder()
        pipeline = (
            builder.union_with("other_coll")
            .match({"status": "active"})
            .limit(10)
            .build()
        )
        assert len(pipeline) == 3
        assert pipeline[0] == {"$unionWith": {"coll": "other_coll", "pipeline": []}}
        assert pipeline[1] == {"$match": {"status": "active"}}
        assert pipeline[2] == {"$limit": 10}

    def test_union_with_copy_independent(self):
        """copy() after union_with yields independent builder; modifying copy does not change original."""
        builder = PipelineBuilder()
        builder.union_with("logs", [{"$match": {"x": 1}}])
        c = builder.copy()
        c.limit(5)
        assert len(builder) == 1
        assert len(c) == 2
        assert builder.build()[0]["$unionWith"]["coll"] == "logs"
        assert builder.build()[0]["$unionWith"]["pipeline"] == [{"$match": {"x": 1}}]
        assert c.build()[0]["$unionWith"]["coll"] == "logs"
        assert c.build()[1] == {"$limit": 5}
