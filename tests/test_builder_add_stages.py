"""
Tests for PipelineBuilder.add_stages (add multiple stages at once).

Author: seligoroff
"""
import pytest

from mongo_pipebuilder import PipelineBuilder


class TestAddStages:
    """Tests for add_stages method."""

    def test_add_stages_empty_list_adds_nothing(self):
        """add_stages([]) adds no stages."""
        builder = PipelineBuilder()
        builder.match({"x": 1}).add_stages([])
        pipeline = builder.build()
        assert len(pipeline) == 1
        assert pipeline[0] == {"$match": {"x": 1}}

    def test_add_stages_one_stage(self):
        """add_stages([stage]) adds one stage."""
        builder = PipelineBuilder()
        builder.add_stages([{"$match": {"x": 1}}])
        pipeline = builder.build()
        assert len(pipeline) == 1
        assert pipeline[0] == {"$match": {"x": 1}}

    def test_add_stages_two_stages(self):
        """add_stages([s1, s2]) adds two stages in order."""
        builder = PipelineBuilder()
        builder.add_stages([{"$match": {"a": 1}}, {"$limit": 10}])
        pipeline = builder.build()
        assert len(pipeline) == 2
        assert pipeline[0] == {"$match": {"a": 1}}
        assert pipeline[1] == {"$limit": 10}

    def test_add_stages_skips_empty_dict_stages(self):
        """add_stages([{}, stage, ...]) skips empty dicts, adds only non-empty stages."""
        builder = PipelineBuilder()
        builder.add_stages([{}, {"$match": {"x": 1}}])
        pipeline = builder.build()
        assert len(pipeline) == 1
        assert pipeline[0] == {"$match": {"x": 1}}

    def test_add_stages_from_other_builder_build(self):
        """add_stages(other_builder.build()) appends subpipeline; order and content preserved."""
        sub = PipelineBuilder().match({"source": "api"}).project({"name": 1, "_id": 0})
        builder = PipelineBuilder()
        builder.match({"status": "active"}).add_stages(sub.build())
        pipeline = builder.build()
        assert len(pipeline) == 3
        assert pipeline[0] == {"$match": {"status": "active"}}
        assert pipeline[1] == {"$match": {"source": "api"}}
        assert pipeline[2] == {"$project": {"name": 1, "_id": 0}}
        assert len(sub) == 2

    def test_add_stages_none_raises(self):
        """add_stages(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="stages must not be None"):
            builder.add_stages(None)

    def test_add_stages_non_dict_element_raises(self):
        """add_stages(iterable with non-dict element) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="All stages must be dictionaries"):
            builder.add_stages([{"$match": {}}, "not a stage"])

    def test_add_stages_chaining(self):
        """add_stages returns self; can chain with match, limit, etc."""
        builder = PipelineBuilder()
        pipeline = (
            builder.add_stages([{"$match": {"source": "api"}}])
            .match({"status": "active"})
            .limit(10)
            .build()
        )
        assert len(pipeline) == 3
        assert pipeline[0] == {"$match": {"source": "api"}}
        assert pipeline[1] == {"$match": {"status": "active"}}
        assert pipeline[2] == {"$limit": 10}

    def test_add_stages_copy_independent(self):
        """copy() after add_stages yields independent builder; modifying copy does not change original."""
        builder = PipelineBuilder()
        builder.add_stages([{"$match": {"x": 1}}])
        c = builder.copy()
        c.limit(5)
        assert len(builder) == 1
        assert len(c) == 2
        assert builder.build()[0] == {"$match": {"x": 1}}
        assert c.build()[0] == {"$match": {"x": 1}}
        assert c.build()[1] == {"$limit": 5}
