"""
Tests for PipelineBuilder.match_expr ($match with $expr).

Author: seligoroff
"""
import pytest

from mongo_pipebuilder import PipelineBuilder


class TestMatchExpr:
    """Tests for match_expr stage."""

    def test_match_expr_basic_adds_match_with_expr_stage(self):
        """match_expr(expr) adds one $match stage with $expr key."""
        builder = PipelineBuilder()
        expr = {"$eq": ["$id", "$$teamId"]}
        pipeline = builder.match_expr(expr).build()

        assert len(pipeline) == 1
        assert pipeline[0] == {"$match": {"$expr": expr}}

    def test_match_expr_empty_expr_allowed(self):
        """match_expr({}) adds $match stage with $expr: {}."""
        builder = PipelineBuilder()
        pipeline = builder.match_expr({}).build()

        assert len(pipeline) == 1
        assert pipeline[0] == {"$match": {"$expr": {}}}

    def test_match_expr_none_raises(self):
        """match_expr(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="expr cannot be None"):
            builder.match_expr(None)

    def test_match_expr_not_dict_raises(self):
        """match_expr(not a dict) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="expr must be a dict"):
            builder.match_expr([{"$eq": ["$a", "$b"]}])
        with pytest.raises(TypeError, match="expr must be a dict"):
            builder.match_expr("$eq")

    def test_match_expr_chaining(self):
        """match_expr returns self; can chain with match, limit, etc."""
        builder = PipelineBuilder()
        pipeline = (
            builder.match_expr({"$eq": ["$a", "$$b"]})
            .match({"status": "active"})
            .limit(5)
            .build()
        )
        assert len(pipeline) == 3
        assert pipeline[0] == {"$match": {"$expr": {"$eq": ["$a", "$$b"]}}}
        assert pipeline[1] == {"$match": {"status": "active"}}
        assert pipeline[2] == {"$limit": 5}

    def test_match_expr_copy_independent(self):
        """copy() after match_expr yields independent builder; modifying copy does not change original."""
        builder = PipelineBuilder()
        builder.match_expr({"$gte": ["$x", 0]})
        c = builder.copy()
        c.limit(10)
        assert len(builder) == 1
        assert len(c) == 2
        assert builder.build()[0] == {"$match": {"$expr": {"$gte": ["$x", 0]}}}
        assert c.build()[0] == {"$match": {"$expr": {"$gte": ["$x", 0]}}}
        assert c.build()[1] == {"$limit": 10}
