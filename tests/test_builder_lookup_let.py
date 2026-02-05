"""
Tests for PipelineBuilder.lookup_let ($lookup with let and pipeline).

Author: seligoroff
"""
import pytest

from mongo_pipebuilder import PipelineBuilder


class TestLookupLet:
    """Tests for lookup_let stage."""

    def test_lookup_let_basic_with_list_of_stages(self):
        """lookup_let with list of stages adds one $lookup stage with from, let, pipeline, as."""
        builder = PipelineBuilder()
        sub_pipeline = [
            {"$match": {"$expr": {"$eq": ["$id", "$$teamId"]}}},
            {"$project": {"name": 1, "_id": 0}},
        ]
        pipeline = builder.lookup_let(
            from_collection="sso_teams",
            let={"teamId": "$idTeam"},
            pipeline=sub_pipeline,
            as_field="team",
        ).build()

        assert len(pipeline) == 1
        assert "$lookup" in pipeline[0]
        stage = pipeline[0]["$lookup"]
        assert stage["from"] == "sso_teams"
        assert stage["let"] == {"teamId": "$idTeam"}
        assert stage["pipeline"] == sub_pipeline
        assert stage["as"] == "team"
        assert "localField" not in stage
        assert "foreignField" not in stage

    def test_lookup_let_with_pipeline_builder(self):
        """lookup_let accepts PipelineBuilder; pipeline in stage is the built list of stages."""
        sub = PipelineBuilder()
        sub.match({"active": True}).project({"name": 1, "_id": 0})

        builder = PipelineBuilder()
        pipeline = builder.lookup_let(
            from_collection="users",
            let={"userId": "$_id"},
            pipeline=sub,
            as_field="user",
        ).build()

        assert len(pipeline) == 1
        stage = pipeline[0]["$lookup"]
        assert stage["from"] == "users"
        assert stage["let"] == {"userId": "$_id"}
        assert stage["as"] == "user"
        assert stage["pipeline"] == [
            {"$match": {"active": True}},
            {"$project": {"name": 1, "_id": 0}},
        ]
        # passed builder unchanged
        assert len(sub) == 2

    def test_lookup_let_empty_let_allowed(self):
        """lookup_let accepts empty let dict."""
        builder = PipelineBuilder()
        pipeline = builder.lookup_let(
            from_collection="c",
            let={},
            pipeline=[{"$match": {"x": 1}}],
            as_field="out",
        ).build()
        assert pipeline[0]["$lookup"]["let"] == {}

    def test_lookup_let_from_collection_empty_raises(self):
        """lookup_let raises ValueError for empty from_collection."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="from_collection must be a non-empty string"):
            builder.lookup_let("", {"x": "$y"}, [{"$match": {}}], as_field="z")

    def test_lookup_let_from_collection_not_string_raises(self):
        """lookup_let raises TypeError if from_collection is not a string."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="from_collection must be a string"):
            builder.lookup_let(123, {"x": "$y"}, [{"$match": {}}], as_field="z")

    def test_lookup_let_let_none_raises(self):
        """lookup_let raises TypeError if let is None."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="let cannot be None"):
            builder.lookup_let("c", None, [{"$match": {}}], as_field="out")

    def test_lookup_let_let_not_dict_raises(self):
        """lookup_let raises TypeError if let is not a dict."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="let must be a dict"):
            builder.lookup_let("c", "not a dict", [{"$match": {}}], as_field="out")

    def test_lookup_let_pipeline_none_raises(self):
        """lookup_let raises TypeError if pipeline is None."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="pipeline cannot be None"):
            builder.lookup_let("c", {}, None, as_field="out")

    def test_lookup_let_pipeline_empty_list_raises(self):
        """lookup_let raises ValueError for empty pipeline list."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="pipeline cannot be empty"):
            builder.lookup_let("c", {}, [], as_field="out")

    def test_lookup_let_pipeline_list_with_non_dict_raises(self):
        """lookup_let raises TypeError if pipeline list contains non-dict."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="All pipeline stages must be dictionaries"):
            builder.lookup_let("c", {}, [{"$match": {}}, "not a stage"], as_field="out")

    def test_lookup_let_pipeline_not_list_or_builder_raises(self):
        """lookup_let raises TypeError if pipeline is neither list nor PipelineBuilder."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="pipeline must be a list or PipelineBuilder"):
            builder.lookup_let("c", {}, "not a pipeline", as_field="out")

    def test_lookup_let_as_field_empty_raises(self):
        """lookup_let raises ValueError for empty as_field."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="as_field must be a non-empty string"):
            builder.lookup_let("c", {}, [{"$match": {}}], as_field="")

    def test_lookup_let_as_field_not_string_raises(self):
        """lookup_let raises TypeError if as_field is not a string."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="as_field must be a string"):
            builder.lookup_let("c", {}, [{"$match": {}}], as_field=123)

    def test_lookup_let_chaining(self):
        """lookup_let returns self; can chain with match, limit, etc."""
        builder = PipelineBuilder()
        pipeline = (
            builder.lookup_let(
                from_collection="teams",
                let={"tid": "$teamId"},
                pipeline=[{"$match": {"$expr": {"$eq": ["$_id", "$$tid"]}}}],
                as_field="team",
            )
            .match({"status": "active"})
            .build()
        )
        assert len(pipeline) == 2
        assert pipeline[0]["$lookup"]["from"] == "teams"
        assert pipeline[0]["$lookup"]["as"] == "team"
        assert pipeline[1] == {"$match": {"status": "active"}}

    def test_lookup_let_copy_independent(self):
        """copy() after lookup_let yields independent builder; modifying copy does not change original."""
        builder = PipelineBuilder()
        builder.lookup_let(
            from_collection="x",
            let={"v": "$f"},
            pipeline=[{"$match": {}}],
            as_field="out",
        )
        c = builder.copy()
        c.limit(5)
        assert len(builder) == 1
        assert len(c) == 2
        assert builder.build()[0]["$lookup"]["from"] == "x"
        assert c.build()[0]["$lookup"]["from"] == "x"
        assert c.build()[1] == {"$limit": 5}
