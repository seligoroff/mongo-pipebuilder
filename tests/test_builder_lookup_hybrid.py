"""
Tests for PipelineBuilder.lookup_hybrid (combined $lookup: local/foreign + let + pipeline).

Author: seligoroff
"""
import pytest

from mongo_pipebuilder import PipelineBuilder


class TestLookupHybrid:
    """TDD tests for lookup_hybrid stage."""

    def test_lookup_hybrid_basic_combined_case(self):
        """lookup_hybrid adds one $lookup stage with local/foreign + let + pipeline + as."""
        builder = PipelineBuilder()
        sub_pipeline = [
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$$local_season_id", "$idSeason"]},
                            {"$eq": ["$$local_tournament_id", "$idTournament"]},
                        ]
                    }
                }
            }
        ]
        pipeline = builder.lookup_hybrid(
            from_collection="sso_matches",
            as_field="match",
            local_field="idMatch",
            foreign_field="id",
            let={
                "local_season_id": "$$season_id",
                "local_tournament_id": "$$tournament_id",
            },
            pipeline=sub_pipeline,
        ).build()

        assert len(pipeline) == 1
        assert "$lookup" in pipeline[0]
        stage = pipeline[0]["$lookup"]
        assert stage["from"] == "sso_matches"
        assert stage["as"] == "match"
        assert stage["localField"] == "idMatch"
        assert stage["foreignField"] == "id"
        assert stage["let"] == {
            "local_season_id": "$$season_id",
            "local_tournament_id": "$$tournament_id",
        }
        assert stage["pipeline"] == sub_pipeline

    def test_lookup_hybrid_with_pipeline_builder(self):
        """lookup_hybrid accepts PipelineBuilder for pipeline and keeps stage order."""
        sub = PipelineBuilder().match({"active": True}).project({"idSeason": 1, "_id": 0})
        builder = PipelineBuilder()
        pipeline = builder.lookup_hybrid(
            from_collection="sso_matches",
            as_field="match",
            local_field="idMatch",
            foreign_field="id",
            let={"local_season_id": "$$season_id"},
            pipeline=sub,
        ).build()

        assert len(pipeline) == 1
        stage = pipeline[0]["$lookup"]
        assert stage["pipeline"] == [
            {"$match": {"active": True}},
            {"$project": {"idSeason": 1, "_id": 0}},
        ]
        assert len(sub) == 2

    def test_lookup_hybrid_chaining(self):
        """lookup_hybrid returns self and supports chaining."""
        builder = PipelineBuilder()
        pipeline = (
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="match",
                local_field="idMatch",
                foreign_field="id",
                let={"local_season_id": "$$season_id"},
                pipeline=[{"$match": {"$expr": {"$eq": ["$$local_season_id", "$idSeason"]}}}],
            )
            .match({"status": "active"})
            .limit(1)
            .build()
        )
        assert len(pipeline) == 3
        assert "$lookup" in pipeline[0]
        assert pipeline[1] == {"$match": {"status": "active"}}
        assert pipeline[2] == {"$limit": 1}

    def test_lookup_hybrid_copy_independent(self):
        """copy() after lookup_hybrid yields independent builder."""
        builder = PipelineBuilder()
        builder.lookup_hybrid(
            from_collection="sso_matches",
            as_field="match",
            local_field="idMatch",
            foreign_field="id",
            let={"local_season_id": "$$season_id"},
            pipeline=[{"$match": {"$expr": {"$eq": ["$$local_season_id", "$idSeason"]}}}],
        )
        c = builder.copy()
        c.limit(5)

        assert len(builder) == 1
        assert len(c) == 2
        assert "$lookup" in builder.build()[0]
        assert "$lookup" in c.build()[0]
        assert c.build()[1] == {"$limit": 5}

    def test_lookup_hybrid_from_collection_not_string_raises(self):
        """lookup_hybrid raises TypeError when from_collection is not a string."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError):
            builder.lookup_hybrid(
                from_collection=123,
                as_field="m",
                local_field="idMatch",
                foreign_field="id",
                let={"x": "$$x"},
                pipeline=[{"$match": {}}],
            )

    def test_lookup_hybrid_from_collection_empty_raises(self):
        """lookup_hybrid raises ValueError when from_collection is empty."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError):
            builder.lookup_hybrid(
                from_collection="",
                as_field="m",
                local_field="idMatch",
                foreign_field="id",
                let={"x": "$$x"},
                pipeline=[{"$match": {}}],
            )

    def test_lookup_hybrid_as_field_not_string_raises(self):
        """lookup_hybrid raises TypeError when as_field is not a string."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field=123,
                local_field="idMatch",
                foreign_field="id",
                let={"x": "$$x"},
                pipeline=[{"$match": {}}],
            )

    def test_lookup_hybrid_as_field_empty_raises(self):
        """lookup_hybrid raises ValueError when as_field is empty."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="",
                local_field="idMatch",
                foreign_field="id",
                let={"x": "$$x"},
                pipeline=[{"$match": {}}],
            )

    def test_lookup_hybrid_only_local_field_raises(self):
        """lookup_hybrid raises ValueError when only local_field is provided."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="m",
                local_field="idMatch",
                let={"x": "$$x"},
                pipeline=[{"$match": {}}],
            )

    def test_lookup_hybrid_only_foreign_field_raises(self):
        """lookup_hybrid raises ValueError when only foreign_field is provided."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="m",
                foreign_field="id",
                let={"x": "$$x"},
                pipeline=[{"$match": {}}],
            )

    def test_lookup_hybrid_local_foreign_not_string_raises(self):
        """lookup_hybrid raises TypeError when local/foreign fields are not strings."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="m",
                local_field=123,
                foreign_field="id",
                let={"x": "$$x"},
                pipeline=[{"$match": {}}],
            )

    def test_lookup_hybrid_local_foreign_empty_raises(self):
        """lookup_hybrid raises ValueError when local/foreign fields are empty strings."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="m",
                local_field="",
                foreign_field="id",
                let={"x": "$$x"},
                pipeline=[{"$match": {}}],
            )

    def test_lookup_hybrid_let_not_dict_raises(self):
        """lookup_hybrid raises TypeError when let is not a dict."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="m",
                local_field="idMatch",
                foreign_field="id",
                let="not-a-dict",
                pipeline=[{"$match": {}}],
            )

    def test_lookup_hybrid_let_without_pipeline_raises(self):
        """lookup_hybrid raises ValueError when let is provided without pipeline."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="m",
                local_field="idMatch",
                foreign_field="id",
                let={"x": "$$x"},
            )

    def test_lookup_hybrid_pipeline_not_list_or_builder_raises(self):
        """lookup_hybrid raises TypeError when pipeline is invalid type."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="m",
                local_field="idMatch",
                foreign_field="id",
                let={"x": "$$x"},
                pipeline="not-a-pipeline",
            )

    def test_lookup_hybrid_pipeline_list_with_non_dict_raises(self):
        """lookup_hybrid raises TypeError when pipeline list contains non-dict stage."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="m",
                local_field="idMatch",
                foreign_field="id",
                let={"x": "$$x"},
                pipeline=[{"$match": {}}, "not-a-stage"],
            )

    def test_lookup_hybrid_pipeline_empty_raises(self):
        """lookup_hybrid raises ValueError when pipeline is an empty list."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="m",
                local_field="idMatch",
                foreign_field="id",
                let={"x": "$$x"},
                pipeline=[],
            )

    def test_lookup_hybrid_pipeline_without_let_raises(self):
        """lookup_hybrid raises ValueError for pipeline-only mode (non-target use case)."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError):
            builder.lookup_hybrid(
                from_collection="sso_matches",
                as_field="m",
                local_field="idMatch",
                foreign_field="id",
                pipeline=[{"$match": {}}],
            )
