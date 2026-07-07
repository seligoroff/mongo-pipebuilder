"""
Tests for PipelineBuilder.unwind path prefix ($) — MongoDB-valid $unwind path.

Reproduces issue: localdocs/issues/unwind-path-prefix
"""
from mongo_pipebuilder import PipelineBuilder


class TestUnwindPathPrefix:
    """$unwind path must be a MongoDB field path starting with $."""

    def test_unwind_path_auto_prefixes_dollar(self):
        """unwind('tags') must produce path '$tags', not bare 'tags'."""
        pipeline = PipelineBuilder().unwind("tags").build()

        assert pipeline == [{"$unwind": {"path": "$tags"}}]

    def test_unwind_path_keeps_existing_dollar_prefix(self):
        """unwind('$tags') must not double the prefix."""
        pipeline = PipelineBuilder().unwind("$tags").build()

        assert pipeline == [{"$unwind": {"path": "$tags"}}]

    def test_unwind_path_with_options_uses_prefixed_path(self):
        """Options must not change dollar-prefix behavior for path."""
        pipeline = PipelineBuilder().unwind(
            "items",
            preserve_null_and_empty_arrays=True,
            include_array_index="itemIndex",
        ).build()

        assert pipeline == [{
            "$unwind": {
                "path": "$items",
                "preserveNullAndEmptyArrays": True,
                "includeArrayIndex": "itemIndex",
            }
        }]
