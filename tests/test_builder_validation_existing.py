"""
Validation tests for existing aggregation stages.

Tests error handling and validation for existing stages (match, lookup, group, etc.).
"""
import pytest

from mongo_pipebuilder import PipelineBuilder


class TestMatchValidation:
    """Tests for $match stage validation."""

    def test_match_none_raises_error(self):
        """Test that match(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="conditions cannot be None"):
            builder.match(None)

    def test_match_invalid_type_raises_error(self):
        """Test that match(123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="conditions must be a dict"):
            builder.match(123)

    def test_match_empty_dict_allowed(self):
        """Test that match({}) is allowed (skipped)."""
        builder = PipelineBuilder()
        pipeline = builder.match({}).build()
        assert pipeline == []


class TestLookupValidation:
    """Tests for $lookup stage validation."""

    def test_lookup_empty_strings_raise_error(self):
        """Test that lookup with empty strings raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="from_collection must be a non-empty string"):
            builder.lookup("", "local", "foreign", "as")
        with pytest.raises(ValueError, match="local_field must be a non-empty string"):
            builder.lookup("users", "", "foreign", "as")
        with pytest.raises(ValueError, match="foreign_field must be a non-empty string"):
            builder.lookup("users", "local", "", "as")
        with pytest.raises(ValueError, match="as_field must be a non-empty string"):
            builder.lookup("users", "local", "foreign", "")

    def test_lookup_non_string_arguments_raise_type_error(self):
        """Test that lookup with non-string arguments raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="from_collection must be a string"):
            builder.lookup(123, "local", "foreign", "as")
        with pytest.raises(TypeError, match="local_field must be a string"):
            builder.lookup("users", 123, "foreign", "as")
        with pytest.raises(TypeError, match="foreign_field must be a string"):
            builder.lookup("users", "local", None, "as")
        with pytest.raises(TypeError, match="as_field must be a string"):
            builder.lookup("users", "local", "foreign", ["as"])

    def test_lookup_invalid_pipeline_type_raises_error(self):
        """Test that lookup(..., pipeline=123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="pipeline must be a list"):
            builder.lookup("users", "userId", "_id", "user", pipeline=123)

    def test_lookup_pipeline_with_non_dict_stages_raises_error(self):
        """Test that lookup(..., pipeline=[123]) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="All pipeline stages must be dictionaries"):
            builder.lookup("users", "userId", "_id", "user", pipeline=[123])


class TestGroupValidation:
    """Tests for $group stage validation."""

    def test_group_empty_both_raises_error(self):
        """Test that group({}, {}) raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="group_by and accumulators cannot both be empty"):
            builder.group({}, {})

    def test_group_empty_group_by_allowed(self):
        """Test that group({}, {"count": {"$sum": 1}}) is allowed."""
        builder = PipelineBuilder()
        pipeline = builder.group({}, {"count": {"$sum": 1}}).build()
        assert pipeline == [{"$group": {"_id": {}, "count": {"$sum": 1}}}]

    def test_group_with_string_group_by(self):
        """Test that group() accepts string for group_by (field path)."""
        builder = PipelineBuilder()
        # String field path is valid in MongoDB
        pipeline = builder.group("$categoryType", {"total": {"$sum": "$amount"}}).build()
        assert pipeline == [{"$group": {"_id": "$categoryType", "total": {"$sum": "$amount"}}}]

    def test_group_empty_string_with_empty_accumulators_raises_error(self):
        """Test that group('', {}) raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="group_by and accumulators cannot both be empty"):
            builder.group("", {})

    def test_group_invalid_accumulators_type_raises_error(self):
        """Test that group({}, 123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="accumulators must be a dict"):
            builder.group({}, 123)

    def test_group_nested_id_wrapper_raises_error(self):
        """Test that group({'_id': ...}, ...) raises ValueError with guidance."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="Invalid group_by: you passed a dict wrapper"):
            builder.group({"_id": ["$a", "$b"]}, {"count": {"$sum": 1}})

    def test_group_id_in_accumulators_raises_error(self):
        """Test that group() rejects '_id' key in accumulators."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="accumulators cannot contain an '_id' key"):
            builder.group("$category", {"_id": "$other", "count": {"$sum": 1}})
        assert builder.build() == []

    def test_group_id_like_accumulator_names_allowed(self):
        """Test that accumulator names merely containing 'id' are still allowed."""
        builder = PipelineBuilder()
        pipeline = builder.group("$category", {"ids": {"$addToSet": "$_id"}}).build()
        assert pipeline == [{"$group": {"_id": "$category", "ids": {"$addToSet": "$_id"}}}]


class TestUnwindValidation:
    """Tests for $unwind stage validation."""

    def test_unwind_empty_string_raises_error(self):
        """Test that unwind('') raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="path cannot be empty"):
            builder.unwind("")

    def test_unwind_non_string_raises_error(self):
        """Test that unwind(123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="path must be a string"):
            builder.unwind(123)


class TestSortValidation:
    """Tests for $sort stage validation."""

    def test_sort_none_raises_error(self):
        """Test that sort(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="fields cannot be None"):
            builder.sort(None)

    def test_sort_invalid_type_raises_error(self):
        """Test that sort(123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="fields must be a dict"):
            builder.sort(123)


class TestLimitValidation:
    """Tests for $limit stage validation."""

    def test_limit_negative_raises_error(self):
        """Test that limit(-1) raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="limit cannot be negative"):
            builder.limit(-1)

    def test_limit_non_integer_raises_error(self):
        """Test that limit('10') raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="limit must be an integer"):
            builder.limit("10")

    def test_limit_zero_allowed(self):
        """Test that limit(0) is allowed (skipped)."""
        builder = PipelineBuilder()
        pipeline = builder.limit(0).build()
        assert pipeline == []

    def test_limit_bool_raises_error(self):
        """Test that limit(True/False) raises TypeError (bool is a subclass of int)."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="limit must be an integer"):
            builder.limit(True)
        with pytest.raises(TypeError, match="limit must be an integer"):
            builder.limit(False)
        assert builder.build() == []


class TestSkipValidation:
    """Tests for $skip stage validation."""

    def test_skip_negative_raises_error(self):
        """Test that skip(-1) raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="skip cannot be negative"):
            builder.skip(-1)

    def test_skip_non_integer_raises_error(self):
        """Test that skip('10') raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="skip must be an integer"):
            builder.skip("10")

    def test_skip_zero_allowed(self):
        """Test that skip(0) is allowed (skipped)."""
        builder = PipelineBuilder()
        pipeline = builder.skip(0).build()
        assert pipeline == []

    def test_skip_bool_raises_error(self):
        """Test that skip(True/False) raises TypeError (bool is a subclass of int)."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="skip must be an integer"):
            builder.skip(True)
        with pytest.raises(TypeError, match="skip must be an integer"):
            builder.skip(False)
        assert builder.build() == []


class TestAddFieldsValidation:
    """Tests for $addFields stage validation."""

    def test_add_fields_none_raises_error(self):
        """Test that add_fields(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="fields cannot be None"):
            builder.add_fields(None)

    def test_add_fields_invalid_type_raises_error(self):
        """Test that add_fields(123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="fields must be a dict"):
            builder.add_fields(123)


class TestProjectValidation:
    """Tests for $project stage validation."""

    def test_project_none_raises_error(self):
        """Test that project(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="fields cannot be None"):
            builder.project(None)

    def test_project_invalid_type_raises_error(self):
        """Test that project(123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="fields must be a dict"):
            builder.project(123)




