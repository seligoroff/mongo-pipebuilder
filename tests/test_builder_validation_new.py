"""
Validation tests for new aggregation stages.

Tests error handling and validation for newly added stages.
"""
import pytest
from mongo_pipebuilder import PipelineBuilder


class TestUnsetValidation:
    """Tests for $unset stage validation."""

    def test_unset_none_raises_error(self):
        """Test that unset(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="fields cannot be None"):
            builder.unset(None)

    def test_unset_empty_string_raises_error(self):
        """Test that unset('') raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="fields cannot be an empty string"):
            builder.unset("")

    def test_unset_empty_list_raises_error(self):
        """Test that unset([]) raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="fields cannot be an empty list"):
            builder.unset([])

    def test_unset_list_with_empty_string_raises_error(self):
        """Test that unset(['field', '']) raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="fields list cannot contain empty strings"):
            builder.unset(["field1", ""])

    def test_unset_list_with_non_string_raises_error(self):
        """Test that unset(['field', 123]) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="all items in fields list must be strings"):
            builder.unset(["field1", 123])

    def test_unset_invalid_type_raises_error(self):
        """Test that unset(123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="fields must be a string or list of strings"):
            builder.unset(123)


class TestReplaceRootValidation:
    """Tests for $replaceRoot stage validation."""

    def test_replace_root_none_raises_error(self):
        """Test that replace_root(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="new_root cannot be None"):
            builder.replace_root(None)

    def test_replace_root_empty_dict_raises_error(self):
        """Test that replace_root({}) raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="new_root cannot be empty"):
            builder.replace_root({})

    def test_replace_root_missing_newroot_raises_error(self):
        """Test that replace_root without 'newRoot' key raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="new_root must contain 'newRoot' key"):
            builder.replace_root({"other": "value"})

    def test_replace_root_invalid_type_raises_error(self):
        """Test that replace_root(123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="new_root must be a dict"):
            builder.replace_root(123)


class TestReplaceWithValidation:
    """Tests for $replaceWith stage validation."""

    def test_replace_with_none_raises_error(self):
        """Test that replace_with(None) raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="replacement cannot be None"):
            builder.replace_with(None)


class TestFacetValidation:
    """Tests for $facet stage validation."""

    def test_facet_none_raises_error(self):
        """Test that facet(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="facets cannot be None"):
            builder.facet(None)

    def test_facet_empty_dict_raises_error(self):
        """Test that facet({}) raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="facets cannot be empty"):
            builder.facet({})

    def test_facet_invalid_type_raises_error(self):
        """Test that facet(123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="facets must be a dict"):
            builder.facet(123)

    def test_facet_value_not_list_raises_error(self):
        """Test that facet with non-list value raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="facet 'items' must be a list"):
            builder.facet({"items": "not a list"})

    def test_facet_stage_not_dict_raises_error(self):
        """Test that facet with non-dict stage raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="all stages in facet 'items' must be dictionaries"):
            builder.facet({"items": ["not a dict", 123]})


class TestCountValidation:
    """Tests for $count stage validation."""

    def test_count_none_raises_error(self):
        """Test that count(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="field_name cannot be None"):
            builder.count(None)

    def test_count_empty_string_raises_error(self):
        """Test that count('') raises ValueError."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="field_name cannot be empty"):
            builder.count("")

    def test_count_invalid_type_raises_error(self):
        """Test that count(123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="field_name must be a string"):
            builder.count(123)


class TestSetFieldValidation:
    """Tests for $set stage validation."""

    def test_set_field_none_raises_error(self):
        """Test that set_field(None) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="fields cannot be None"):
            builder.set_field(None)

    def test_set_field_invalid_type_raises_error(self):
        """Test that set_field(123) raises TypeError."""
        builder = PipelineBuilder()
        with pytest.raises(TypeError, match="fields must be a dict"):
            builder.set_field(123)




