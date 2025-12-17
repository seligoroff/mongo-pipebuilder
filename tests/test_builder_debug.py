"""
Tests for PipelineBuilder debug and analysis methods.

Tests for methods from Proposal 2 (debug/validation) and Proposal 4 (pipeline analysis).

Author: seligoroff
"""
import json
import tempfile
from pathlib import Path

import pytest
from mongo_pipebuilder import PipelineBuilder


class TestPipelineBuilderDebug:
    """Tests for Proposal 2: Debug and validation methods."""

    def test_len_empty_builder(self):
        """Test __len__ with empty builder."""
        builder = PipelineBuilder()
        assert len(builder) == 0

    def test_len_single_stage(self):
        """Test __len__ with single stage."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        assert len(builder) == 1

    def test_len_multiple_stages(self):
        """Test __len__ with multiple stages."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10).sort({"name": 1})
        assert len(builder) == 3

    def test_repr_empty_builder(self):
        """Test __repr__ with empty builder."""
        builder = PipelineBuilder()
        assert repr(builder) == "PipelineBuilder(stages=0)"

    def test_repr_single_stage(self):
        """Test __repr__ with single stage."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        repr_str = repr(builder)
        assert "PipelineBuilder(stages=1" in repr_str
        assert "$match" in repr_str

    def test_repr_multiple_stages(self):
        """Test __repr__ with multiple stages (up to 3)."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10).sort({"name": 1})
        repr_str = repr(builder)
        assert "PipelineBuilder(stages=3" in repr_str
        assert "$match" in repr_str
        assert "$limit" in repr_str
        assert "$sort" in repr_str
        assert "..." not in repr_str  # No ellipsis for 3 stages

    def test_repr_many_stages(self):
        """Test __repr__ with more than 3 stages (should show ellipsis)."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10).sort({"name": 1}).skip(5)
        repr_str = repr(builder)
        assert "PipelineBuilder(stages=4" in repr_str
        assert "..." in repr_str  # Should show ellipsis for more than 3 stages

    def test_clear_empty_builder(self):
        """Test clear() on empty builder."""
        builder = PipelineBuilder()
        result = builder.clear()
        assert len(builder) == 0
        assert result is builder  # Should return self for chaining

    def test_clear_with_stages(self):
        """Test clear() removes all stages."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10).sort({"name": 1})
        assert len(builder) == 3
        
        result = builder.clear()
        assert len(builder) == 0
        assert result is builder  # Should return self for chaining
        assert builder.build() == []

    def test_clear_returns_self(self):
        """Test that clear() returns self for method chaining."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        result = builder.clear().match({"new": "status"})
        assert len(builder) == 1
        assert result is builder

    def test_copy_empty_builder(self):
        """Test copy() on empty builder."""
        builder1 = PipelineBuilder()
        builder2 = builder1.copy()
        
        assert builder2 is not builder1  # Different objects
        assert len(builder2) == 0
        assert builder2.build() == []

    def test_copy_with_stages(self):
        """Test copy() creates independent copy."""
        builder1 = PipelineBuilder()
        builder1.match({"status": "active"}).limit(10)
        
        builder2 = builder1.copy()
        
        # Verify copy has same stages
        assert len(builder2) == 2
        assert builder2.build() == builder1.build()
        
        # Modify copy - original should be unchanged
        builder2.sort({"name": 1})
        assert len(builder1) == 2  # Original unchanged
        assert len(builder2) == 3  # Copy modified
        
        # Modify original - copy should be unchanged
        builder1.skip(5)
        assert len(builder1) == 3  # Original modified
        assert len(builder2) == 3  # Copy unchanged (already had 3 stages)

    def test_copy_independence(self):
        """Test that copy() creates truly independent copy."""
        builder1 = PipelineBuilder()
        builder1.match({"status": "active"})
        
        builder2 = builder1.copy()
        builder2.limit(10)
        
        # Verify they are independent
        assert builder1.build() == [{"$match": {"status": "active"}}]
        assert builder2.build() == [
            {"$match": {"status": "active"}},
            {"$limit": 10}
        ]

    def test_validate_empty_builder_raises_error(self):
        """Test validate() raises ValueError on empty pipeline."""
        builder = PipelineBuilder()
        with pytest.raises(ValueError, match="Pipeline cannot be empty"):
            builder.validate()

    def test_validate_with_stages_returns_true(self):
        """Test validate() returns True for valid pipeline."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        assert builder.validate() is True

    def test_validate_with_multiple_stages_returns_true(self):
        """Test validate() returns True for pipeline with multiple stages."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10).sort({"name": 1})
        assert builder.validate() is True

    def test_validate_out_as_last_stage(self):
        """Test validate() allows $out as the last stage."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).add_stage({"$out": "output_collection"})
        assert builder.validate() is True

    def test_validate_out_not_last_raises_error(self):
        """Test validate() raises error if $out is not the last stage."""
        builder = PipelineBuilder()
        builder.add_stage({"$out": "output_collection"}).match({"status": "active"})
        with pytest.raises(ValueError, match="\\$out stage must be the last stage"):
            builder.validate()

    def test_validate_out_in_middle_raises_error(self):
        """Test validate() raises error if $out is in the middle of pipeline."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).add_stage({"$out": "output_collection"}).limit(10)
        with pytest.raises(ValueError, match="\\$out stage must be the last stage"):
            builder.validate()

    def test_validate_merge_as_last_stage(self):
        """Test validate() allows $merge as the last stage."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).add_stage({"$merge": {"into": "output_collection"}})
        assert builder.validate() is True

    def test_validate_merge_not_last_raises_error(self):
        """Test validate() raises error if $merge is not the last stage."""
        builder = PipelineBuilder()
        builder.add_stage({"$merge": {"into": "output_collection"}}).match({"status": "active"})
        with pytest.raises(ValueError, match="\\$merge stage must be the last stage"):
            builder.validate()

    def test_validate_both_out_and_merge_raises_error(self):
        """Test validate() raises error if pipeline contains both $out and $merge."""
        builder = PipelineBuilder()
        builder.add_stage({"$out": "output1"}).add_stage({"$merge": {"into": "output2"}})
        with pytest.raises(ValueError, match="cannot contain both \\$out and \\$merge"):
            builder.validate()


class TestPipelineBuilderAnalysis:
    """Tests for Proposal 4: Pipeline analysis methods."""

    def test_get_stage_types_empty_builder(self):
        """Test get_stage_types() on empty builder."""
        builder = PipelineBuilder()
        assert builder.get_stage_types() == []

    def test_get_stage_types_single_stage(self):
        """Test get_stage_types() with single stage."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        assert builder.get_stage_types() == ["$match"]

    def test_get_stage_types_multiple_stages(self):
        """Test get_stage_types() with multiple stages."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10).sort({"name": 1})
        assert builder.get_stage_types() == ["$match", "$limit", "$sort"]

    def test_get_stage_types_preserves_order(self):
        """Test get_stage_types() preserves stage order."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).lookup(
            from_collection="users",
            local_field="userId",
            foreign_field="_id",
            as_field="user"
        ).limit(10)
        assert builder.get_stage_types() == ["$match", "$lookup", "$limit"]

    def test_get_stage_types_with_duplicate_stages(self):
        """Test get_stage_types() with duplicate stage types."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).match({"deleted": False}).limit(10)
        assert builder.get_stage_types() == ["$match", "$match", "$limit"]

    def test_has_stage_empty_builder(self):
        """Test has_stage() on empty builder."""
        builder = PipelineBuilder()
        assert builder.has_stage("$match") is False

    def test_has_stage_present(self):
        """Test has_stage() when stage is present."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        assert builder.has_stage("$match") is True

    def test_has_stage_not_present(self):
        """Test has_stage() when stage is not present."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10)
        assert builder.has_stage("$match") is True
        assert builder.has_stage("$limit") is True
        assert builder.has_stage("$group") is False
        assert builder.has_stage("$lookup") is False

    def test_has_stage_multiple_occurrences(self):
        """Test has_stage() with multiple occurrences of same stage."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).match({"deleted": False})
        assert builder.has_stage("$match") is True

    def test_has_stage_invalid_type_raises_error(self):
        """Test has_stage() raises TypeError for non-string argument."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        with pytest.raises(TypeError, match="stage_type must be a string"):
            builder.has_stage(123)
        
        with pytest.raises(TypeError, match="stage_type must be a string"):
            builder.has_stage(None)
        
        with pytest.raises(TypeError, match="stage_type must be a string"):
            builder.has_stage(["$match"])

    def test_has_stage_case_sensitive(self):
        """Test has_stage() is case sensitive."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        assert builder.has_stage("$match") is True
        assert builder.has_stage("$MATCH") is False  # Case sensitive

    def test_get_stage_types_and_has_stage_consistency(self):
        """Test consistency between get_stage_types() and has_stage()."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10).sort({"name": 1})
        
        stage_types = builder.get_stage_types()
        assert builder.has_stage("$match") is True
        assert builder.has_stage("$limit") is True
        assert builder.has_stage("$sort") is True
        
        # All stages from get_stage_types should return True in has_stage
        for stage_type in stage_types:
            assert builder.has_stage(stage_type) is True


class TestPipelineBuilderDebugMethods:
    """Tests for Phase 1 debug methods: pretty_print, to_json_file, get_stage_at."""

    def test_get_stage_at_valid_index(self):
        """Test get_stage_at() with valid index."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10).sort({"name": 1})
        
        stage0 = builder.get_stage_at(0)
        assert stage0 == {"$match": {"status": "active"}}
        
        stage1 = builder.get_stage_at(1)
        assert stage1 == {"$limit": 10}
        
        stage2 = builder.get_stage_at(2)
        assert stage2 == {"$sort": {"name": 1}}

    def test_get_stage_at_returns_copy(self):
        """Test that get_stage_at() returns a copy, not reference."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        stage = builder.get_stage_at(0)
        stage["$match"]["new_field"] = "value"
        
        # Original should be unchanged
        original_stage = builder.get_stage_at(0)
        assert "new_field" not in original_stage["$match"]

    def test_get_stage_at_invalid_index_negative(self):
        """Test get_stage_at() raises IndexError for negative index."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        with pytest.raises(IndexError, match="Index -1 out of range"):
            builder.get_stage_at(-1)

    def test_get_stage_at_invalid_index_too_large(self):
        """Test get_stage_at() raises IndexError for index too large."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        with pytest.raises(IndexError, match="Index 10 out of range"):
            builder.get_stage_at(10)

    def test_get_stage_at_empty_builder(self):
        """Test get_stage_at() raises IndexError on empty builder."""
        builder = PipelineBuilder()
        
        with pytest.raises(IndexError, match="Index 0 out of range"):
            builder.get_stage_at(0)

    def test_pretty_print_empty_builder(self):
        """Test pretty_print() with empty builder."""
        builder = PipelineBuilder()
        result = builder.pretty_print()
        
        assert result == "[]"
        # Should be valid JSON
        json.loads(result)

    def test_pretty_print_single_stage(self):
        """Test pretty_print() with single stage."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        result = builder.pretty_print()
        
        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed == [{"$match": {"status": "active"}}]
        
        # Should contain expected content
        assert "$match" in result
        assert "status" in result
        assert "active" in result

    def test_pretty_print_multiple_stages(self):
        """Test pretty_print() with multiple stages."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10).sort({"name": 1})
        result = builder.pretty_print()
        
        # Should be valid JSON
        parsed = json.loads(result)
        assert len(parsed) == 3
        assert parsed[0] == {"$match": {"status": "active"}}
        assert parsed[1] == {"$limit": 10}
        assert parsed[2] == {"$sort": {"name": 1}}

    def test_pretty_print_custom_indent(self):
        """Test pretty_print() with custom indent."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        result = builder.pretty_print(indent=4)
        
        # Should be valid JSON
        parsed = json.loads(result)
        assert parsed == [{"$match": {"status": "active"}}]
        
        # Should use 4 spaces for indentation
        lines = result.split("\n")
        if len(lines) > 1:
            assert lines[1].startswith("    ")  # 4 spaces

    def test_pretty_print_ensure_ascii(self):
        """Test pretty_print() with ensure_ascii=True."""
        builder = PipelineBuilder()
        builder.match({"name": "тест"})  # Non-ASCII characters
        result_ascii = builder.pretty_print(ensure_ascii=True)
        result_no_ascii = builder.pretty_print(ensure_ascii=False)
        
        # Both should be valid JSON
        json.loads(result_ascii)
        json.loads(result_no_ascii)
        
        # Non-ASCII version should contain original characters
        assert "тест" in result_no_ascii

    def test_to_json_file_basic(self):
        """Test to_json_file() saves pipeline correctly."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_pipeline.json"
            builder.to_json_file(filepath)
            
            # File should exist
            assert filepath.exists()
            
            # Should contain valid JSON
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            assert "pipeline" in data
            assert data["pipeline"] == [
                {"$match": {"status": "active"}},
                {"$limit": 10}
            ]

    def test_to_json_file_with_metadata(self):
        """Test to_json_file() with metadata."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        metadata = {
            "version": "1.0",
            "author": "developer",
            "description": "Test pipeline"
        }
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_pipeline.json"
            builder.to_json_file(filepath, metadata=metadata)
            
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            assert "pipeline" in data
            assert "metadata" in data
            assert data["metadata"] == metadata

    def test_to_json_file_creates_directory(self):
        """Test to_json_file() creates parent directories if they don't exist."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "nested" / "path" / "test_pipeline.json"
            
            # Directory shouldn't exist yet
            assert not filepath.parent.exists()
            
            builder.to_json_file(filepath)
            
            # File and directory should be created
            assert filepath.exists()
            assert filepath.parent.exists()

    def test_to_json_file_string_path(self):
        """Test to_json_file() accepts string path."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = str(Path(tmpdir) / "test_pipeline.json")
            builder.to_json_file(filepath)
            
            assert Path(filepath).exists()

    def test_to_json_file_custom_indent(self):
        """Test to_json_file() with custom indent."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_pipeline.json"
            builder.to_json_file(filepath, indent=4)
            
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
                lines = content.split("\n")
                if len(lines) > 1:
                    assert lines[1].startswith("    ")  # 4 spaces

    def test_pretty_print_and_to_json_file_consistency(self):
        """Test that pretty_print() and to_json_file() produce consistent output."""
        builder = PipelineBuilder()
        builder.match({"status": "active"}).limit(10)
        
        pretty_output = builder.pretty_print()
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_pipeline.json"
            builder.to_json_file(filepath)
            
            with open(filepath, "r", encoding="utf-8") as f:
                file_data = json.load(f)
            
            # Pipeline in file should match pretty_print output when parsed
            pretty_parsed = json.loads(pretty_output)
            assert file_data["pipeline"] == pretty_parsed

    def test_get_stage_at_with_complex_stage(self):
        """Test get_stage_at() with complex stage (e.g., lookup)."""
        builder = PipelineBuilder()
        builder.match({"status": "active"})
        builder.lookup(
            from_collection="users",
            local_field="userId",
            foreign_field="_id",
            as_field="user"
        )
        
        stage = builder.get_stage_at(1)
        assert "$lookup" in stage
        assert stage["$lookup"]["from"] == "users"
        assert stage["$lookup"]["localField"] == "userId"
        assert stage["$lookup"]["foreignField"] == "_id"
        assert stage["$lookup"]["as"] == "user"

    def test_compare_with_no_differences(self):
        """Test compare_with() returns 'No differences.' for identical pipelines."""
        a = PipelineBuilder().match({"status": "active"}).limit(10)
        b = PipelineBuilder().match({"status": "active"}).limit(10)
        assert a.compare_with(b) == "No differences."

    def test_compare_with_has_diff(self):
        """Test compare_with() returns unified diff when pipelines differ."""
        legacy = PipelineBuilder().match({"status": "active"})
        new = PipelineBuilder().match({"status": "inactive"})
        diff = new.compare_with(legacy)
        assert diff != "No differences."
        assert "--- new" in diff
        assert "+++ other" in diff
        assert "\"active\"" in diff or "active" in diff
        assert "\"inactive\"" in diff or "inactive" in diff

    def test_compare_with_invalid_other_raises(self):
        """Test compare_with() validates 'other' argument."""
        builder = PipelineBuilder().match({"status": "active"})
        with pytest.raises(TypeError, match="other must be a PipelineBuilder"):
            builder.compare_with([])  # type: ignore[arg-type]

    def test_compare_with_negative_context_lines_raises(self):
        """Test compare_with() validates context_lines."""
        a = PipelineBuilder().match({"status": "active"})
        b = PipelineBuilder().match({"status": "inactive"})
        with pytest.raises(ValueError, match="context_lines cannot be negative"):
            a.compare_with(b, context_lines=-1)

    def test_pretty_print_stage_by_index(self):
        """Test pretty_print_stage() with stage index."""
        builder = PipelineBuilder().match({"status": "active"}).limit(10)
        s = builder.pretty_print_stage(0)
        parsed = json.loads(s)
        assert parsed == {"$match": {"status": "active"}}

    def test_pretty_print_stage_by_dict(self):
        """Test pretty_print_stage() with stage dict."""
        builder = PipelineBuilder()
        stage = {"$limit": 5}
        s = builder.pretty_print_stage(stage, indent=4)
        parsed = json.loads(s)
        assert parsed == stage

    def test_pretty_print_stage_invalid_type_raises(self):
        """Test pretty_print_stage() validates stage argument."""
        builder = PipelineBuilder().match({"status": "active"})
        with pytest.raises(TypeError, match="stage must be an int index or a dict"):
            builder.pretty_print_stage("0")  # type: ignore[arg-type]

