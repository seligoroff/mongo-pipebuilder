# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2025-12-17

### Added
- `compare_with()` - Generate a unified diff between two pipelines (useful for legacy vs builder migrations)
- `pretty_print_stage()` - Pretty-print a single stage by index or by dict

### Changed
- `group()` now rejects `group_by={"_id": ...}` with a helpful `ValueError` explaining the correct usage (prevents nested `_id` mistakes)
- `get_stage_at()` now returns a deep copy of the stage to avoid accidental mutation of builder internals

### Fixed
- Updated README examples to reflect correct `group(group_by=...)` usage
- Added best practice docs for array `_id` after `$group` (prefer `$arrayElemAt` and materialize fields)

[0.3.1]: https://github.com/seligoroff/mongo-pipebuilder/releases/tag/v0.3.1

## [0.3.0] - 2025-12-16

### Added
- Debug and inspection methods (Phase 1):
  - `get_stage_at(index)` - Get a specific stage from the pipeline by index
  - `pretty_print()` - Return formatted JSON string representation of the pipeline
  - `to_json_file()` - Save the pipeline to a JSON file with optional metadata
- Comprehensive tests for new debug methods (18 new test cases)

### Changed
- Added `json` and `pathlib` imports to support debug methods
- Updated documentation with examples for new debug methods

[0.3.0]: https://github.com/seligoroff/mongo-pipebuilder/releases/tag/v0.3.0

## [0.2.3] - 2025-11-23

### Changed
- Improved code quality and maintainability:
  - Simplified `validate()` method by removing code duplication for `$out`/`$merge` validation
  - Optimized `get_stage_types()` method to use `next(iter())` instead of `list().keys()[0]` for better performance
  - Fixed docstring in `match()` method to correctly document `TypeError` instead of `ValueError`

[0.2.3]: https://github.com/seligoroff/mongo-pipebuilder/releases/tag/v0.2.3

## [0.2.2] - 2025-11-23

### Fixed
- `group()` method now accepts string field paths for `group_by` parameter
  - Previously only accepted dictionaries, but MongoDB allows strings (e.g., `"$categoryType"`)
  - This fixes compatibility with existing code that uses string field paths in `$group` stages
  - Both string paths and dictionary expressions are now supported

### Changed
- Updated `group()` method signature to accept `Union[str, Dict[str, Any], Any]` for `group_by`
- Improved validation to handle both string and dictionary `group_by` values correctly

[0.2.2]: https://github.com/seligoroff/mongo-pipebuilder/releases/tag/v0.2.2

## [0.2.1] - 2025-11-23

### Added
- Enhanced `validate()` method with critical MongoDB rule validation:
  - Validates that `$out` and `$merge` stages are the last stages in pipeline
  - Validates that `$out` and `$merge` are not used together
  - Provides clear error messages with stage positions

### Changed
- Improved `validate()` method to check pipeline structure beyond empty check

[0.2.1]: https://github.com/seligoroff/mongo-pipebuilder/releases/tag/v0.2.1

## [0.2.0] - 2025-11-22

### Added
- `prepend()` - Add stage at the beginning of pipeline
- `insert_at()` - Insert stage at specific position (0-based index)
- Debug and analysis methods:
  - `__len__()` - Get number of stages
  - `__repr__()` - String representation for debugging
  - `clear()` - Clear all stages
  - `copy()` - Create independent copy of builder
  - `validate()` - Validate pipeline before execution
  - `get_stage_types()` - Get list of stage types
  - `has_stage()` - Check if stage type exists
- Additional MongoDB aggregation stages:
  - `unset()` - Remove fields from documents
  - `replace_root()` / `replace_with()` - Replace root document
  - `facet()` - Parallel execution of sub-pipelines
  - `count()` - Count documents
  - `set_field()` - Alias for `$addFields`
- Comprehensive validation for all methods (fail-fast approach)
- Full test coverage (96.58%)
- Updated documentation with new methods and examples



## [0.1.0] - 2025-11-21

### Added
- Initial release
- `PipelineBuilder` class with fluent interface
- Support for core MongoDB aggregation stages:
  - `match()` - Filter documents
  - `lookup()` - Join collections (with optional nested pipelines)
  - `add_fields()` - Add or modify fields
  - `project()` - Reshape documents
  - `group()` - Group documents
  - `unwind()` - Deconstruct arrays
  - `sort()` - Sort documents
  - `limit()` - Limit results
  - `skip()` - Skip documents
  - `add_stage()` - Add custom stages
- Full type hints support
- Comprehensive test suite
- Examples and documentation



