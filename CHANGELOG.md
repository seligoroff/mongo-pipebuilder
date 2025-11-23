# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025-11-23

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

[0.2.0]: https://github.com/seligoroff/mongo-pipebuilder/releases/tag/v0.2.0

## [0.1.0] - 2025-11-22

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



[0.1.0]: https://github.com/yourusername/mongo-pipebuilder/releases/tag/v0.1.0

