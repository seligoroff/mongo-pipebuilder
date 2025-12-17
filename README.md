# mongo-pipebuilder

[![PyPI version](https://badge.fury.io/py/mongo-pipebuilder.svg)](https://badge.fury.io/py/mongo-pipebuilder)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Test Coverage](https://img.shields.io/badge/coverage-96%25-green.svg)](https://github.com/seligoroff/mongo-pipebuilder)

Type-safe, fluent MongoDB aggregation pipeline builder for Python.

## Overview

`mongo-pipebuilder` provides a clean, type-safe way to build MongoDB aggregation pipelines using the Builder Pattern with a fluent interface for maximum readability and safety.

## Features

- **Type-safe**: Full type hints support with IDE autocomplete
- **Fluent interface**: Chain methods for readable, maintainable code
- **Zero dependencies**: Pure Python, lightweight package
- **Extensible**: Easy to add custom stages via `add_stage()`
- **Well tested**: Comprehensive test suite with 96%+ coverage

## Installation

```bash
pip install mongo-pipebuilder
```

## Quick Start

```python
from mongo_pipebuilder import PipelineBuilder

# Build a pipeline
pipeline = (
    PipelineBuilder()
    .match({"status": "active"})
    .lookup(
        from_collection="users",
        local_field="userId",
        foreign_field="_id",
        as_field="user"
    )
    .project({"name": 1, "user.email": 1})
    .sort({"name": 1})
    .limit(10)
    .build()
)

# Use with pymongo
from pymongo import MongoClient
client = MongoClient()
collection = client.db.my_collection
results = collection.aggregate(pipeline)
```

## API Reference

### PipelineBuilder

Main class for building aggregation pipelines.

#### Methods

##### `match(conditions: Dict[str, Any]) -> Self`

Adds a `$match` stage to filter documents.

```python
.match({"status": "active", "age": {"$gte": 18}})
```

##### `lookup(from_collection: str, local_field: str, foreign_field: str, as_field: str, pipeline: Optional[List[Dict[str, Any]]] = None) -> Self`

Adds a `$lookup` stage to join with another collection.

```python
.lookup(
    from_collection="users",
    local_field="userId",
    foreign_field="_id",
    as_field="user",
    pipeline=[{"$match": {"active": True}}]  # Optional nested pipeline
)
```

##### `add_fields(fields: Dict[str, Any]) -> Self`

Adds a `$addFields` stage to add or modify fields.

```python
.add_fields({"fullName": {"$concat": ["$firstName", " ", "$lastName"]}})
```

##### `project(fields: Dict[str, Any]) -> Self`

Adds a `$project` stage to reshape documents.

```python
.project({"name": 1, "email": 1, "_id": 0})
```

##### `group(group_by: Dict[str, Any], accumulators: Dict[str, Any]) -> Self`

Adds a `$group` stage to group documents.

```python
.group(
    group_by={"category": "$category"},
    accumulators={"total": {"$sum": "$amount"}}
)
```

##### `unwind(path: str, preserve_null_and_empty_arrays: bool = False, include_array_index: Optional[str] = None) -> Self`

Adds a `$unwind` stage to deconstruct arrays.

```python
.unwind("tags", preserve_null_and_empty_arrays=True)
.unwind("items", include_array_index="itemIndex")
```

##### `sort(fields: Dict[str, int]) -> Self`

Adds a `$sort` stage.

```python
.sort({"createdAt": -1, "name": 1})
```

##### `limit(limit: int) -> Self`

Adds a `$limit` stage.

```python
.limit(10)
```

##### `skip(skip: int) -> Self`

Adds a `$skip` stage.

```python
.skip(20)
```

##### `unset(fields: Union[str, List[str]]) -> Self`

Adds a `$unset` stage to remove fields from documents.

```python
.unset("temp_field")
.unset(["field1", "field2", "field3"])
```

##### `replace_root(new_root: Dict[str, Any]) -> Self`

Adds a `$replaceRoot` stage to replace the root document.

```python
.replace_root({"newRoot": "$embedded"})
.replace_root({"newRoot": {"$mergeObjects": ["$doc1", "$doc2"]}})
```

##### `replace_with(replacement: Any) -> Self`

Adds a `$replaceWith` stage (alias for `$replaceRoot` in MongoDB 4.2+).

```python
.replace_with("$embedded")
.replace_with({"$mergeObjects": ["$doc1", "$doc2"]})
```

##### `facet(facets: Dict[str, List[Dict[str, Any]]]) -> Self`

Adds a `$facet` stage for parallel execution of multiple sub-pipelines.

```python
.facet({
    "items": [{"$skip": 10}, {"$limit": 20}],
    "meta": [{"$count": "total"}]
})
```

##### `count(field_name: str = "count") -> Self`

Adds a `$count` stage to count documents.

```python
.match({"status": "active"}).count("active_count")
```

##### `set_field(fields: Dict[str, Any]) -> Self`

Adds a `$set` stage (alias for `$addFields` in MongoDB 3.4+).

```python
.set_field({"status": "active", "updatedAt": "$$NOW"})
```

##### `add_stage(stage: Dict[str, Any]) -> Self`

Adds a custom stage for advanced use cases.

```python
.add_stage({"$facet": {
    "categories": [{"$group": {"_id": "$category"}}],
    "total": [{"$count": "count"}]
}})
```

##### `prepend(stage: Dict[str, Any]) -> Self`

Adds a stage at the beginning of the pipeline.

```python
builder.match({"status": "active"})
builder.prepend({"$match": {"deleted": False}})
# Pipeline: [{"$match": {"deleted": False}}, {"$match": {"status": "active"}}]
```

##### `insert_at(position: int, stage: Dict[str, Any]) -> Self`

Inserts a stage at a specific position (0-based index) in the pipeline.

```python
builder.match({"status": "active"}).group("$category", {"count": {"$sum": 1}})
builder.insert_at(1, {"$sort": {"name": 1}})
# Pipeline: [{"$match": {...}}, {"$sort": {...}}, {"$group": {...}}]
```

**Note:** For inserting before a specific stage type, combine with `get_stage_types()`:

```python
stage_types = builder.get_stage_types()
group_index = stage_types.index("$group")
builder.insert_at(group_index, {"$addFields": {"x": 1}})
```

##### `copy() -> PipelineBuilder`

Creates an independent copy of the builder with current stages. Useful for creating immutable variants and composing pipelines.

```python
builder1 = PipelineBuilder().match({"status": "active"})
builder2 = builder1.copy()
builder2.limit(10)

# Original unchanged
assert len(builder1) == 1
assert len(builder2) == 2
```

See [Composing and Reusing Pipelines](#composing-and-reusing-pipelines) for practical examples.

##### `validate() -> bool`

Validates the pipeline before execution. Checks that:
- Pipeline is not empty
- `$out` and `$merge` stages are the last stages (critical MongoDB rule)
- `$out` and `$merge` are not used together

```python
builder = PipelineBuilder()
builder.match({"status": "active"}).validate()  # Returns True

# Invalid: $out not last
builder.add_stage({"$out": "output"}).match({"status": "active"})
builder.validate()  # Raises ValueError: $out stage must be the last stage
```

##### `get_stage_at(index: int) -> Dict[str, Any]`

Gets a specific stage from the pipeline by index. Returns a copy of the stage.

```python
builder = PipelineBuilder()
builder.match({"status": "active"}).limit(10)
stage = builder.get_stage_at(0)  # Returns {"$match": {"status": "active"}}
```

##### `pretty_print(indent: int = 2, ensure_ascii: bool = False) -> str`

Returns a formatted JSON string representation of the pipeline. Useful for debugging.

```python
builder = PipelineBuilder()
builder.match({"status": "active"}).limit(10)
print(builder.pretty_print())
# [
#   {
#     "$match": {
#       "status": "active"
#     }
#   },
#   {
#     "$limit": 10
#   }
# ]
```

##### `pretty_print_stage(stage: Union[int, Dict[str, Any]], indent: int = 2, ensure_ascii: bool = False) -> str`

Returns a formatted JSON string representation of a single stage (by index or by dict).

```python
builder = PipelineBuilder().match({"status": "active"}).limit(10)
print(builder.pretty_print_stage(0))  # Prints the $match stage
```

##### `to_json_file(filepath: Union[str, Path], indent: int = 2, ensure_ascii: bool = False, metadata: Optional[Dict[str, Any]] = None) -> None`

Saves the pipeline to a JSON file. Useful for debugging, comparison, or versioning.

```python
builder = PipelineBuilder()
builder.match({"status": "active"}).limit(10)

# Basic usage
builder.to_json_file("debug_pipeline.json")

# With metadata
builder.to_json_file(
    "pipeline.json",
    metadata={"version": "1.0", "author": "developer"}
)
```

##### `compare_with(other: PipelineBuilder, context_lines: int = 3) -> str`

Returns a unified diff between two pipelines (useful for comparing “new” builder pipelines vs legacy/template pipelines).

```python
legacy = PipelineBuilder().match({"status": "active"}).limit(10)
new = PipelineBuilder().match({"status": "inactive"}).limit(10)

print(new.compare_with(legacy))
```

##### `build() -> List[Dict[str, Any]]`

Returns the complete pipeline as a list of stage dictionaries.

## Examples

### Complex Pipeline with Nested Lookup

```python
pipeline = (
    PipelineBuilder()
    .match({"status": "published"})
    .lookup(
        from_collection="authors",
        local_field="authorId",
        foreign_field="_id",
        as_field="author"
    )
    .unwind("author", preserve_null_and_empty_arrays=True)
    .lookup(
        from_collection="categories",
        local_field="categoryId",
        foreign_field="_id",
        as_field="category",
        pipeline=[
            {"$match": {"active": True}},
            {"$project": {"name": 1, "slug": 1}}
        ]
    )
    .unwind("category")
    .add_fields({
        "authorName": "$author.name",
        "categoryName": "$category.name"
    })
    .project({
        "title": 1,
        "authorName": 1,
        "categoryName": 1,
        "publishedAt": 1
    })
    .sort({"publishedAt": -1})
    .limit(20)
    .build()
)
```

### Aggregation with Grouping

```python
pipeline = (
    PipelineBuilder()
    .match({"date": {"$gte": "2024-01-01"}})
    .group(
        group_by={"month": {"$dateToString": {"format": "%Y-%m", "date": "$date"}}},
        accumulators={
            "totalSales": {"$sum": "$amount"},
            "avgAmount": {"$avg": "$amount"},
            "count": {"$sum": 1}
        }
    )
    .sort({"month": 1})
    .build()
)
```

### Composing and Reusing Pipelines

The `copy()` method allows you to create immutable variants of pipelines, enabling safe composition and reuse. This is useful when you need to:
- Create multiple variants from a base pipeline
- Compose pipelines functionally
- Cache base pipelines safely
- Pass pipelines to functions without side effects

#### Example: Building Multiple Variants from a Base Pipeline

```python
from mongo_pipebuilder import PipelineBuilder

# Base pipeline with common filtering and joining
base_pipeline = (
    PipelineBuilder()
    .match({"status": "published", "deleted": False})
    .lookup(
        from_collection="authors",
        local_field="authorId",
        foreign_field="_id",
        as_field="author"
    )
    .unwind("author", preserve_null_and_empty_arrays=True)
    .project({
        "title": 1,
        "authorName": "$author.name",
        "publishedAt": 1
    })
)

# Create variants with different sorting and limits
recent_posts = base_pipeline.copy().sort({"publishedAt": -1}).limit(10).build()
popular_posts = base_pipeline.copy().sort({"views": -1}).limit(5).build()
author_posts = base_pipeline.copy().match({"authorName": "John Doe"}).build()

# Base pipeline remains unchanged
assert len(base_pipeline) == 4  # Still has 4 stages
```

#### Example: Functional Composition Pattern

```python
def add_pagination(builder, page: int, page_size: int = 10):
    """Add pagination to a pipeline."""
    return builder.copy().skip(page * page_size).limit(page_size)

def add_sorting(builder, sort_field: str, ascending: bool = True):
    """Add sorting to a pipeline."""
    return builder.copy().sort({sort_field: 1 if ascending else -1})

# Compose pipelines functionally
base = PipelineBuilder().match({"status": "active"})

# Create different variants
page1 = add_pagination(add_sorting(base, "createdAt"), page=0)
page2 = add_pagination(add_sorting(base, "createdAt"), page=1)
sorted_by_name = add_sorting(base, "name", ascending=True)

# All variants are independent
assert len(base) == 1  # Base unchanged
assert len(page1) == 3  # match + sort + skip + limit
```

#### Example: Caching Base Pipelines

```python
from functools import lru_cache

@lru_cache(maxsize=100)
def get_base_pipeline(user_id: str):
    """Cache base pipeline for a user."""
    return (
        PipelineBuilder()
        .match({"userId": user_id, "status": "active"})
        .lookup(
            from_collection="profiles",
            local_field="userId",
            foreign_field="_id",
            as_field="profile"
        )
    )

# Reuse cached base pipeline with different modifications
user_id = "12345"
base = get_base_pipeline(user_id)

# Create multiple queries from cached base
recent = base.copy().sort({"createdAt": -1}).limit(10).build()
by_category = base.copy().match({"category": "tech"}).build()
with_stats = base.copy().group("$category", {"count": {"$sum": 1}}).build()

# Base pipeline is safely cached and reused
```

## Best Practices

### Array `_id` after `$group`: prefer `$arrayElemAt` and materialize fields

If you use `$group` with an array `_id` (e.g. `["_idSeason", "_idTournament"]`), avoid relying on `$_id` later in the pipeline.
Instead, **extract elements with `$arrayElemAt` and store them into explicit fields**, then use those fields in subsequent stages.

```python
pipeline = (
    PipelineBuilder()
    .group(
        group_by=["$idSeason", "$idTournament"],
        accumulators={"idTeams": {"$addToSet": "$idTeam"}},
    )
    .project({
        "idSeason": {"$arrayElemAt": ["$_id", 0]},
        "idTournament": {"$arrayElemAt": ["$_id", 1]},
        "idTeams": 1,
        # Optional: preserve array _id explicitly if you really need it later
        # "_id": "$_id",
    })
    .build()
)
```

This pattern reduces surprises and helps avoid errors like:
`$first's argument must be an array, but is object`.

#### Example: Pipeline Factories

```python
class PipelineFactory:
    """Factory for creating common pipeline patterns."""
    
    @staticmethod
    def base_article_pipeline():
        """Base pipeline for articles."""
        return (
            PipelineBuilder()
            .match({"status": "published"})
            .lookup(
                from_collection="authors",
                local_field="authorId",
                foreign_field="_id",
                as_field="author"
            )
        )
    
    @staticmethod
    def with_author_filter(builder, author_name: str):
        """Add author filter to pipeline."""
        return builder.copy().match({"author.name": author_name})
    
    @staticmethod
    def with_date_range(builder, start_date: str, end_date: str):
        """Add date range filter to pipeline."""
        return builder.copy().match({
            "publishedAt": {"$gte": start_date, "$lte": end_date}
        })

# Usage
base = PipelineFactory.base_article_pipeline()
johns_articles = PipelineFactory.with_author_filter(base, "John Doe")
recent_johns = PipelineFactory.with_date_range(
    johns_articles, 
    start_date="2024-01-01",
    end_date="2024-12-31"
).sort({"publishedAt": -1}).limit(10).build()
```

**Key Benefits:**
- Safe reuse: Base pipelines remain unchanged
- Functional composition: Build pipelines from smaller parts
- Caching friendly: Base pipelines can be safely cached
- No side effects: Functions can safely modify copies
- Thread-safe: Multiple threads can use copies independently

## Development

### Project Structure

```
mongo-pipebuilder/
├── src/
│   └── mongo_pipebuilder/
│       ├── __init__.py
│       └── builder.py
├── tests/
│   └── test_builder.py
├── examples/
│   └── examples.py
├── pyproject.toml
├── README.md
└── LICENSE
```

### Running Tests

```bash
pytest tests/
```

### Contributing

See [DEVELOPMENT.md](DEVELOPMENT.md) for development guidelines.

## License

MIT License - see [LICENSE](LICENSE) file for details.

















