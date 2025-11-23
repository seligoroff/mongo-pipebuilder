# mongo-pipebuilder

Type-safe, fluent MongoDB aggregation pipeline builder for Python.

## Overview

`mongo-pipebuilder` provides a clean, type-safe way to build MongoDB aggregation pipelines using the Builder Pattern with a fluent interface for maximum readability and safety.

## Features

- ✅ **Type-safe**: Full type hints support with IDE autocomplete
- ✅ **Fluent interface**: Chain methods for readable, maintainable code
- ✅ **Zero dependencies**: Pure Python, lightweight package
- ✅ **Extensible**: Easy to add custom stages via `add_stage()`
- ✅ **Well tested**: Comprehensive test suite with 96%+ coverage

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
builder.match({"status": "active"}).group({"_id": "$category"}, {"count": {"$sum": 1}})
builder.insert_at(1, {"$sort": {"name": 1}})
# Pipeline: [{"$match": {...}}, {"$sort": {...}}, {"$group": {...}}]
```

**Note:** For inserting before a specific stage type, combine with `get_stage_types()`:

```python
stage_types = builder.get_stage_types()
group_index = stage_types.index("$group")
builder.insert_at(group_index, {"$addFields": {"x": 1}})
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

