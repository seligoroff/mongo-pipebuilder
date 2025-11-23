"""
Usage examples for mongo-pipebuilder.

These examples demonstrate various ways to use PipelineBuilder
for building MongoDB aggregation pipelines.

Author: seligoroff
"""

from mongo_pipebuilder import PipelineBuilder


def example_simple_query():
    """Simple example: filtering and sorting."""
    pipeline = (
        PipelineBuilder()
        .match({"status": "active", "age": {"$gte": 18}})
        .sort({"name": 1})
        .limit(10)
        .build()
    )
    return pipeline


def example_with_lookup():
    """Example with collection joining."""
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
        .project({
            "title": 1,
            "authorName": "$author.name",
            "authorEmail": "$author.email",
            "_id": 0
        })
        .build()
    )
    return pipeline


def example_with_nested_lookup():
    """Example with nested lookup pipeline."""
    nested_pipeline = [
        {"$match": {"active": True}},
        {"$project": {"name": 1, "slug": 1}}
    ]
    
    pipeline = (
        PipelineBuilder()
        .match({"status": "published"})
        .lookup(
            from_collection="categories",
            local_field="categoryId",
            foreign_field="_id",
            as_field="category",
            pipeline=nested_pipeline
        )
        .unwind("category")
        .build()
    )
    return pipeline


def example_aggregation():
    """Example aggregation with grouping."""
    pipeline = (
        PipelineBuilder()
        .match({"date": {"$gte": "2024-01-01"}})
        .group(
            group_by={"month": {"$dateToString": {"format": "%Y-%m", "date": "$date"}}},
            accumulators={
                "totalSales": {"$sum": "$amount"},
                "avgAmount": {"$avg": "$amount"},
                "count": {"$sum": 1},
                "maxAmount": {"$max": "$amount"},
                "minAmount": {"$min": "$amount"}
            }
        )
        .sort({"month": 1})
        .build()
    )
    return pipeline


def example_complex_pipeline():
    """Complex example with multiple stages."""
    pipeline = (
        PipelineBuilder()
        # Filtering
        .match({
            "status": "published",
            "publishedAt": {"$gte": "2024-01-01"}
        })
        # Join with authors
        .lookup(
            from_collection="authors",
            local_field="authorId",
            foreign_field="_id",
            as_field="author"
        )
        .unwind("author", preserve_null_and_empty_arrays=True)
        # Join with categories
        .lookup(
            from_collection="categories",
            local_field="categoryId",
            foreign_field="_id",
            as_field="category"
        )
        .unwind("category")
        # Add computed fields
        .add_fields({
            "authorName": "$author.name",
            "categoryName": "$category.name",
            "fullTitle": {"$concat": ["$category.name", " - ", "$title"]}
        })
        # Project required fields
        .project({
            "title": 1,
            "authorName": 1,
            "categoryName": 1,
            "fullTitle": 1,
            "publishedAt": 1,
            "_id": 0
        })
        # Sorting
        .sort({"publishedAt": -1, "title": 1})
        # Pagination
        .skip(0)
        .limit(20)
        .build()
    )
    return pipeline


def example_with_custom_stage():
    """Example of using arbitrary stage via add_stage."""
    pipeline = (
        PipelineBuilder()
        .match({"status": "active"})
        .add_stage({
            "$facet": {
                "categories": [
                    {"$group": {"_id": "$category", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ],
                "total": [
                    {"$count": "count"}
                ],
                "topItems": [
                    {"$sort": {"views": -1}},
                    {"$limit": 5}
                ]
            }
        })
        .build()
    )
    return pipeline


def example_pagination():
    """Example of pagination."""
    def get_paginated_pipeline(page: int = 1, page_size: int = 20):
        skip = (page - 1) * page_size
        return (
            PipelineBuilder()
            .match({"status": "active"})
            .sort({"createdAt": -1})
            .skip(skip)
            .limit(page_size)
            .build()
        )
    
    return get_paginated_pipeline(page=2, page_size=10)


def example_with_prepend():
    """Example of using prepend() to add initial filter."""
    pipeline = (
        PipelineBuilder()
        .match({"status": "active"})
        .limit(10)
        .prepend({"$match": {"deleted": False}})  # Add initial filter
        .build()
    )
    # Result: [{"$match": {"deleted": False}}, {"$match": {"status": "active"}}, {"$limit": 10}]
    return pipeline


def example_with_insert_at():
    """Example of using insert_at() to insert stage at specific position."""
    pipeline = (
        PipelineBuilder()
        .match({"status": "active"})
        .group({"_id": "$category"}, {"count": {"$sum": 1}})
        .insert_at(1, {"$sort": {"name": 1}})  # Insert sort before group
        .build()
    )
    # Result: [{"$match": {...}}, {"$sort": {...}}, {"$group": {...}}]
    return pipeline


def example_insert_before_stage_type():
    """Example of inserting before specific stage type using get_stage_types()."""
    builder = PipelineBuilder()
    builder.match({"status": "active"})
    builder.lookup("users", "userId", "_id", "user")
    builder.group({"_id": "$category"}, {"count": {"$sum": 1}})
    
    # Find position of $group and insert $addFields before it
    stage_types = builder.get_stage_types()
    group_index = stage_types.index("$group")
    builder.insert_at(group_index, {"$addFields": {"categoryUpper": {"$toUpper": "$category"}}})
    
    return builder.build()


if __name__ == "__main__":
    # Print examples for demonstration
    import json
    
    print("=== Simple Query ===")
    print(json.dumps(example_simple_query(), indent=2))
    
    print("\n=== With Lookup ===")
    print(json.dumps(example_with_lookup(), indent=2))
    
    print("\n=== Aggregation ===")
    print(json.dumps(example_aggregation(), indent=2))

