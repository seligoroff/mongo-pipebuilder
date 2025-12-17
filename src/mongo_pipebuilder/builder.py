"""
Pipeline Builder for MongoDB aggregation pipelines.

Builder Pattern implementation for safe construction of MongoDB aggregation pipelines


Author: seligoroff
"""
import copy
import difflib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# For compatibility with Python < 3.11
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class PipelineBuilder:
    """Builder for MongoDB aggregation pipelines with fluent interface."""

    def __init__(self) -> None:
        """Initialize a new builder with an empty pipeline."""
        self._stages: List[Dict[str, Any]] = []

    def match(self, conditions: Dict[str, Any]) -> Self:
        """
        Add a $match stage for filtering documents.
        
        Args:
            conditions: Dictionary with filtering conditions
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If conditions is None or not a dictionary
            
        Example:
            >>> builder.match({"status": "active", "age": {"$gte": 18}})
        """
        if conditions is None:
            raise TypeError("conditions cannot be None, use empty dict {} instead")
        if not isinstance(conditions, dict):
            raise TypeError(f"conditions must be a dict, got {type(conditions)}")
        if conditions:
            self._stages.append({"$match": conditions})
        return self

    def lookup(
        self,
        from_collection: str,
        local_field: str,
        foreign_field: str,
        as_field: str,
        pipeline: Optional[List[Dict[str, Any]]] = None,
    ) -> Self:
        """
        Add a $lookup stage for joining with another collection.
        
        Args:
            from_collection: Name of the collection to join with
            local_field: Field in the current collection
            foreign_field: Field in the target collection
            as_field: Name of the field for join results
            pipeline: Optional nested pipeline for filtering
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If pipeline is not None and not a list, or if string fields are not strings
            ValueError: If required string fields are empty
            
        Example:
            >>> builder.lookup(
            ...     from_collection="users",
            ...     local_field="userId",
            ...     foreign_field="_id",
            ...     as_field="user"
            ... )
        """
        # Validate string parameters
        if not isinstance(from_collection, str) or not from_collection:
            raise ValueError("from_collection must be a non-empty string")
        if not isinstance(local_field, str) or not local_field:
            raise ValueError("local_field must be a non-empty string")
        if not isinstance(foreign_field, str) or not foreign_field:
            raise ValueError("foreign_field must be a non-empty string")
        if not isinstance(as_field, str) or not as_field:
            raise ValueError("as_field must be a non-empty string")
        
        # Validate pipeline
        if pipeline is not None:
            if not isinstance(pipeline, list):
                raise TypeError(f"pipeline must be a list, got {type(pipeline)}")
            if not all(isinstance(stage, dict) for stage in pipeline):
                raise TypeError("All pipeline stages must be dictionaries")
        
        lookup_stage: Dict[str, Any] = {
            "from": from_collection,
            "localField": local_field,
            "foreignField": foreign_field,
            "as": as_field,
        }
        if pipeline:
            lookup_stage["pipeline"] = pipeline
        self._stages.append({"$lookup": lookup_stage})
        return self

    def add_fields(self, fields: Dict[str, Any]) -> Self:
        """
        Add an $addFields stage for adding or modifying fields.
        
        Args:
            fields: Dictionary with new fields and their expressions
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If fields is not a dictionary
            
        Example:
            >>> builder.add_fields({
            ...     "fullName": {"$concat": ["$firstName", " ", "$lastName"]}
            ... })
        """
        if fields is None:
            raise TypeError("fields cannot be None, use empty dict {} instead")
        if not isinstance(fields, dict):
            raise TypeError(f"fields must be a dict, got {type(fields)}")
        if fields:
            self._stages.append({"$addFields": fields})
        return self

    def project(self, fields: Dict[str, Any]) -> Self:
        """
        Add a $project stage for reshaping documents.
        
        Args:
            fields: Dictionary with fields to include/exclude or transform
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If fields is not a dictionary
            
        Example:
            >>> builder.project({"name": 1, "email": 1, "_id": 0})
        """
        if fields is None:
            raise TypeError("fields cannot be None, use empty dict {} instead")
        if not isinstance(fields, dict):
            raise TypeError(f"fields must be a dict, got {type(fields)}")
        if fields:
            self._stages.append({"$project": fields})
        return self

    def group(self, group_by: Union[str, Dict[str, Any], Any], accumulators: Dict[str, Any]) -> Self:
        """
        Add a $group stage for grouping documents.
        
        Args:
            group_by: Expression for grouping (becomes _id). Can be:
                     - A string (field path, e.g., "$category")
                     - A dict (composite key, e.g., {"category": "$category"})
                     - Any other value (null, number, etc.)
            accumulators: Dictionary with accumulators (sum, avg, count, etc.)
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If accumulators is not a dictionary
            ValueError: If both group_by and accumulators are empty (when group_by is dict/str)
            
        Example:
            >>> builder.group(
            ...     group_by="$category",  # String field path
            ...     accumulators={"total": {"$sum": "$amount"}}
            ... )
            >>> builder.group(
            ...     group_by={"category": "$category"},  # Composite key
            ...     accumulators={"total": {"$sum": "$amount"}}
            ... )
        """
        if not isinstance(accumulators, dict):
            raise TypeError(f"accumulators must be a dict, got {type(accumulators)}")

        # Guard against a common mistake: passing {"_id": ...} as group_by.
        # group_by should be the expression that becomes the $group _id.
        # If users pass {"_id": expr}, MongoDB will create nested _id and later
        # expressions like $first: "$_id" may fail because $_id becomes an object.
        if isinstance(group_by, dict) and set(group_by.keys()) == {"_id"}:
            inner = group_by["_id"]
            raise ValueError(
                "Invalid group_by: you passed a dict wrapper {'_id': ...} to PipelineBuilder.group().\n"
                "PipelineBuilder.group(group_by=...) expects the expression that becomes $group._id.\n"
                "\n"
                "Did you mean one of these?\n"
                f"- builder.group(group_by={inner!r}, accumulators=...)\n"
                f"- builder.group(group_by={inner!r}, accumulators={{...}})  # same, explicit\n"
                "\n"
                "Examples:\n"
                "- Array _id: builder.group(group_by=['$idSeason', '$idTournament'], accumulators={...})\n"
                "- Field path: builder.group(group_by='$category', accumulators={...})\n"
                "- Composite key: builder.group(group_by={'category': '$category'}, accumulators={...})\n"
                "\n"
                "Why this matters: {'_id': expr} would create a nested _id object in MongoDB, and later\n"
                "operators like $first/$last on '$_id' may fail with: \"$first's argument must be an array, but is object\"."
            )
        
        # Validate empty cases
        # group_by can be None, empty string, empty dict, etc. - all are valid in MongoDB
        # But if it's a string and empty, or dict and empty, and accumulators is also empty,
        # it's likely an error
        if isinstance(group_by, dict):
            if not group_by and not accumulators:
                raise ValueError("group_by and accumulators cannot both be empty")
        elif isinstance(group_by, str):
            if not group_by and not accumulators:
                raise ValueError("group_by and accumulators cannot both be empty")
        
        group_stage = {"_id": group_by, **accumulators}
        self._stages.append({"$group": group_stage})
        return self

    def unwind(
        self,
        path: str,
        preserve_null_and_empty_arrays: bool = False,
        include_array_index: Optional[str] = None,
    ) -> Self:
        """
        Add an $unwind stage for unwinding arrays.
        
        Args:
            path: Path to the array field
            preserve_null_and_empty_arrays: Preserve documents with null/empty arrays
            include_array_index: Name of the field for array element index
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If path is not a string
            ValueError: If path is empty
            
        Example:
            >>> builder.unwind("tags", preserve_null_and_empty_arrays=True)
            >>> builder.unwind("items", include_array_index="itemIndex")
        """
        if not isinstance(path, str):
            raise TypeError(f"path must be a string, got {type(path)}")
        if not path:
            raise ValueError("path cannot be empty")
        
        unwind_stage: Dict[str, Any] = {"path": path}
        if preserve_null_and_empty_arrays:
            unwind_stage["preserveNullAndEmptyArrays"] = True
        if include_array_index:
            unwind_stage["includeArrayIndex"] = include_array_index
        self._stages.append({"$unwind": unwind_stage})
        return self

    def sort(self, fields: Dict[str, int]) -> Self:
        """
        Add a $sort stage for sorting documents.
        
        Args:
            fields: Dictionary with fields and sort direction (1 - asc, -1 - desc)
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If fields is not a dictionary
            
        Example:
            >>> builder.sort({"createdAt": -1, "name": 1})
        """
        if fields is None:
            raise TypeError("fields cannot be None, use empty dict {} instead")
        if not isinstance(fields, dict):
            raise TypeError(f"fields must be a dict, got {type(fields)}")
        if fields:
            self._stages.append({"$sort": fields})
        return self

    def limit(self, limit: int) -> Self:
        """
        Add a $limit stage to limit the number of documents.
        
        Args:
            limit: Maximum number of documents
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If limit is not an integer
            ValueError: If limit is negative
            
        Example:
            >>> builder.limit(10)
        """
        if not isinstance(limit, int):
            raise TypeError(f"limit must be an integer, got {type(limit)}")
        if limit < 0:
            raise ValueError("limit cannot be negative")
        if limit > 0:
            self._stages.append({"$limit": limit})
        return self

    def skip(self, skip: int) -> Self:
        """
        Add a $skip stage to skip documents.
        
        Args:
            skip: Number of documents to skip
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If skip is not an integer
            ValueError: If skip is negative
            
        Example:
            >>> builder.skip(20)
        """
        if not isinstance(skip, int):
            raise TypeError(f"skip must be an integer, got {type(skip)}")
        if skip < 0:
            raise ValueError("skip cannot be negative")
        if skip > 0:
            self._stages.append({"$skip": skip})
        return self

    def unset(self, fields: Union[str, List[str]]) -> Self:
        """
        Add a $unset stage to remove fields from documents.
        
        Args:
            fields: Field name or list of field names to remove
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If fields is not a string or list of strings
            ValueError: If fields is empty
            
        Example:
            >>> builder.unset("temp_field")
            >>> builder.unset(["field1", "field2", "field3"])
        """
        if fields is None:
            raise TypeError("fields cannot be None")
        
        if isinstance(fields, str):
            if not fields:
                raise ValueError("fields cannot be an empty string")
            self._stages.append({"$unset": fields})
        elif isinstance(fields, list):
            if not fields:
                raise ValueError("fields cannot be an empty list")
            if not all(isinstance(f, str) for f in fields):
                raise TypeError("all items in fields list must be strings")
            if not all(f for f in fields):  # Check for empty strings
                raise ValueError("fields list cannot contain empty strings")
            # MongoDB accepts list for multiple fields, or string for single field
            self._stages.append({"$unset": fields if len(fields) > 1 else fields[0]})
        else:
            raise TypeError(f"fields must be a string or list of strings, got {type(fields)}")
        
        return self

    def replace_root(self, new_root: Dict[str, Any]) -> Self:
        """
        Add a $replaceRoot stage to replace the root document.
        
        Args:
            new_root: Expression for the new root document (must contain 'newRoot' key)
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If new_root is not a dictionary
            ValueError: If new_root is empty or missing 'newRoot' key
            
        Example:
            >>> builder.replace_root({"newRoot": "$embedded"})
            >>> builder.replace_root({"newRoot": {"$mergeObjects": ["$doc1", "$doc2"]}})
        """
        if new_root is None:
            raise TypeError("new_root cannot be None, use empty dict {} instead")
        if not isinstance(new_root, dict):
            raise TypeError(f"new_root must be a dict, got {type(new_root)}")
        if not new_root:
            raise ValueError("new_root cannot be empty")
        if "newRoot" not in new_root:
            raise ValueError("new_root must contain 'newRoot' key")
        
        self._stages.append({"$replaceRoot": new_root})
        return self

    def replace_with(self, replacement: Any) -> Self:
        """
        Add a $replaceWith stage (alias for $replaceRoot in MongoDB 4.2+).
        
        Args:
            replacement: Expression for the replacement document
            
        Returns:
            Self for method chaining
            
        Raises:
            ValueError: If replacement is None
            
        Example:
            >>> builder.replace_with("$embedded")
            >>> builder.replace_with({"$mergeObjects": ["$doc1", "$doc2"]})
        """
        if replacement is None:
            raise ValueError("replacement cannot be None")
        
        self._stages.append({"$replaceWith": replacement})
        return self

    def facet(self, facets: Dict[str, List[Dict[str, Any]]]) -> Self:
        """
        Add a $facet stage for parallel execution of multiple sub-pipelines.
        
        Args:
            facets: Dictionary where keys are output field names and values are
                   lists of pipeline stages for each sub-pipeline
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If facets is not a dictionary
            ValueError: If facets is empty or contains invalid values
            
        Example:
            >>> builder.facet({
            ...     "items": [{"$skip": 10}, {"$limit": 20}],
            ...     "meta": [{"$count": "total"}]
            ... })
        """
        if facets is None:
            raise TypeError("facets cannot be None, use empty dict {} instead")
        if not isinstance(facets, dict):
            raise TypeError(f"facets must be a dict, got {type(facets)}")
        if not facets:
            raise ValueError("facets cannot be empty")
        
        # Validate that all values are lists of dictionaries
        for key, value in facets.items():
            if not isinstance(value, list):
                raise TypeError(f"facet '{key}' must be a list, got {type(value)}")
            if not all(isinstance(stage, dict) for stage in value):
                raise TypeError(f"all stages in facet '{key}' must be dictionaries")
        
        self._stages.append({"$facet": facets})
        return self

    def count(self, field_name: str = "count") -> Self:
        """
        Add a $count stage to count documents.
        
        Args:
            field_name: Name of the field for the count result
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If field_name is not a string
            ValueError: If field_name is empty
            
        Example:
            >>> builder.match({"status": "active"}).count("active_count")
        """
        if field_name is None:
            raise TypeError("field_name cannot be None")
        if not isinstance(field_name, str):
            raise TypeError(f"field_name must be a string, got {type(field_name)}")
        if not field_name:
            raise ValueError("field_name cannot be empty")
        
        self._stages.append({"$count": field_name})
        return self

    def set_field(self, fields: Dict[str, Any]) -> Self:
        """
        Add a $set stage (alias for $addFields in MongoDB 3.4+).
        
        Functionally equivalent to add_fields(), but $set is a more intuitive alias.
        
        Args:
            fields: Dictionary with fields and their values/expressions
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If fields is not a dictionary
            ValueError: If fields is empty
            
        Example:
            >>> builder.set_field({"status": "active", "updatedAt": "$$NOW"})
        """
        if fields is None:
            raise TypeError("fields cannot be None, use empty dict {} instead")
        if not isinstance(fields, dict):
            raise TypeError(f"fields must be a dict, got {type(fields)}")
        if not fields:
            # Empty dict - valid case, skip (same as add_fields behavior)
            return self
        
        self._stages.append({"$set": fields})
        return self

    def add_stage(self, stage: Dict[str, Any]) -> Self:
        """
        Add an arbitrary pipeline stage for advanced use cases.
        
        Args:
            stage: Dictionary with an arbitrary MongoDB aggregation stage
            
        Returns:
            Self for method chaining
            
        Example:
            >>> builder.add_stage({
            ...     "$facet": {
            ...         "categories": [{"$group": {"_id": "$category"}}],
            ...         "total": [{"$count": "count"}]
            ...     }
            ... })
        """
        if stage:
            self._stages.append(stage)
        return self

    def __len__(self) -> int:
        """
        Return the number of stages in the pipeline.
        
        Returns:
            Number of stages
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"}).limit(10)
            >>> len(builder)
            2
        """
        return len(self._stages)

    def __repr__(self) -> str:
        """
        Return a string representation of the builder for debugging.
        
        Returns:
            String representation showing stage count and preview
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"}).limit(10)
            >>> repr(builder)
            'PipelineBuilder(stages=2, preview=[$match, $limit])'
        """
        stages_count = len(self._stages)
        if stages_count == 0:
            return "PipelineBuilder(stages=0)"
        
        stage_types = [list(stage.keys())[0] for stage in self._stages[:3]]
        stages_preview = ", ".join(stage_types)
        if stages_count > 3:
            stages_preview += "..."
        return f"PipelineBuilder(stages={stages_count}, preview=[{stages_preview}])"

    def clear(self) -> Self:
        """
        Clear all stages from the pipeline.
        
        Returns:
            Self for method chaining
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"}).clear()
            >>> len(builder)
            0
        """
        self._stages.clear()
        return self

    def copy(self) -> "PipelineBuilder":
        """
        Create a copy of the builder with current stages.
        
        Returns:
            New PipelineBuilder instance with copied stages
            
        Example:
            >>> builder1 = PipelineBuilder().match({"status": "active"})
            >>> builder2 = builder1.copy()
            >>> builder2.limit(10)
            >>> len(builder1)  # Original unchanged
            1
            >>> len(builder2)  # Copy has new stage
            2
        """
        new_builder = PipelineBuilder()
        new_builder._stages = self._stages.copy()
        return new_builder

    def validate(self) -> bool:
        """
        Validate the pipeline before execution.
        
        Checks that the pipeline is not empty and has valid structure.
        Validates critical MongoDB rules:
        - $out and $merge stages must be the last stage in the pipeline
        
        Returns:
            True if pipeline is valid
            
        Raises:
            ValueError: If pipeline is empty or has validation errors
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"}).validate()
            True
            >>> PipelineBuilder().validate()
            ValueError: Pipeline cannot be empty
        """
        if not self._stages:
            raise ValueError("Pipeline cannot be empty")
        
        # Validate that $out and $merge are the last stages (critical MongoDB rule)
        stage_types = self.get_stage_types()
        
        # Check if $out or $merge exist
        has_out = "$out" in stage_types
        has_merge = "$merge" in stage_types
        
        if has_out and has_merge:
            raise ValueError(
                "Pipeline cannot contain both $out and $merge stages. "
                "Only one output stage is allowed."
            )
        
        # Check if $out or $merge exist and validate position
        for stage_name in ["$out", "$merge"]:
            if stage_name in stage_types:
                stage_index = stage_types.index(stage_name)
                if stage_index != len(stage_types) - 1:
                    raise ValueError(
                        f"{stage_name} stage must be the last stage in the pipeline. "
                        f"Found at position {stage_index + 1} of {len(stage_types)}."
                    )
        
        return True

    def get_stage_types(self) -> List[str]:
        """
        Get a list of stage types in the pipeline.
        
        Returns:
            List of stage type strings (e.g., ["$match", "$lookup", "$limit"])
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"}).limit(10)
            >>> builder.get_stage_types()
            ['$match', '$limit']
        """
        return [next(iter(stage)) for stage in self._stages]

    def has_stage(self, stage_type: str) -> bool:
        """
        Check if the pipeline contains a specific stage type.
        
        Args:
            stage_type: Type of stage to check (e.g., "$match", "$lookup")
            
        Returns:
            True if the stage type is present in the pipeline
            
        Raises:
            TypeError: If stage_type is not a string
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"}).limit(10)
            >>> builder.has_stage("$match")
            True
            >>> builder.has_stage("$group")
            False
        """
        if not isinstance(stage_type, str):
            raise TypeError(f"stage_type must be a string, got {type(stage_type)}")
        # Check if stage_type is a key in any stage dictionary
        return any(stage_type in stage for stage in self._stages)

    def prepend(self, stage: Dict[str, Any]) -> Self:
        """
        Add a stage at the beginning of the pipeline.
        
        Args:
            stage: Dictionary with a MongoDB aggregation stage
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If stage is not a dictionary
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"})
            >>> builder.prepend({"$match": {"deleted": False}})
            >>> builder.build()
            [{"$match": {"deleted": False}}, {"$match": {"status": "active"}}]
        """
        if stage is None:
            raise TypeError("stage cannot be None")
        if not isinstance(stage, dict):
            raise TypeError(f"stage must be a dict, got {type(stage)}")
        if stage:
            self._stages.insert(0, stage)
        return self

    def insert_at(self, position: int, stage: Dict[str, Any]) -> Self:
        """
        Insert a stage at a specific position in the pipeline.
        
        Args:
            position: Index where to insert (0-based)
            stage: Dictionary with a MongoDB aggregation stage to insert
            
        Returns:
            Self for method chaining
            
        Raises:
            TypeError: If stage is not a dictionary
            IndexError: If position is out of range [0, len(stages)]
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"}).group({"_id": "$category"}, {})
            >>> builder.insert_at(1, {"$sort": {"name": 1}})
            >>> builder.build()
            [{"$match": {"status": "active"}}, {"$sort": {"name": 1}}, {"$group": {...}}]
        """
        if stage is None:
            raise TypeError("stage cannot be None")
        if not isinstance(stage, dict):
            raise TypeError(f"stage must be a dict, got {type(stage)}")
        if not stage:
            return self
        
        if position < 0 or position > len(self._stages):
            raise IndexError(
                f"Position {position} out of range [0, {len(self._stages)}]"
            )
        
        self._stages.insert(position, stage)
        return self

    def get_stage_at(self, index: int) -> Dict[str, Any]:
        """
        Get a specific stage from the pipeline by index.
        
        Args:
            index: Zero-based index of the stage to retrieve
            
        Returns:
            Dictionary representing the stage at the given index
            
        Raises:
            IndexError: If index is out of range
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"}).limit(10)
            >>> stage = builder.get_stage_at(0)
            >>> stage
            {"$match": {"status": "active"}}
        """
        if index < 0 or index >= len(self._stages):
            raise IndexError(
                f"Index {index} out of range [0, {len(self._stages)}]"
            )
        # Return a deep copy so callers can safely mutate nested structures
        return copy.deepcopy(self._stages[index])

    def pretty_print(self, indent: int = 2, ensure_ascii: bool = False) -> str:
        """
        Return a formatted JSON string representation of the pipeline.
        
        Useful for debugging and understanding pipeline structure.
        
        Args:
            indent: Number of spaces for indentation (default: 2)
            ensure_ascii: If False, non-ASCII characters are output as-is (default: False)
            
        Returns:
            Formatted JSON string of the pipeline
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"}).limit(10)
            >>> print(builder.pretty_print())
            [
              {
                "$match": {
                  "status": "active"
                }
              },
              {
                "$limit": 10
              }
            ]
        """
        return json.dumps(self._stages, indent=indent, ensure_ascii=ensure_ascii)

    def pretty_print_stage(
        self,
        stage: Union[int, Dict[str, Any]],
        indent: int = 2,
        ensure_ascii: bool = False,
    ) -> str:
        """
        Return a formatted JSON string representation of a single stage.

        Args:
            stage: Stage index (0-based) or a stage dict
            indent: Number of spaces for indentation (default: 2)
            ensure_ascii: If False, non-ASCII characters are output as-is (default: False)

        Returns:
            Formatted JSON string of the stage

        Raises:
            TypeError: If stage is not an int or dict
            IndexError: If stage is an int out of range
        """
        if isinstance(stage, int):
            stage_dict = self.get_stage_at(stage)
        elif isinstance(stage, dict):
            stage_dict = copy.deepcopy(stage)
        else:
            raise TypeError(f"stage must be an int index or a dict, got {type(stage)}")

        return json.dumps(stage_dict, indent=indent, ensure_ascii=ensure_ascii)

    def to_json_file(
        self,
        filepath: Union[str, Path],
        indent: int = 2,
        ensure_ascii: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Save the pipeline to a JSON file.
        
        Useful for debugging, comparison with other pipelines, or versioning.
        
        Args:
            filepath: Path to the output JSON file (str or Path)
            indent: Number of spaces for indentation (default: 2)
            ensure_ascii: If False, non-ASCII characters are output as-is (default: False)
            metadata: Optional metadata to include in the JSON file
            
        Raises:
            IOError: If file cannot be written
            
        Example:
            >>> builder = PipelineBuilder()
            >>> builder.match({"status": "active"}).limit(10)
            >>> builder.to_json_file("debug_pipeline.json")
            
            >>> # With metadata
            >>> builder.to_json_file(
            ...     "pipeline.json",
            ...     metadata={"version": "1.0", "author": "developer"}
            ... )
        """
        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        output: Dict[str, Any] = {
            "pipeline": self._stages,
        }
        if metadata:
            output["metadata"] = metadata
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=indent, ensure_ascii=ensure_ascii)

    def compare_with(self, other: "PipelineBuilder", context_lines: int = 3) -> str:
        """
        Compare this pipeline with another pipeline and return a unified diff.
        
        This is useful when migrating legacy pipelines (e.g., templates) to builder code.
        
        Args:
            other: Another PipelineBuilder instance to compare with
            context_lines: Number of context lines in the unified diff (default: 3)
        
        Returns:
            Unified diff as a string. Returns "No differences." if pipelines are identical.
        
        Raises:
            TypeError: If other is not a PipelineBuilder
            ValueError: If context_lines is negative
        
        Example:
            >>> legacy = PipelineBuilder().match({"a": 1})
            >>> new = PipelineBuilder().match({"a": 2})
            >>> print(new.compare_with(legacy))
        """
        if not isinstance(other, PipelineBuilder):
            raise TypeError(f"other must be a PipelineBuilder, got {type(other)}")
        if not isinstance(context_lines, int):
            raise TypeError(f"context_lines must be an int, got {type(context_lines)}")
        if context_lines < 0:
            raise ValueError("context_lines cannot be negative")

        a = json.dumps(
            self.build(),
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ).splitlines(keepends=True)
        b = json.dumps(
            other.build(),
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ).splitlines(keepends=True)

        diff = difflib.unified_diff(a, b, fromfile="new", tofile="other", n=context_lines)
        out = "".join(diff)
        return out if out else "No differences."

    def build(self) -> List[Dict[str, Any]]:
        """
        Return the completed pipeline.
        
        Returns:
            List of dictionaries with aggregation pipeline stages
            
        Example:
            >>> pipeline = builder.build()
            >>> collection.aggregate(pipeline)
        """
        return self._stages.copy()

