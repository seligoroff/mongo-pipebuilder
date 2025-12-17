"""
mongo-pipebuilder: Type-safe, fluent MongoDB aggregation pipeline builder.

This package provides a Builder Pattern implementation for constructing
MongoDB aggregation pipelines safely and readably.

Author: seligoroff
"""

from mongo_pipebuilder.builder import PipelineBuilder

__version__ = "0.3.1"
__all__ = ["PipelineBuilder"]

