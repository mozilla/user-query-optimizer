import approximates
import column_selection
import partitions
import nested_subqueries
from optimizer import Optimizer
from collections import defaultdict


class AthenaOptimizer(Optimizer):
    def __init__(self, schema):
        Optimizer.__init__(self, schema)
        self.optimizations = defaultdict(list)

    def _checkApproximates(self, parsed_queries):
        approximates.checkApproximates(self.optimizations, parsed_queries, "approx_distinct")

    def _checkColumnSelection(self, parsed_queries):
        column_selection.checkColumnSelection(self.optimizations, parsed_queries)

    def _checkPartitions(self, parsed_queries):
        partitions.checkPartitions(self.optimizations, self.schema, parsed_queries)

    def _checkOrdering(self, parsed_queries):
        parquet_ordering.checkOrdering(self.optimizations, self.schema, parsed_queries)

    def _extractNestedSubqueries(self, parsed_queries):
        nested_subqueries.extractNestedSubqueries(self.optimizations, parsed_queries)

    def _checkFiltering(self, parsed_queries):
        filtering_transforms.checkFiltering(self.optimizations, self.schema, parsed_queries)
