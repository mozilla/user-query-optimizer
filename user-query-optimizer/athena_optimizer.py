import sys
sys.path.append('../user-query-optimizer/optimizations')
import approximates
import column_selection
import partitions
import nested_subqueries

class AthenaOptimizer:
    def __init__(self, optimizations, schema):
        self.optimizations = optimizations
        self.schema = schema

    def _checkApproximates(self, parsed_queries):
        approximates.checkApproximates(self.optimizations, parsed_queries, "approx_distinct")

    def _checkColumnSelection(self, parsed_queries):
        column_selection.checkColumnSelection(self.optimizations, parsed_queries)

    def _checkPartitions(self, parsed_queries):
        partitions.checkPartitions(self.optimizations, self.schema, parsed_queries)

    def _extractNestedSubqueries(self, parsed_queries):
        nested_subqueries.extractNestedSubqueries(self.optimizations, parsed_queries)
