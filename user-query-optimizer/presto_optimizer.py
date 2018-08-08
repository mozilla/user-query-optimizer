import sys
sys.path.append('../user-query-optimizer/optimizations')
import approximates
import column_selection

class PrestoOptimizer:
    def __init__(self, optimizations, schema):
        self.optimizations = optimizations

    def _checkApproximates(self, parsed_queries):
        approximates.checkApproximates(self.optimizations, parsed_queries)

    def _checkColumnSelection(self, parsed_queries):
        column_selection.checkColumnSelection(self.optimizations, parsed_queries)
