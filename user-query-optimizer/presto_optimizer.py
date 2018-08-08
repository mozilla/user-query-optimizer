import sys
sys.path.append('../user-query-optimizer/optimizations')
import approximates

class PrestoOptimizer:
    def __init__(self, optimizations, schema):
        self.optimizations = optimizations

    def _checkApproximates(self, parsed_queries):
        approximates.checkApproximates(self.optimizations, parsed_queries, "COUNT\s*\(\s*DISTINCT")
