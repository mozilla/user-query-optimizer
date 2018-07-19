from collections import defaultdict
import re
import sqlparse

class Optimizer:
    def __init__(self, query, schema):
        # This probably doesn't need to be stored like this? Will keep for now
        self.query = query
        self.schema = schema
        self.optimizationRef = dict([
            (1, "Check Approximates"),
            (2, "Check Column Selection"),
            (3, "Check Partitions")
        ])

    # Input: query, schema; Output: optimization recommendations
    def optimize_query(self):
        # Format Query
        formatted_query = sqlparse.format(
                            self.query,
                            reindent = True,
                            keyword_case = 'upper'
                        )

        # Switch from unicode to utf-8 encoding
        formatted_query = formatted_query.encode('utf-8')

        # Split so optimizations can be made with reference
        #to specific lines in query
        lines = formatted_query.splitlines()

        # Run all optimization checks
        optimizations = self.__runOptimizationChecks(lines)

        # Print lines with line numbers
        for ind, l in enumerate(lines):
            print(str(ind + 1) + " " + l)

        # Print optimizations for each line
        print(optimizations)


    def __runOptimizationChecks(self, lines):
        # Dictionary: line -> list of optimizations
        optimizations = defaultdict(list)

        # Run tests
        self.__checkApproximates(lines, optimizations)
        self.__checkColumnSelection(lines, optimizations)
        self.__checkPartitions(lines, optimizations)

        return optimizations

    # Optimization #1
    #   Suggest using approximate algorithms (e.g. approx_distinct()
    #   instead of COUNT(DISTINCT ...))
    def __checkApproximates(self, lines, optimizations):
        pass

    # Optimization # 2
    #   Suggest selecting the columns the user wants explicitly,
    #   rather than using (SELECT *)
    def __checkColumnSelection(self, lines, optimizations):
        for ind, l in enumerate(lines):
            if re.search("SELECT \*", l) is not None:
                optimizations[ind] += [2]

    # Optimization # 3
    #    Suggest filtering on partitioned columns
    def __checkPartitions(self, lines, optimizations):
        pass
