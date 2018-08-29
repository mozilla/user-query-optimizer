import re
import sqlparse
from collections import defaultdict
from collections import OrderedDict
from clickhouse_cli.ui.parseutils.ctes import extract_ctes
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Newline, CTE, Wildcard

class Optimizer:
    def __init__(self, schema):
        self.schema = schema

    # Main public function to optimize a query
    def optimize_query(self, query):
        # Parse queries and extract CTEs
        # Strip comments to help sqlparse correctly extract the identifier list
        formatted_query = str(sqlparse.format(query, strip_comments = True)).strip()
        parsed_queries = self._parse_query(formatted_query)

        # Run all optimization checks
        self._runOptimizationChecks(parsed_queries)

        # Find subquery in original query again, and adjust line numbers
        adjusted_opts = self._adjust_linenums(formatted_query)

        # Print query
        self._print_query_lines(formatted_query)

        # Print optimizations
        self._print_optimizations(adjusted_opts)

        return adjusted_opts

    # Run each optimiztion check on parsed queries and update optimizations dictionary
    # Input: parsed_queries
    # Output: None
    def _runOptimizationChecks(self, parsed_queries):
        # Run tests
        self._checkApproximates(parsed_queries)
        self._checkColumnSelection(parsed_queries)
        self._checkPartitions(parsed_queries)
        self._extractNestedSubqueries(parsed_queries)


    # Parse query using sqlparse and extract CTEs
    # Input: Original query formatted
    # Output: List of statements - each is a parsed query for each CTE in each query in the original
    def _parse_query(self, formatted_query):
        parsed = sqlparse.parse(formatted_query)
        parsed_queries = []
        for query in parsed:
            query = str(query)
            ctes = extract_ctes(query)
            cte_list, remainder_query = ctes[0], ctes[1]
            for cte in cte_list:
                cte_str = query[cte.start + 1:cte.stop - 1]
                parsed_cte = sqlparse.parse(cte_str.strip())
                parsed_queries.append(parsed_cte)

            parsed_remainder = sqlparse.parse(remainder_query.strip())
            parsed_queries.append(parsed_remainder)
        return parsed_queries

    # Find subquery in original query again, and adjust line numbers
    # Input: original formatted query
    # Returns: dictionary from line numbers in original formatted query -> list of optimizations
    def _adjust_linenums(self, formatted_query):
        adjusted_opts = defaultdict(list)
        for k, v in self.optimizations.iteritems():
            for opt in v:
                match = formatted_query.find(str(k).strip())
                if match != -1:
                    adjusted_lineno = formatted_query[:match].count("\n") + opt[0]
                    adjusted_opts[adjusted_lineno].append(opt[1])
        return adjusted_opts

    # Prints formatted query with line numbers
    # Input: formatted parse_query
    # Output: Print query to console with line numbers
    def _print_query_lines(self, formatted_query):
        lines = formatted_query.split("\n")
        for ind, l in enumerate(lines):
            print(str(ind + 1) + " " + l)
        print("\n")

    # Print optimizations (line numbers in original query, and optimization(s) for line)
    # Input: Optimizations dictionary with line numbers adjusted to original formatted query
    # Output: Print optimizations to console
    def _print_optimizations(self, adjusted_opts):
        print("\nOptimizations")
        if len(adjusted_opts) == 0:
            print("\tNone found")
        else:
            # Print optimizations
            for k, v in OrderedDict(adjusted_opts).iteritems():
                print("\tLine " + str(k + 1) + ": " + ", ".join(v) + "\n")
        print("\n")
