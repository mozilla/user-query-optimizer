import re
import sqlparse
from collections import defaultdict
from collections import OrderedDict
from clickhouse_cli.ui.parseutils.ctes import extract_ctes
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword, DML, Newline, CTE, Wildcard


class Optimizer:
    def __init__(self, query, schema):
        self.query = query
        self.schema = schema

    # Input: query, schema; Output: optimization recommendations
    def optimize_query(self):
        # Run all optimization checks
        optimizations = self.__runOptimizationChecks(lines)

        # Print lines with line numbers
        for ind, l in enumerate(lines):
            print(str(ind + 1) + " " + l)

        # Print optimizations for each line in order
        optimizations = OrderedDict(optimizations.items())
        print("\nOptimizations")
        if len(optimizations) == 0:
            print("\tNone found")
        for k, v in optimizations.iteritems():
            print("\tLine " + str(k + 1) + ": " + ", ".join(optimizations[k]) + "\n")

    def __runOptimizationChecks(self, lines):
        # Dictionary: line -> list of optimizations
        optimizations = defaultdict(list)

        # Run tests
        self.__checkApproximates(lines, optimizations)
        self.__checkColumnSelection(lines, optimizations)
        self.__checkPartitions(lines, optimizations)

        return optimizations

    # Optimization #1
    #   Suggest using approximate algorithms (e.g. approx_distinct() instead of COUNT(DISTINCT ...))
    #   Consider other approximations later on (when to use approx_percentile?)
    def __checkApproximates(self, optimizations):
        formatted_query = str(sqlparse.format(self.query, strip_comments = True))
        parsed = sqlparse.parse(formatted_query)
        parsed_queries = []
        for query in parsed:
            query = str(query)
            ctes = extract_ctes(query)
            cte_list, remainder_query = ctes[0], ctes[1]
            for cte in cte_list:
                cte_str = query[cte.start + 1:cte.stop - 1]
                parsed_cte = sqlparse.parse(cte_str)
                parsed_queries += [parsed_cte]

            parsed_remainder = sqlparse.parse(remainder_query)
            parsed_queries += [parsed_remainder]

        optimizations = defaultdict(list)
        for stmt_list in parsed_queries:
            lineno = 0
            for stmt in stmt_list:
                select_seen = False
                for token in stmt.tokens:
                    if token.ttype is Newline:
                        lineno += 1
                    if select_seen:
                        if token.ttype is Keyword and token.value.upper() == "FROM":
                            break
                        else:
                            if isinstance(token, IdentifierList):
                                for identifier in token.get_identifiers():
                                    if re.search("COUNT\s*\(\s*DISTINCT", str(identifier), re.IGNORECASE):
                                        optimizations[stmt] += [(lineno, "use approximation")]
                            elif isinstance(token, Identifier):
                                if re.search("COUNT\s*\(\s*DISTINCT", str(token), re.IGNORECASE):
                                    optimizations[stmt] += [(lineno, "use approximation")]
                    if token.ttype is DML and token.value.upper() == "SELECT":
                        select_seen = True

        # find subquery in original query again, and adjust line numbers
        adjusted_opts = {}
        for k, v in optimizations.iteritems():
            for opt in v:
                match = formatted_query.find(str(k))
                if match != -1:
                    adjusted_lineno = formatted_query[:(match + opt[0])].count("\n")
                    adjusted_opts[adjusted_lineno] = opt[1]

        # Print lines
        lines = formatted_query.split("\n")
        for ind, l in enumerate(lines):
            if l.strip() != "":
                print(str(ind) + " " + l)
        print("\n")

        # Print optimizations
        for k, v in OrderedDict(adjusted_opts).iteritems():
            print("Line number " + str(k) + ": " + v + "\n")
        return optimizations

    # Optimization # 2
    #   Suggest selecting the columns the user wants explicitly, rather than using (SELECT *)
    def __checkColumnSelection(self, lines, optimizations):
        for ind, l in enumerate(lines):
            if re.search("SELECT \*", l, re.IGNORECASE) is not None:
                optimizations[ind] += ["Column Selection"]

    # Optimization # 3
    #    Suggest filtering on partitioned columns
    def __checkPartitions(self, lines, optimizations):
        pass
