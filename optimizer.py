from collections import defaultdict
from collections import OrderedDict
import re
import sqlparse

class Optimizer:
    def __init__(self, query, schema):
        self.query = query
        self.schema = schema

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
        # to specific lines in query
        lines = formatted_query.splitlines()

        # Remove comments
        for ind, l in enumerate(lines):
            lines[ind] = re.sub("(--.*|#.*)", "", l)

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
        self.__checkUnion(lines, optimizations)
        self.__checkAggregatingLikes(lines, optimizations)
        self.__checkSimpleEquijoins(lines, optimizations)
        self.__checkNestedQueries(lines, optimizations)
        self.__checkGroupByExpressions(lines, optimizations)

        return optimizations

    # Optimization #1
    #   Suggest using approximate algorithms (e.g. approx_distinct()
    #   instead of COUNT(DISTINCT ...))
    def __checkApproximates(self, lines, optimizations):
        for ind, l in enumerate(lines):
            if re.search("COUNT\s*\(DISTINCT", l, re.IGNORECASE) is not None:
                optimizations[ind] += ["Use approx_distinct() rather than COUNT(DISTINCT...)"]

    # Optimization # 2
    #   Suggest selecting the columns the user wants explicitly,
    #   rather than using (SELECT *)
    def __checkColumnSelection(self, lines, optimizations):
        for ind, l in enumerate(lines):
            if re.search("SELECT \*", l, re.IGNORECASE) is not None:
                optimizations[ind] += ["Column Selection"]

    # Optimization # 3
    #    Suggest filtering on partitioned columns
    def __checkPartitions(self, lines, optimizations):
        pass

    # Optimization # 4
    #    Replace UNION with UNION ALL if duplicates do not need to be removed
    #    https://docs.treasuredata.com/articles/presto-query-faq#q-query-that-produces-a-huge-result-is-slow
    def __checkUnion(self, lines, optimizations):
        for ind, l in enumerate(lines):
            if re.search("UNION\s*(^((?!ALL).))*$", l, re.IGNORECASE) is not None:
                optimizations[ind] += ["Replace UNION with UNION ALL if duplicates allowed"]

    # Optimization # 5
    #    Aggregate a series of LIKE clauses into one regexp_like expression.
    #    https://docs.treasuredata.com/articles/presto-query-faq
    def __checkAggregatingLikes(self, lines, optimizations):
        count = 0
        like_ind = []
        for ind, l in enumerate(lines):
            if re.search("LIKE", l, re.IGNORECASE) is not None:
                count += 1
                like_ind += [ind]
        if count >= 3:
            for i in like_ind:
                optimizations[i] += ["Aggregate a series of LIKE clauses into one regexp_like expression."]

    # Optimization # 6
    #       When the join condition involves several expressions, you can make
    #       it faster by pushing down this condition into a sub query
    #       to prepare a join key beforehand
    def __checkSimpleEquijoins(self, lines, optimizations):
        for ind, l in enumerate(lines):
            if re.search("\sON\s", l, re.IGNORECASE) is not None:
                if re.search("[+\-\*\/0-9]", l, re.IGNORECASE) is not None: # complex join = operations, numbers
                    optimizations[ind] += ["Push down a complex join condition into a sub query."]

    # Optimization # 7
    #       If your query becomes complex or deeply nested,
    #       try to extract sub queries using WITH clause.
    def __checkNestedQueries(self, lines, optimizations):
        for ind, l in enumerate(lines):
            if re.search("FROM\s*\(?\s*$", l, re.IGNORECASE) is not None:
                if re.search("\s*\(?\s*SELECT", lines[ind], re.IGNORECASE) is not None:
                    optimizations[ind] += ["Try to extract nested subqueries using a WITH clause."]
                if ind != len(lines) - 1 and re.search("\s*\(?\s*SELECT", lines[ind + 1], re.IGNORECASE) is not None:
                    optimizations[ind + 1] += ["Try to extract nested subqueries using a WITH clause."]

    # Optimization # 8
    #    Specify GROUP BY targets with numbers for expressions
    def __checkGroupByExpressions(self, lines, optimizations):
        for ind, l in enumerate(lines):
            if re.search("GROUP BY", l, re.IGNORECASE) is not None:
                # if all in one line
                vars = re.sub("GROUP BY", "", l).strip().split(",")
                for v in vars:
                    # Assume if var has whitespace, it's an expression
                    if re.search("\s", v.strip()):
                        optimizations[ind] += ["Specify GROUP BY targets with numbers for expressions"]

                # if vars on multiple lines
                i = ind
                while i < len(lines) - 1 and re.search(",", lines[i + 1]) is None:
                    if re.search("\s", lines[i + 1]) is not None:
                        optimizations[i] += ["Specify GROUP BY targets with numbers for expressions"]
                    i += 1
