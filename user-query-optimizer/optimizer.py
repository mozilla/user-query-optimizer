import re
import sqlparse
from collections import defaultdict
from collections import OrderedDict
from clickhouse_cli.ui.parseutils.ctes import extract_ctes
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Newline, CTE, Wildcard


class Optimizer:
    def __init__(self, schema, db):
        self.schema = schema
        # Database: Presto, Athena, or Spark
        self.db = db
        # Dictionary: statement -> list of tuples (lineno, optimization)
        self.optimizations = defaultdict(list)


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

    # Run each optimiztion check on parsed queries and update optimizations dictionary
    # Input: parsed_queries
    # Output: None
    def _runOptimizationChecks(self, parsed_queries):
        # Run tests
        self._checkApproximates(parsed_queries)
        self._checkColumnSelection(parsed_queries)
        self._checkPartitions(parsed_queries)

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
        adjusted_opts = {}
        for k, v in self.optimizations.iteritems():
            for opt in v:
                match = formatted_query.find(str(k).strip())
                if match != -1:
                    adjusted_lineno = formatted_query[:match].count("\n") + opt[0]
                    adjusted_opts[adjusted_lineno] = opt[1]
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
                print("\tLine " + str(k + 1) + ": " + v + "\n")
        print("\n")

    # Optimization #1
    #   Suggest using approximate algorithms (e.g. approx_distinct() instead of COUNT(DISTINCT ...));
    #   Consider other approximations later on (when to use approx_percentile?);
    #   Extracts SELECT identifiers, looks for "COUNT(DISTINCT ....)" and adds
    #   statement as key, and (line no, "use approximation") tuple to corresponding
    #   list in self.optimizations dictionary if found;
    #   Same for all databases I think?

    def _checkApproximates(self, parsed_queries):
        for stmt_list in parsed_queries:
            for stmt in stmt_list:
                seen_stmt = ""
                select_seen = False
                for token in stmt.tokens:
                    if select_seen:
                        if token.ttype is Keyword and token.value.upper() == "FROM":
                            break
                        else:
                            if isinstance(token, IdentifierList):
                                for identifier in token.get_identifiers():
                                    if re.search("COUNT\s*\(\s*DISTINCT", str(identifier), re.IGNORECASE):
                                        # newlines in sqlparse sometimes group clauses together - need to recalculate
                                        lineno = seen_stmt.count("\n")
                                        self.optimizations[stmt].append((lineno, "use approximation"))
                            elif isinstance(token, Identifier):
                                if re.search("COUNT\s*\(\s*DISTINCT", str(token), re.IGNORECASE):
                                    lineno = seen_stmt.count("\n")
                                    self.optimizations[stmt].append((lineno, "use approximation"))
                    if token.ttype is DML and token.value.upper() == "SELECT":
                        select_seen = True
                    seen_stmt += str(token)

    # Optimization # 2
    #   Suggest selecting the columns the user wants explicitly, rather than using (SELECT *)
    def _checkColumnSelection(self, parsed_queries):
        for stmt_list in parsed_queries:
            for stmt in stmt_list:
                seen_stmt = ""
                select_seen = False
                for token in stmt.tokens:
                    if select_seen:
                        if token.ttype is Keyword and token.value.upper() == "FROM":
                            break
                        else:
                            if isinstance(token, IdentifierList):
                                for identifier in token.get_identifiers():
                                    if identifier.ttype is Wildcard:
                                        # newlines in sqlparse sometimes group clauses together - need to recalculate
                                        lineno = seen_stmt.count("\n")
                                        self.optimizations[stmt].append((lineno, "select columns explicitly"))
                            elif token.ttype is Wildcard:
                                lineno = seen_stmt.count("\n")
                                self.optimizations[stmt].append((lineno, "select columns explicitly"))

                    if token.ttype is DML and token.value.upper() == "SELECT":
                        select_seen = True
                    seen_stmt += str(token)

    # Optimization # 3
    #    Suggest filtering on partitioned columns
    #    Only make suggestion if user has not filtered on ANY partitioned columns
    #    Future: give indications about the size of data that will be scanned
    #   (e.g. you are filtering on a partitioned column, but it will be 2.2TB of data.
    #       Considering reducing the set of data further, using the same partitioned colum
    #       or [other partitioned columns here])
    #   Line numbers: If there's a where clause without a partitioned column, line number = where clause;
    #                If there's no where clause, line number = first line of the query / CTE
    def _checkPartitions(self, parsed_queries):
        for stmt_list in parsed_queries:
            where_line = None
            for stmt in stmt_list:
                seen_stmt = ""
                partition_seen = False
                for token in stmt.tokens:
                    if isinstance(token, Where):
                        # newlines in sqlparse sometimes group clauses together - need to recalculate
                        where_line = seen_stmt.count("\n")
                        for item in token.tokens:
                            if isinstance(item, Comparison):
                                if item.left.value in self.schema["partitions"] or \
                                    item.right.value in self.schema["partitions"]:
                                        partition_seen = True
                                        break
                    seen_stmt += str(token)
                if not partition_seen:
                    lineno = 0 if where_line is None else where_line
                    self.optimizations[stmt].append((lineno, "filter on a partitioned column"))

    # Optimization # 4
    #       Use a WITH clause rather than nested subqueries
    def _extractNestedSubqueries(self, parsed_queries):
        for stmt_list in parsed_queries:
            for stmt in stmt_list:
                seen_stmt = ""
                for token in stmt.tokens:
                    if isinstance(token, IdentifierList):
                        for identifier in token.get_identifiers():
                            if re.search("\s*\(\s*SELECT", str(identifier), re.IGNORECASE):
                                lineno = seen_stmt.count("\n")
                                self.optimizations[stmt].append((lineno, "use a WITH clause rather than a nested subquery."))
                    elif isinstance(token, Identifier):
                        if re.search("\s*\(\s*SELECT", str(token), re.IGNORECASE):
                            lineno = seen_stmt.count("\n")
                            self.optimizations[stmt].append((lineno, "use a WITH clause rather than a nested subquery."))
                    elif isinstance(token, Comparison):
                        if re.search("\s*\(\s*SELECT", str(token.left.value), re.IGNORECASE) \
                            or re.search("\s*\(\s*SELECT", str(token.right.value), re.IGNORECASE):
                            lineno = seen_stmt.count("\n")
                            self.optimizations[stmt].append((lineno, "use a WITH clause rather than a nested subquery."))

                    seen_stmt += str(token)
