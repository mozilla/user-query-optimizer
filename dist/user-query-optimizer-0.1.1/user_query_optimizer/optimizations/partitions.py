import re
import sqlparse
from collections import defaultdict
from collections import OrderedDict
from clickhouse_cli.ui.parseutils.ctes import extract_ctes
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Newline, CTE, Wildcard

# Optimization # 3
#    Suggest filtering on partitioned columns
#    Only make suggestion if user has not filtered on ANY partitioned columns
#    Future: give indications about the size of data that will be scanned
#   (e.g. you are filtering on a partitioned column, but it will be 2.2TB of data.
#       Considering reducing the set of data further, using the same partitioned colum
#       or [other partitioned columns here])
#   Line numbers: If there's a where clause without a partitioned column, line number = where clause;
#                If there's no where clause, line number = first line of the query / CTE
def checkPartitions(optimizations, schema, parsed_queries, *db_params):
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
                            if item.left.value in schema["partitions"] or \
                                item.right.value in schema["partitions"]:
                                    partition_seen = True
                                    break
                seen_stmt += str(token)
            if not partition_seen:
                lineno = 0 if where_line is None else where_line
                optimizations[stmt].append((lineno, "filter on a partitioned column"))
