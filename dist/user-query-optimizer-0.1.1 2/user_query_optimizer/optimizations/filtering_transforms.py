import re
import types
import sqlparse
from collections import defaultdict
from collections import OrderedDict
from clickhouse_cli.ui.parseutils.ctes import extract_ctes
from clickhouse_cli.ui.parseutils.tables import extract_tables
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Newline, CTE, Wildcard


# Optimization # 6
def checkFiltering(optimizations, schema, parsed_queries, *db_params):
    for stmt_list in parsed_queries:
        for stmt in stmt_list:
            seen_stmt = ""
            for token in stmt.tokens:
                if isinstance(token, Where):
                    # newlines in sqlparse sometimes group clauses together - need to recalculate
                    for item in token.tokens:
                        if isinstance(item, Function):
                            params = [str(token) for token in list(item.get_parameters())]
                            for param in params:
                                if param in schema["partitions"]:
                                    lineno = seen_stmt.count("\n")
                                    optimizations[stmt].append((lineno, "eliminate overhead from transforming partitioned column " + param))

                seen_stmt += str(token)
