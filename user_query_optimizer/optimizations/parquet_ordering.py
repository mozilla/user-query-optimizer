import re
import sqlparse
from collections import defaultdict
from collections import OrderedDict
from clickhouse_cli.ui.parseutils.ctes import extract_ctes
from clickhouse_cli.ui.parseutils.tables import extract_tables
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Newline, CTE, Wildcard

# Optimization # 4
def checkOrdering(optimizations, schema, parsed_queries, *db_params):
    for stmt_list in parsed_queries:
        where_line = None
        for stmt in stmt_list:
            tables = [str(tr.name) for tr in extract_tables(str(stmt))]
            if schema["name"] not in tables:
                continue
            ordering = set(schema["ordering"])
            seen_stmt = ""
            for token in stmt.tokens:
                if isinstance(token, Where):
                    # newlines in sqlparse sometimes group clauses together - need to recalculate
                    where_line = seen_stmt.count("\n")
                    for item in token.tokens:
                        if isinstance(item, Comparison):
                            if item.left.value in ordering:
                                 ordering.remove(item.left.value)
                            if item.right.value in ordering:
                                    ordering.remove(item.right.value)
                seen_stmt += str(token)
            if len(ordering) != 0:
                lineno = 0 if where_line is None else where_line
                optimizations[stmt].append((lineno, "recommended columns to filter from greatest to least performance boost: " + ", ".join(schema["ordering"])))
