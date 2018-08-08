import re
import sqlparse
from collections import defaultdict
from collections import OrderedDict
from clickhouse_cli.ui.parseutils.ctes import extract_ctes
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Newline, CTE, Wildcard

# Optimization # 2
#   Suggest selecting the columns the user wants explicitly, rather than using (SELECT *)
def checkColumnSelection(optimizations, parsed_queries, *db_params):
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
                                    optimizations[stmt].append((lineno, "select columns explicitly"))
                        elif token.ttype is Wildcard:
                            lineno = seen_stmt.count("\n")
                            optimizations[stmt].append((lineno, "select columns explicitly"))

                if token.ttype is DML and token.value.upper() == "SELECT":
                    select_seen = True
                seen_stmt += str(token)
