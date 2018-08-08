import re
import sqlparse
from collections import defaultdict
from collections import OrderedDict
from clickhouse_cli.ui.parseutils.ctes import extract_ctes
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Newline, CTE, Wildcard

# Optimization # 4
#       Use a WITH clause rather than nested subqueries
def extractNestedSubqueries(optimizations, parsed_queries, *db_params):
    for stmt_list in parsed_queries:
        for stmt in stmt_list:
            seen_stmt = ""
            for token in stmt.tokens:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        if re.search("\s*\(\s*SELECT", str(identifier), re.IGNORECASE):
                            lineno = seen_stmt.count("\n")
                            optimizations[stmt].append((lineno, "use a WITH clause rather than a nested subquery."))
                elif isinstance(token, Identifier):
                    if re.search("\s*\(\s*SELECT", str(token), re.IGNORECASE):
                        lineno = seen_stmt.count("\n")
                        optimizations[stmt].append((lineno, "use a WITH clause rather than a nested subquery."))
                elif isinstance(token, Comparison):
                    if re.search("\s*\(\s*SELECT", str(token.left.value), re.IGNORECASE) \
                        or re.search("\s*\(\s*SELECT", str(token.right.value), re.IGNORECASE):
                        lineno = seen_stmt.count("\n")
                        optimizations[stmt].append((lineno, "use a WITH clause rather than a nested subquery."))

                seen_stmt += str(token)
