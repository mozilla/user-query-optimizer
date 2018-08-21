import re
import sqlparse
from collections import defaultdict
from collections import OrderedDict
from clickhouse_cli.ui.parseutils.ctes import extract_ctes
from clickhouse_cli.ui.parseutils.tables import extract_tables
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Newline, CTE, Wildcard

# Optimization # 6
def checkFiltering(optimizations, parsed_queries, *db_params):
    for stmt_list in parsed_queries:
        for stmt in stmt_list:
            seen_stmt = ""
            for token in stmt.tokens:
                if isinstance(token, Where):
                    # newlines in sqlparse sometimes group clauses together - need to recalculate
                    for item in token.tokens:
                        if isinstance(item, Function):
                            if re.search("date_parse\(", str(item), re.IGNORECASE):
                                lineno = seen_stmt.count("\n")
                                optimizations[stmt].append((lineno, "eliminate overhead by using 'submission_date_s3 >= date_format(...) rather than date_parse(submission_date_s3, '%Y%m%d') >= ..."))

                seen_stmt += str(token)
