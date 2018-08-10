import re
import sqlparse
from collections import defaultdict
from collections import OrderedDict
from clickhouse_cli.ui.parseutils.ctes import extract_ctes
from sqlparse.sql import IdentifierList, Identifier, Function, Where, Comparison
from sqlparse.tokens import Keyword, DML, Newline, CTE, Wildcard

# Optimization #1
#   Suggest using approximate algorithms (e.g. approx_distinct() instead of COUNT(DISTINCT ...));
#   Consider other approximations later on (when to use approx_percentile?);
#   Extracts SELECT identifiers, looks for "COUNT(DISTINCT ....)" and adds
#   statement as key, and (line no, "use approximation") tuple to corresponding
#   list in self.optimizations dictionary if found;
#   Same for all databases I think?

def checkApproximates(optimizations, parsed_queries, *db_params):
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
                                    optimizations[stmt].append((lineno, "use approximation - " + db_params[0]))
                        elif isinstance(token, Identifier):
                            if re.search("COUNT\s*\(\s*DISTINCT", str(token), re.IGNORECASE):
                                lineno = seen_stmt.count("\n")
                                optimizations[stmt].append((lineno, "use approximation - " + db_params[0]))
                if token.ttype is DML and token.value.upper() == "SELECT":
                    select_seen = True
                seen_stmt += str(token)
