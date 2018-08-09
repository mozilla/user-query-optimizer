import re
from collections import defaultdict
from collections import OrderedDict
import sys
sys.path.append('../user-query-optimizer')
import optimizer
import sqlparse

def test_all(queries, op):
    test_ops = {}

    for ind, query in enumerate(queries):
        adjusted_opts = op.optimize_query(query)

        # Add optimizations for current query to dictionary for all test files
        if len(adjusted_opts) > 0:
            test_ops['test-query-' + str(ind + 1) + '.txt'] = adjusted_opts.keys()
