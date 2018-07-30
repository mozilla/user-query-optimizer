import re
from collections import defaultdict
from collections import OrderedDict
import sys
sys.path.append('../user-query-optimizer')
import optimizer
import sqlparse

def test_approximates(queries):
    print("\n")
    # dictionary from test-query-file -> list of line numbers in that query with an approx optimization
    correct_opts = {
        'test-query-3.txt': [0]
    }
    test_ops = {}

    for ind, query in enumerate(queries):
        # Instantiate optimizer object
        schema = {} # Placeholder
        op = optimizer.Optimizer(query, schema)

        # Parse query and extract ctes
        # Strip comments to help sqlparse correctly extract the identifier list 
        formatted_query = str(sqlparse.format(query, strip_comments = True))
        parsed_queries = op._Optimizer__parse_query(formatted_query)

        op._Optimizer__checkApproximates(parsed_queries)

        # Find subquery in original query again, and adjust line numbers
        adjusted_opts = op._Optimizer__adjust_linenums(formatted_query)

        # Print query with line numbers
        op._Optimizer__print_query_lines(formatted_query)

        # Print Optimizations
        op._Optimizer__print_optimizations(adjusted_opts)

        # Add optimizations for current query to dictionary for all test files
        if len(adjusted_opts) > 0:
            test_ops['test-query-' + str(ind + 1) + '.txt'] = adjusted_opts.keys()


    assert len(test_ops) == 1
    assert test_ops == correct_opts
