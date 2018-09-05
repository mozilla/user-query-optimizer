import re
from collections import defaultdict
from collections import OrderedDict
import sys
sys.path.append('../user_query_optimizer')
import optimizer
import sqlparse

def test_all(queries, presto_op):
    correct_ops = {
        'test-query-1.txt': {0: ['filter on a partitioned column']},
        'test-query-2.txt': {1: ['select columns explicitly'],
                            10: ['filter on a partitioned column'],
                            35: ['filter on a partitioned column'],
                            22: ['filter on a partitioned column'],
                            17: ['filter on a partitioned column']},
        'test-query-3.txt': {0: ['use approximation - approx_distinct',
                                'select columns explicitly',
                                'filter on a partitioned column']},
        'test-query-4.txt': {0: ['filter on a partitioned column'],
                            4: ['filter on a partitioned column']},
        'test-query-5.txt': {0: ['select columns explicitly'],
                            3: ['filter on a partitioned column']},
        'test-query-6.txt': {0: ['filter on a partitioned column'],
                            7: ['use a WITH clause rather than a nested subquery.']},
        'test-query-7.txt': {0: ['filter on a partitioned column',
                                'use a WITH clause rather than a nested subquery.']},
        'test-query-8.txt': {0: ['filter on a partitioned column']},
        'test-query-9.txt': {0: ['select columns explicitly']},
        'test-query-10.txt': {0: ['filter on a partitioned column'],
                            4: ['use a WITH clause rather than a nested subquery.']}
    }

    test_ops = {}

    for ind, query in enumerate(queries):
        adjusted_opts = presto_op.optimize_query(query)

        # Add optimizations for current query to dictionary for all test files
        if len(adjusted_opts) > 0:
            test_ops['test-query-' + str(ind + 1) + '.txt'] = adjusted_opts

    assert len(test_ops) == 10
    assert test_ops == correct_ops
