import re
from collections import defaultdict
from collections import OrderedDict
import sys
sys.path.append('../user-query-optimizer')
import optimizer
import sqlparse

def test_approximates(queries, op):
    print("\n")
    # dictionary from test-query-file -> list of line numbers in that query with an approx optimization
    correct_opts = {
        'test-query-3.txt': [0]
    }
    test_ops = {}

    for ind, query in enumerate(queries):
        # Parse query and extract ctes
        # Strip comments to help sqlparse correctly extract the identifier list
        formatted_query = str(sqlparse.format(query, strip_comments = True)).strip()
        parsed_queries = op._parse_query(formatted_query)

        op._checkApproximates(parsed_queries)

        # Find subquery in original query again, and adjust line numbers
        adjusted_opts = op._adjust_linenums(formatted_query)

        # Print query with line numbers
        # op._print_query_lines(formatted_query)

        # Print Optimizations
        # op._print_optimizations(adjusted_opts)

        # Add optimizations for current query to dictionary for all test files
        if len(adjusted_opts) > 0:
            test_ops['test-query-' + str(ind + 1) + '.txt'] = adjusted_opts.keys()


    assert len(test_ops) == 1
    assert test_ops == correct_opts

def test_column_selection(queries, op):
    print("\n")
    # dictionary from test-query-file -> list of line numbers in that query with an approx optimization
    correct_opts = {
        'test-query-2.txt': [1],
        'test-query-3.txt': [0],
        'test-query-5.txt': [0],
        'test-query-9.txt': [0]
    }
    test_ops = {}

    for ind, query in enumerate(queries):
        # Parse query and extract ctes
        # Strip comments to help sqlparse correctly extract the identifier list
        formatted_query = str(sqlparse.format(query, strip_comments = True)).strip()
        parsed_queries = op._parse_query(formatted_query)

        op._checkColumnSelection(parsed_queries)

        # Find subquery in original query again, and adjust line numbers
        adjusted_opts = op._adjust_linenums(formatted_query)

        # Print query with line numbers
        # op._print_query_lines(formatted_query)

        # Print Optimizations
        # op._print_optimizations(adjusted_opts)

        # Add optimizations for current query to dictionary for all test files
        if len(adjusted_opts) > 0:
            test_ops['test-query-' + str(ind + 1) + '.txt'] = adjusted_opts.keys()


    assert len(test_ops) == 4
    assert test_ops == correct_opts

def test_partitions(queries, op):
    print("\n")
    # dictionary from test-query-file -> list of line numbers in that query with an approx optimization
    correct_opts = {
        'test-query-1.txt': [0],
        'test-query-2.txt': [10, 17, 22, 35],
        'test-query-3.txt': [0],
        'test-query-4.txt': [0, 4],
        'test-query-5.txt': [3],
        'test-query-6.txt': [0], # for some reason, sqlparse not recognizing two statemnets
        'test-query-7.txt': [0],
        'test-query-8.txt': [0],
        'test-query-10.txt': [0]
    }
    test_ops = {}

    for ind, query in enumerate(queries):
        # Parse query and extract ctes
        # Strip comments to help sqlparse correctly extract the identifier list
        formatted_query = str(sqlparse.format(query, strip_comments = True)).strip()
        parsed_queries = op._parse_query(formatted_query)

        op._checkPartitions(parsed_queries)

        # Find subquery in original query again, and adjust line numbers
        adjusted_opts = op._adjust_linenums(formatted_query)

        # Print query with line numbers
        # op._print_query_lines(formatted_query)

        # Print Optimizations
        # op._print_optimizations(adjusted_opts)

        # Add optimizations for current query to dictionary for all test files
        if len(adjusted_opts) > 0:
            test_ops['test-query-' + str(ind + 1) + '.txt'] = adjusted_opts.keys()

        # Sort list of lines for comparison
        for k, v in test_ops.iteritems():
            test_ops[k] = sorted(v)

    assert len(test_ops) == 9
    assert test_ops == correct_opts

def test_nested_subqueries(queries, op):
    print("\n")
    # dictionary from test-query-file -> list of line numbers in that query with an approx optimization
    correct_opts = {
        'test-query-6.txt': [7],
        'test-query-7.txt': [0],
        'test-query-10.txt': [4]
    }
    test_ops = {}
    
    for ind, query in enumerate(queries):
        # Parse query and extract ctes
        # Strip comments to help sqlparse correctly extract the identifier list
        formatted_query = str(sqlparse.format(query, strip_comments = True)).strip()
        parsed_queries = op._parse_query(formatted_query)

        op._extractNestedSubqueries(parsed_queries)

        # Find subquery in original query again, and adjust line numbers
        adjusted_opts = op._adjust_linenums(formatted_query)

        # Print query with line numbers
        op._print_query_lines(formatted_query)

        # Print Optimizations
        op._print_optimizations(adjusted_opts)

        # Add optimizations for current query to dictionary for all test files
        if len(adjusted_opts) > 0:
            test_ops['test-query-' + str(ind + 1) + '.txt'] = adjusted_opts.keys()


    assert len(test_ops) == 3
    assert test_ops == correct_opts
