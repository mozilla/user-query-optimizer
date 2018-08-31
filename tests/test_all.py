def test_all(queries, presto_op):
    correct_ops = {
        'test-query-1.txt': {0: ['filter on a partitioned column',
                                 "Filtering on ordered columns can improve performance. Here are some that you aren't filtering on: os, app_name, submission_date_s3"]},
        'test-query-2.txt': {1: ['select columns explicitly'],
                             3: ["Filtering on ordered columns can improve performance. Here are some that you aren't filtering on: submission_date_s3"]},
        'test-query-3.txt': {0: ['use approximation - approx_distinct',
                                 'select columns explicitly',
                                 'filter on a partitioned column',
                                 "Filtering on ordered columns can improve performance. Here are some that you aren't filtering on: os, app_name, submission_date_s3"]},
        'test-query-4.txt': {0: ['filter on a partitioned column',
                                 "Filtering on ordered columns can improve performance. Here are some that you aren't filtering on: os, app_name, submission_date_s3"],
                            4: ['filter on a partitioned column',
                                "Filtering on ordered columns can improve performance. Here are some that you aren't filtering on: os, app_name, submission_date_s3"]},
        'test-query-5.txt': {0: ['select columns explicitly'],
                             3: ['filter on a partitioned column',
                                 "Filtering on ordered columns can improve performance. Here are some that you aren't filtering on: os, app_name, submission_date_s3"]},
        'test-query-6.txt': {0: ['filter on a partitioned column',
                                 "Filtering on ordered columns can improve performance. Here are some that you aren't filtering on: os, app_name, submission_date_s3"],
                             7: ['use a WITH clause rather than a nested subquery.']},
        'test-query-7.txt': {0: ['filter on a partitioned column',
                                 'use a WITH clause rather than a nested subquery.']},
        'test-query-8.txt': {0: ['filter on a partitioned column',
                                 "Filtering on ordered columns can improve performance. Here are some that you aren't filtering on: os, app_name, submission_date_s3"]},
        'test-query-9.txt': {0: ['select columns explicitly'],
                             3: ["Filtering on ordered columns can improve performance. Here are some that you aren't filtering on: os, app_name"]},
        'test-query-10.txt': {0: ['filter on a partitioned column',
                                  "Filtering on ordered columns can improve performance. Here are some that you aren't filtering on: os, app_name, submission_date_s3"],
                             4: ['use a WITH clause rather than a nested subquery.']}
    }

    test_ops = {}

    for ind, query in enumerate(queries):
        adjusted_opts = presto_op.optimize_query(query)

        # Add optimizations for current query to dictionary for all test files
        if len(adjusted_opts) > 0:
            test_ops['test-query-' + str(ind + 1) + '.txt'] = adjusted_opts

    print(test_ops)

    assert len(test_ops) == 10
    assert test_ops == correct_ops
