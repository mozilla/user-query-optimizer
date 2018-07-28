import re
from collections import defaultdict
from collections import OrderedDict
import sys
sys.path.append('../user-query-optimizer')
import optimizer

def test_approximates(queries):
    # dictionary from test query # -> list of line numbers in that query with an approx optimization
    correct_opts = {
        'test-query-3.txt': [0]
    }
    test_ops = {}

    for ind, query in enumerate(queries[0:1]):
        # Instantiate optimizer object
        schema = {} # Placeholder
        op = optimizer.Optimizer(query, schema)

        # Split so optimizations can be made with reference to specific lines in query and remove comments
        # lines = [re.sub("(--.*|#.*)", "", line) for line in query.splitlines()]

        optimizations = defaultdict(list)
        query_ops = op._Optimizer__checkApproximates(optimizations)

        if len(query_ops) > 0:
            test_ops['test-query-' + str(ind + 1) + '.txt'] = query_ops.keys()

    assert len(test_ops) == 1
    assert test_ops == correct_opts
