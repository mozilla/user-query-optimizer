import sys
sys.path.append('../user-query-optimizer')
import optimizer

def test_approximates(queries):
    assert len(queries) == 8
