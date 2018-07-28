import pytest
import glob
import sqlparse

@pytest.fixture()
def queries():
    queries = []
    filepath = 'test_queries/*.txt'
    files = glob.glob(filepath)

    for file in files[2:3]:
        f = open(file, 'r')
        query = f.read()
        queries += [query]
        f.close()

    return queries
