import pytest
import glob

@pytest.fixture()
def queries():
    queries = []
    filepath = 'test_queries/*.txt'
    files = glob.glob(filepath)

    for file in files:
        f = open(file, 'r')
        query = f.read()
        print(query)
        queries += [query]
        f.close()

    return queries
