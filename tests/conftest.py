import pytest
import glob
import sqlparse

@pytest.fixture()
def queries():
    queries = []
    filepath = 'test_queries/*.txt'
    files = glob.glob(filepath)

    for file in files:
        f = open(file, 'r')
        query = f.read()

        # Format Query
        formatted_query = sqlparse.format(
                            query,
                            reindent = True,
                            keyword_case = 'upper'
                        )

        # Switch from unicode to utf-8 encoding
        formatted_query = formatted_query.encode('utf-8')

        queries += [query]
        f.close()

    return queries
