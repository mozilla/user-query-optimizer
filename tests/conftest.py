import pytest
import glob
import sqlparse
import re

@pytest.fixture()
def queries():
    queries = []
    filepath = 'test_queries/*.txt'
    files = glob.glob(filepath)

    for file in sorted(files, key = sort_files_by_number):
        f = open(file, 'r')
        query = f.read()
        queries.append(query)
        f.close()

    return queries


def sort_files_by_number(value):
    numbers = re.compile(r'(\d+)')
    return int(numbers.split(value)[1])
