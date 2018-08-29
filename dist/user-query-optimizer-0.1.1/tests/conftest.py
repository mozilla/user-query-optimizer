import pytest
import glob
import sqlparse
import re
import sys
sys.path.append('../')
from user_query_optimizer import get_optimizer

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

@pytest.fixture()
def presto_op():
    schema = {"partitions" : ["submission_date_s3", "app_name", "os"]}
    optimizer = get_optimizer("presto", schema)
    return optimizer

@pytest.fixture()
def athena_op():
    schema = {"partitions" : ["submission_date_s3", "app_name", "os"]}
    optimizer = get_optimizer("athena", schema)
    return optimizer

@pytest.fixture()
def spark_op():
    schema = {"partitions" : ["submission_date_s3", "app_name", "os"]}
    optimizer = get_optimizer("spark", schema)
    return optimizer
