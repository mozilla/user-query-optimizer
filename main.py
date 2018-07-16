import sys
import re

def parseQuery(query):
    keywords = ["select", "where", "from", "group by", "order by"]


if __name__ == '__main__':
    # Read File
    fname = sys.argv[1]
    with open(fname) as f:
        query = f.read()

    # Ignore case
    query = query.lower()

    # Break into subqueries
    subqueries = ["select" + q for q in query.split("select")]
