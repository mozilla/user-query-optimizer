import sys
import re

# Suggest using approximate algorithms (e.g. approx_distinct()
# instead of COUNT(DISTINCT ...))
def checkApproximates(query):
    pass

# Suggest selecting the columns the user wants explicitly,
# rather than using (SELECT *)
def checkColumnSelection(query):
    if re.search('select \*', query) is not None:
        print('\tIf you do not need all columns of the table, select the columns you want explicitly.\n')

# Suggest filtering on partitioned columns
def checkPartitions(query):
    pass

# Execute each optimization check
def giveHints(query):
    checkApproximates(query)
    checkColumnSelection(query)
    checkPartitions(query)


if __name__ == '__main__':
    # Read File
    fname = sys.argv[1]
    with open(fname) as f:
        query = f.read()

    # Ignore case
    query = query.lower()

    # Break into subqueries
    subqueries = ['select' + q for q in query.split('select')]

    for query in subqueries:
        print(query)
        print("Optimizations:")
        giveHints(query)
        print("\n")
