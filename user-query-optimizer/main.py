import sys
import optimizer

if __name__ == '__main__':
    # Read File
    fname = sys.argv[1]
    with open(fname) as f:
        query = f.read()

    # Ignore case
    schema = {} # placeholder

    # Instantiate optimizer object
    optimizer = optimizer.Optimizer(query, schema)

    # Optimize query
    optimizer.optimize_query()
