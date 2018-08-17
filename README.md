
## Introduction
This API will help you optimize your sql queries for better performance.

## Workflow

### Create the optimizer object
Object specific to a single (database, schema) pair
e.g. `optimizer = get_optimizer("presto", schema)`

### Get optimization hints using `optimize_query(query)`
Output: optimization hints

Usage: `presto_op.optimize_query(query)`

Initial Optimization Checks
  * Using approximate algorithms (`approx_distinct()` instead of `COUNT(DISTINCT ...)`)
  * Selecting the columns the user wants explicitly, rather than using `(SELECT *)`
  * Filtering on partitioned columns
  * Try to extract nested subqueries using a WITH clause.
  * Filter using most effective columns by parquet file ordering

Other Stuff
  * Eliminate date_parse overhead
  * Replace UNION with UNION ALL if duplicates do not need to be removed
  * Aggregate a series of LIKE clauses into one regexp_like expression
  * Push down a complex join condition into a sub query
  * Specify GROUP BY targets with numbers for expressions

### Testing
To run unit tests, run `py.test` (or `py.test -s` to see stdout) in the tests directory.
