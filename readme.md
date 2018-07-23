
## Introduction
This API will help you optimize your sql queries for better performance.

## Workflow

### Create the optimizer object
Object specific to a single (query, schema) pair
e.g. `optimizer = Optimizer(query, schema)`

### Get optimization hints using `optimize_query()`
Output: optimization hints

Usage: `optimizer.optimize_query()`

Initial Optimization Checks
  * Using approximate algorithms (`approx_distinct()` instead of `COUNT(DISTINCT ...)`)
  * Selecting the columns the user wants explicitly, rather than using `(SELECT *)`
  * Filtering on partitioned columns (Not yet implemented)
  * Replace UNION with UNION ALL if duplicates do not need to be removed
  * Aggregate a series of LIKE clauses into one regexp_like expression
  * Push down a complex join condition into a sub query
  * Try to extract nested subqueries using a WITH clause.
  * Specify GROUP BY targets with numbers for expressions (in-progress)

### Testing
To test the optimizer on the test queries, run `python main.py <filename>`.
e.g. `python main.py test_queries/test-query-5.txt`
