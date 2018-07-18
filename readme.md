
## Introduction
This API will help you optimize your sql queries for better performance.

## Workflow

### `optimize_query(query, schema)`
**Not Yet Implemented**
Input: query (type: string), schema (type: tbd)

Output: optimization hints

Usage:
    * Create an optimizer object: `optimizer = Optimizer()`
    * Get optimization hints: `optimizer.optimize_query(query, schema)`

Initial Optimization Checks
  * Using approximate algorithms (`approx_distinct()` instead of `COUNT(DISTINCT ...)`)
  * Selecting the columns the user wants explicitly, rather than using `(SELECT *)`
  * Filtering on partitioned columns
