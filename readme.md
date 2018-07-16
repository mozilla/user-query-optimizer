## Initial Optimizations
  * Using approximate algorithms (approx_distinct() instead of COUNT(DISTINCT ...))
  * Selecting the columns the user wants explicitly, rather than using (SELECT *)
  * Filtering on partitioned columns (although not sure how to identify which columns are partitioned outside of redash)
