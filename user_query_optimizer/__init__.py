from presto_optimizer import PrestoOptimizer
from athena_optimizer import AthenaOptimizer
from spark_optimizer import SparkOptimizer

def get_optimizer(db, schema):
    if db == "presto":
        return PrestoOptimizer(schema)
    elif db == "athena":
        return AthenaOptimizer(schema)
    elif db == "spark":
        return SparkOptimizer(schema)
    else:
        raise ValueError("Database must be one of: 'presto', 'athena', 'spark'")

__version__ = "1.0.0.dev0"

__title__ = "user-query-optimizer"
__description__ = "SQL query optimization hints"
__url__ = "https://github.com/mozilla/user-query-optimizer"
__uri__ = __url__
__doc__ = __description__ + " <" + __uri__ + ">"

__author__ = "Swathi Iyer"
__email__ = "swathii@stanford.edu"

__license__ = "MPL"
