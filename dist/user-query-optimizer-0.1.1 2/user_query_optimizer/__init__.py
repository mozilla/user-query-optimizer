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
