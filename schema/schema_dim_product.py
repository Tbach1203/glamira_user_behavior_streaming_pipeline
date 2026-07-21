from pyspark.sql.types import (StructType,StructField,StringType,DoubleType,LongType)

product_schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
])