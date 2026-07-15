from pyspark.sql.types import (StructType,StructField,StringType,DoubleType)

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
])