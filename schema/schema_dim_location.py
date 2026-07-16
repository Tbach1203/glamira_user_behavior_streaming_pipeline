from pyspark.sql.types import (StructType,StructField,StringType)

location_schema = StructType([
    StructField("ip", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
])