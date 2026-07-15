from pyspark.sql.types import (StructType,StructField,StringType)

location_schema = StructType([
    StructField("ip", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("city_name", StringType(), True),
])