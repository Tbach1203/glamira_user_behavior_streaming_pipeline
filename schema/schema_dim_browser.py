from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

browser_schema = StructType([
    StructField("user_agent", StringType(), True),
    StructField("browser_name", StringType(), True),
    StructField("browser_version", StringType(), True),
    StructField("os_name", StringType(), True),
    StructField("os_version", StringType(), True),
    StructField("device_family", StringType(), True),
    StructField("device_type", StringType(), True),
])