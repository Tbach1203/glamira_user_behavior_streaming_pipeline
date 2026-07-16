from pyspark.sql.types import (StructType, StructField, IntegerType, LongType, DateType, StringType)

date_schema = StructType([
    StructField("date_key", LongType(), False),
    StructField("full_date", DateType(), False),
    StructField("hour", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("year", IntegerType(), False),
    StructField("day_name", StringType(), False),
    StructField("month_name", StringType(), False),
])