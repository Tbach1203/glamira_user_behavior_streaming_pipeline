from pyspark.sql.types import (
    StructType, StructField, StringType
)

product_schema = StructType([
    StructField("product_key", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("product_type_value", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("price", StringType(), True),
    StructField("store_code", StringType(), True),
])