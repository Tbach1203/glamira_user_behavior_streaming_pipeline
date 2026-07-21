import pyspark.sql.functions as f
from utils.product_mapping import (load_product_master,lookup_product)

def build_dim_product(spark,raw_df,product_file_path):
    stream_product_df = (
        raw_df
        .dropDuplicates(["product_id"])
        .select("product_id")
        .filter(
            f.col("product_id").isNotNull()
        )
    )
    product_master_df = load_product_master(spark, product_file_path)
    product_df = lookup_product(stream_product_df, product_master_df)
    product_df = (
        product_df
        .withColumn(
            "product_key",
            f.sha2(
                f.col("product_id").cast("string"),
                256
            )
        )
        .select(
            "product_key",
            f.col("product_id").cast("long").alias("product_id"),
            f.col("name").alias("product_name"),
            f.col("price").alias("product_price")
        )
    )
    return product_df