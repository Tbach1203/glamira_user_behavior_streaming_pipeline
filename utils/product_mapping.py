import pyspark.sql.functions as f


def load_product_master(spark,product_file_path):
    return (
        spark.read
        .json(product_file_path)
        .select(
            "product_id",
            "name",
            "price"
        )
    )

def lookup_product(stream_df,product_master_df):
    # Lookup product_id từ stream sang master product
    return (
        stream_df
        .join(
            f.broadcast(product_master_df),
            on="product_id",
            how="left"
        )
    )