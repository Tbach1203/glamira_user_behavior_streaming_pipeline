import pyspark.sql.functions as f
from schema.schema_dim_location import location_schema

def load_location_master(spark, location_file_path):
    return (
        spark.read
        .schema(location_schema)
        .json(location_file_path)
        .select(
            "ip",
            f.col("country").alias("country_name"),
            f.col("region").alias("region_name"),
            f.col("city").alias("city_name")
        )
    )

def lookup_location(stream_df, location_master_df):
    return (
        stream_df.join(
            f.broadcast(location_master_df),
            on="ip",
            how="left"
        )
    )