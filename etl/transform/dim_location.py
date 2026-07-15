import pyspark.sql.functions as f
from utils.location_mapping import (load_location_master,lookup_location)


def build_dim_location(spark, raw_df, location_file_path):
    stream_location_df = (
        raw_df
        .dropDuplicates(["ip"])
        .select("ip")
        .filter(
            f.col("ip").isNotNull()
        )
    )

    location_master_df = load_location_master(spark, location_file_path)
    location_df = lookup_location(stream_location_df, location_master_df)

    location_df = (
        location_df
        .withColumn(
            "location_key",
            f.sha2(
                f.concat_ws(
                    "|",
                    f.col("country_name"),
                    f.col("region_name"),
                    f.col("city_name")
                ),256)
        )
        .select(
            "location_key",
            "ip",
            "country_name",
            "region_name",
            "city_name"
        )
    )
    return location_df