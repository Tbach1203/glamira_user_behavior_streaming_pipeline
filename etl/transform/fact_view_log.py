import pyspark.sql.functions as f

from utils.fact_view_log_lookup import (lookup_product, lookup_browser, lookup_location, lookup_date)


def build_fact_view_log(raw_df, browser_df, product_df, location_df, date_df):

    fact_df = lookup_browser(raw_df, browser_df)
    fact_df = lookup_product(fact_df, product_df)
    fact_df = lookup_location(fact_df, location_df)
    fact_df = lookup_date(fact_df, date_df)

    fact_df = (
        fact_df
        .withColumn(
            "view_log_key",
            f.sha2(
                f.col("id").cast("string"),
                256
            )
        )
        .select(
            "view_log_key",

            "product_key",
            "date_key",
            "browser_key",
            "location_key",

            f.col("store_id").cast("int").alias("store_id"),

            f.col("referrer_url").alias(
                "log_referrer_url"
            ),

            f.col("current_url").alias(
                "log_current_url"
            ),

            f.to_timestamp(
                "local_time",
                "yyyy-MM-dd HH:mm:ss"
            ).alias(
                "log_local_time"
            ),

            f.from_unixtime(
                f.col("time_stamp") / 1000
            ).cast("timestamp").alias(
                "log_timestamp"
            )
        )
    )
    return fact_df
