import pyspark.sql.functions as f

def lookup_browser(raw_df, browser_df):
    return (
        raw_df.join(
            f.broadcast(
                browser_df.select(
                    "user_agent",
                    "browser_key"
                )
            ),
            on="user_agent",
            how="left"
        )
    )

def lookup_product(raw_df, product_df):
    return (
        raw_df.join(
            product_df.select(
                "product_id",
                "product_key"
            ),
            on="product_id",
            how="left"
        )
    )

def lookup_location(raw_df, location_df):
    return (
        raw_df.join(
            location_df.select(
                "ip",
                "location_key"
            ),
            on="ip",
            how="left"
        )
    )

def lookup_date(raw_df, date_df):
    return (
        raw_df
        .withColumn(
            "date_key",
            f.date_format(
                f.to_timestamp(
                    "local_time",
                    "yyyy-MM-dd HH:mm:ss"
                ),
                "yyyyMMddHH"
            ).cast("long")
        )
        .join(
            f.broadcast(
                date_df.select(
                    "date_key"
                )
            ),
            on="date_key",
            how="left"
        )
    )