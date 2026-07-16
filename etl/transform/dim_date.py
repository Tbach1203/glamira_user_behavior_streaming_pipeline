import pyspark.sql.functions as f
from schema.schema_dim_date import date_schema

import pyspark.sql.functions as f


def build_dim_date(raw_df):
    date_df = (
        raw_df
        .filter(
            f.col("local_time").isNotNull()
        )
        .withColumn(
            "local_timestamp",
            f.to_timestamp(
                "local_time",
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        .filter(f.col("local_timestamp").isNotNull())
        .withColumn("full_date", f.to_date("local_timestamp"))
        .withColumn("hour",f.hour("local_timestamp"))
        .withColumn("date_key", f.date_format( "local_timestamp","yyyyMMddHH").cast("long"))
        .withColumn("day",f.dayofmonth("local_timestamp"))
        .withColumn("month",f.month("local_timestamp"))
        .withColumn("year",f.year("local_timestamp"))
        .withColumn("day_name",f.date_format("local_timestamp","EEEE"))
        .withColumn("month_name",f.date_format("local_timestamp","MMMM"))
        .dropDuplicates(["date_key"])
        .select(
            "date_key",
            "full_date",
            "hour",
            "day",
            "month",
            "year",
            "day_name",
            "month_name"
        )
    )
    return date_df
