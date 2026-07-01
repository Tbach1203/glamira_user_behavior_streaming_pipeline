from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType
from schema.schema_raw_data import raw_log_schema

def read_raw_stream(spark, kafka_conf):
    kafka_df = (
        spark.readStream
        .format("kafka")
        .options(**kafka_conf)
        .load()
    )
    raw_df = (
        kafka_df
        .select(
            from_json(
                col("value").cast(StringType()),
                raw_log_schema
            ).alias("data")
        )
        .select("data.*")
    )
    return raw_df