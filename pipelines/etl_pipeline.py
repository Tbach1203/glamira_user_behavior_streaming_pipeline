from etl.extract.kafka_producer import read_raw_stream
from etl.transform.dim_browser import build_dim_browser


def run_pipeline(spark, kafka_conf):
    raw_df = read_raw_stream(
        spark,
        kafka_conf
    )
    browser_df = build_dim_browser(
        raw_df
    )
    #raw_df.printSchema()
    query = (
        browser_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="30 seconds")
        .start()
    )
    return query