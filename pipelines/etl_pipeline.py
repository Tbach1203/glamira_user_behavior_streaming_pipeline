from etl.extract.kafka_producer import read_raw_stream


def run_pipeline(spark, kafka_conf):

    raw_df = read_raw_stream(
        spark,
        kafka_conf
    )

    raw_df.printSchema()

    query = (
        raw_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="30 seconds")
        .start()
    )

    return query