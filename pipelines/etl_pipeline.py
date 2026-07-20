from etl.extract.kafka_producer import read_raw_stream
from etl.transform.dim_browser import build_dim_browser
from etl.transform.dim_product import build_dim_product
from etl.transform.dim_location import build_dim_location
from etl.transform.dim_date import build_dim_date
from etl.transform.fact_view_log import build_fact_view_log

def process_batch(spark, batch_df, batch_id, input_conf):
    print(f"PROCESS BATCH {batch_id}")

    #dim_browser
    browser_df = build_dim_browser(batch_df)
    print("DIM_BROWSER")
    browser_df.show(20,truncate=False)

    #dim_product
    product_df = build_dim_product(spark, batch_df, input_conf["product_path"])
    print("DIM_PRODUCT")
    product_df.show(20, truncate=False)

    #dim_location
    location_df = build_dim_location(spark, batch_df, input_conf["location_path"])
    print("DIM_LOCATION")
    location_df.show(20, truncate=False)

    #dim_date
    date_df = build_dim_date(batch_df)
    print("DIM_DATE")
    date_df.show(truncate=False)

    fact_df = build_fact_view_log(batch_df,browser_df, product_df, location_df, date_df)
    fact_df.show(truncate=False)


def run_pipeline(spark, kafka_conf, input_conf):
    raw_df = read_raw_stream(spark, kafka_conf)

    query = (
        raw_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id:
                process_batch(spark, batch_df, batch_id, input_conf)
        )
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    return query