from etl.extract.kafka_producer import read_raw_stream
from etl.transform.dim_browser import build_dim_browser
from etl.transform.dim_product import build_dim_product
from etl.transform.dim_location import build_dim_location
from etl.transform.dim_date import build_dim_date
from etl.transform.fact_view_log import build_fact_view_log
from etl.load.postgres_loader import write_dimension, write_fact

def process_batch(spark, batch_df, batch_id, input_conf, postgres_conf):
    print(f"PROCESS BATCH {batch_id}")

    #dim_browser
    browser_df = build_dim_browser(batch_df)
    write_dimension(spark, browser_df, postgres_conf, "dim_browser", "browser_key")


    #dim_product
    product_df = build_dim_product(spark, batch_df, input_conf["product_path"])
    write_dimension(spark, product_df, postgres_conf,"dim_product", "product_key")

    #dim_location
    dim_location_df, location_lookup_df = build_dim_location(spark, batch_df, input_conf["location_path"])
    write_dimension(spark, dim_location_df, postgres_conf, "dim_location", "location_key")

    #dim_date
    date_df = build_dim_date(batch_df)
    write_dimension(spark, date_df, postgres_conf, "dim_date", "date_key")

    fact_df = build_fact_view_log(batch_df,browser_df, product_df, location_lookup_df, date_df)
    write_fact(fact_df, postgres_conf)

    print(f"Batch {batch_id} completed.")

def run_pipeline(spark, kafka_conf, input_conf, postgres_conf):
    raw_df = read_raw_stream(spark, kafka_conf)

    query = (
        raw_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id:
                process_batch(spark, batch_df, batch_id, input_conf, postgres_conf)
        )
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    return query