from etl.extract.kafka_producer import read_raw_stream
from etl.transform.dim_browser import build_dim_browser
from etl.transform.dim_product import build_dim_product


def run_pipeline(spark, kafka_conf, input_conf):
    queries = []

    raw_df = read_raw_stream(spark,kafka_conf)
    #raw_df.printSchema()
    browser_df = build_dim_browser(raw_df)
    product_file_path = input_conf["product_path"]
    product_df = build_dim_product(spark, raw_df, product_file_path)
    
    #dim_product
    print("DIM_PRODUCT")
    product_query = (
    product_df.writeStream
    .queryName("dim_product")
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 20)
    .trigger(processingTime="20 seconds")
    .start()
)

    #dim_browser
    print("DIM_BROWSER")
    browser_query = (
        browser_df.writeStream
        .queryName("dim_browser")
        .format("console")
        .outputMode("append") 
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="20 seconds")
        .start()
    )
    return [
    browser_query,
    product_query]