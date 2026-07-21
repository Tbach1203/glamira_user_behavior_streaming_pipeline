import os
from pyspark.sql import SparkSession
import config.config as conf
from config.logger import Log4j
from pipelines.etl_pipeline import run_pipeline

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    conf_path = base_dir + "/spark.conf"
    # conf_path = base_dir + "/spark.local.conf"
    config = conf.Config(conf_path)

    spark = (
        SparkSession.builder
        .config(conf=config.spark_conf)
        .getOrCreate()
    )

    log = Log4j(spark)
    log.info("Spark application started.")

    query = run_pipeline(spark,config.kafka_conf,config.input_conf, config.postgres_conf)
    query.awaitTermination()