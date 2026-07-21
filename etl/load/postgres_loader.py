import pyspark.sql.functions as f


def read_table(spark, postgres_conf, table_name):
    return (
        spark.read
        .format("jdbc")
        .option("driver", postgres_conf["driver"])
        .option("url", postgres_conf["url"])
        .option("dbtable", table_name)
        .option("user", postgres_conf["user"])
        .option("password", postgres_conf["password"])
        .load()
    )

def append_table(df, postgres_conf, table_name):
    (
        df
        .coalesce(1)                # tránh nhiều executor ghi cùng lúc
        .write
        .format("jdbc")
        .option("driver", postgres_conf["driver"])
        .option("url", postgres_conf["url"])
        .option("dbtable", table_name)
        .option("user", postgres_conf["user"])
        .option("password", postgres_conf["password"])
        .mode("append")
        .save()
    )

def write_dimension(spark, df, postgres_conf, table_name, key_column):
    # loại duplicate trong batch
    df = df.dropDuplicates([key_column])

    existed_df = (
        read_table(spark, postgres_conf, table_name)
        .select(key_column)
    )

    new_df = (
        df.join(
            existed_df,
            key_column,
            "left_anti"
        )
    )

    append_table(new_df, postgres_conf, table_name)
    return new_df

def write_fact(df, postgres_conf, table_name="fact_view_log"):
    fact_df = (df.dropDuplicates(["view_log_key"]))

    if not fact_df.rdd.isEmpty():
        append_table(fact_df, postgres_conf, table_name)

    return fact_df