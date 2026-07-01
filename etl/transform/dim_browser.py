import pyspark.sql.functions as f
from utils.parse_browser import parse_browser_udf

def build_dim_browser(raw_df):

    browser_df = (
        raw_df
        .filter(f.col("user_agent").isNotNull())
        .withColumn(
            "browser",
            parse_browser_udf("user_agent")
        )
        .select("browser.*")
        .dropDuplicates(["user_agent"])
    )

    return browser_df