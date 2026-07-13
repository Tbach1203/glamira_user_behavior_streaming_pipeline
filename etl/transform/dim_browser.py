import pyspark.sql.functions as f

from utils.parse_user_agent import parse_browser_udf


def build_dim_browser(raw_df):
    browser_df = (raw_df
            .dropDuplicates(["user_agent"])
            .filter(f.col("user_agent").isNotNull()) 
            .withColumn("browser",parse_browser_udf("user_agent"))
            .select("browser.*")
            .withColumn("browser_key",f.sha2(f.col("user_agent"),256))
            .select(
                "browser_key",
                "user_agent",
                "browser_name",
                "browser_version",
                "os_name",
                "os_version",
                "device_family",
                "device_type"
            )
        )
    return browser_df