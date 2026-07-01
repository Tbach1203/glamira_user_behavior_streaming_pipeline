from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from user_agents import parse
from schema.schema_dim_browser import browser_schema

def parse_browser(user_agent):
    if user_agent is None:
        return None
    ua = parse(user_agent)
    if ua.is_mobile:
        device_type = "Mobile"
    elif ua.is_tablet:
        device_type = "Tablet"
    elif ua.is_pc:
        device_type = "PC"
    elif ua.is_bot:
        device_type = "Bot"
    else:
        device_type = "Other"

    return (
        user_agent,
        ua.browser.family,
        ua.browser.version_string,
        ua.os.family,
        ua.os.version_string,
        ua.device.family,
        device_type,
    )

parse_browser_udf = udf(parse_browser,browser_schema)