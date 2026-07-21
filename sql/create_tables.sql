CREATE TABLE IF NOT EXISTS dim_browser(
    browser_key varchar(64) PRIMARY KEY,
    user_agent text,
    browser_name varchar(100),
    browser_version varchar(50),
    os_name varchar(100),
    os_version varchar(50),
    device_family varchar(100),
    device_type varchar(50)
);

CREATE TABLE IF NOT EXISTS dim_product(
    product_key varchar(64) PRIMARY KEY,
    product_id bigint,
    product_name text,
    product_price numeric
);

CREATE TABLE IF NOT EXISTS dim_location(
    location_key varchar(64) PRIMARY KEY,
    country_name varchar(100),
    region_name varchar(100),
    city_name varchar(100)
);

CREATE TABLE IF NOT EXISTS dim_date(
    date_key bigint PRIMARY KEY,
    full_date date,
    hour integer,
    day integer,
    month integer,
    year integer,
    day_name varchar(20),
    month_name varchar(20)
);

CREATE TABLE IF NOT EXISTS fact_view_log(
    view_log_key      VARCHAR(64) PRIMARY KEY,

    product_key       VARCHAR(64),
    browser_key       VARCHAR(64),
    location_key      VARCHAR(64),
    date_key          BIGINT,

    store_id          BIGINT,

    log_referrer_url  TEXT,
    log_current_url   TEXT,

    log_local_time    TIMESTAMP,
    log_timestamp     TIMESTAMP,

    CONSTRAINT fk_product
        FOREIGN KEY(product_key)
        REFERENCES dim_product(product_key),

    CONSTRAINT fk_browser
        FOREIGN KEY(browser_key)
        REFERENCES dim_browser(browser_key),

    CONSTRAINT fk_location
        FOREIGN KEY(location_key)
        REFERENCES dim_location(location_key),

    CONSTRAINT fk_date
        FOREIGN KEY(date_key)
        REFERENCES dim_date(date_key)
);
