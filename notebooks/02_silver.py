# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: make the records usable
# MAGIC
# MAGIC Silver standardizes the source types, removes repeated source keys, joins
# MAGIC the order facts to the requested dimensions and labels questionable rows.
# MAGIC The fact table is at order-and-seller grain: an order with two sellers has
# MAGIC two rows, so dashboard totals use distinct order IDs.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, StringType, StructField, StructType,
)

CATALOG, SCHEMA = "workspace", "logistics"
DB = f"{CATALOG}.{SCHEMA}"

def read_bronze(name):
    return spark.table(f"{DB}.bronze_{name}")

def clean_text(column_name):
    """Normalize human-readable places without changing business identifiers."""
    return F.upper(F.trim(F.col(column_name)))

def parse_timestamp(column_name):
    # Kaggle timestamps contain local date/time text; keep timestamps, and do
    # not pretend they include a timezone offset that the source never gave.
    return F.to_timestamp(F.col(column_name), "yyyy-MM-dd HH:mm:ss")

# Read known primary keys, keeping one row for a repeated source key.
orders = (read_bronze("orders")
    .select(
        "order_id", "customer_id", "order_status",
        parse_timestamp("order_purchase_timestamp").alias("order_purchase_ts"),
        parse_timestamp("order_approved_at").alias("order_approved_ts"),
        parse_timestamp("order_delivered_carrier_date").alias("order_carrier_ts"),
        parse_timestamp("order_delivered_customer_date").alias("order_delivered_ts"),
        F.to_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss").alias("order_estimated_ts"),
    ).dropDuplicates(["order_id"]))

customers = (read_bronze("customers")
    .select(
        "customer_id",
        clean_text("customer_city").alias("customer_city"),
        clean_text("customer_state").alias("customer_state"),
        F.col("customer_zip_code_prefix").cast("int").alias("customer_zip_prefix"),
    ).dropDuplicates(["customer_id"]))

sellers = (read_bronze("sellers")
    .select(
        "seller_id",
        clean_text("seller_city").alias("seller_city"),
        clean_text("seller_state").alias("seller_state"),
        F.col("seller_zip_code_prefix").cast("int").alias("seller_zip_prefix"),
    ).dropDuplicates(["seller_id"]))

products = (read_bronze("products")
    .select(
        "product_id",
        F.col("product_weight_g").cast("double").alias("product_weight_g"),
    ).dropDuplicates(["product_id"]))

items = (read_bronze("order_items")
    .select(
        "order_id", F.col("order_item_id").cast("int").alias("order_item_id"),
        "product_id", "seller_id",
        F.to_timestamp("shipping_limit_date", "yyyy-MM-dd HH:mm:ss").alias("shipping_limit_ts"),
        F.col("price").cast("double").alias("price"),
        F.col("freight_value").cast("double").alias("freight_value"),
    ).dropDuplicates(["order_id", "order_item_id"])
    .join(products, "product_id", "left"))

# Collapse item rows to order/seller grain. This retains seller attribution while
# preventing repeated payment and review records from multiplying each item.
item_seller = (items.groupBy("order_id", "seller_id")
    .agg(
        F.countDistinct("order_item_id").alias("item_count"),
        F.sum("price").alias("item_price"),
        F.sum("freight_value").alias("freight_value"),
        F.sum("product_weight_g").alias("weight_g"),
        F.max("shipping_limit_ts").alias("shipping_limit_ts"),
    ))

payments = (read_bronze("order_payments")
    .select(
        "order_id",
        F.col("payment_value").cast("double").alias("payment_value"),
        F.col("payment_installments").cast("int").alias("payment_installments"),
        "payment_type",
    ).groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("payment_value"),
        F.max("payment_installments").alias("payment_installments_max"),
        F.concat_ws(", ", F.sort_array(F.collect_set("payment_type"))).alias("payment_types"),
    ))

reviews = (read_bronze("order_reviews")
    .select("order_id", F.col("review_score").cast("int").alias("review_score"))
    .groupBy("order_id")
    .agg(
        F.avg("review_score").alias("review_score_avg"),
        F.count("review_score").alias("review_count"),
    ))

# A ZIP-prefix centroid reduces repeated geolocation records to one point per
# prefix, preventing location joins from multiplying shipment rows.
geo = (read_bronze("geolocation")
    .select(
        F.col("geolocation_zip_code_prefix").cast("int").alias("zip_prefix"),
        F.col("geolocation_lat").cast("double").alias("latitude"),
        F.col("geolocation_lng").cast("double").alias("longitude"),
    ).where(F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180))
    .groupBy("zip_prefix")
    .agg(F.avg("latitude").alias("latitude"), F.avg("longitude").alias("longitude")))

# Expand each daily weather JSON array into one row per date. State comes from
# the API response filename (weather_SP.json), not a guessed data column.
weather_schema = StructType([
    StructField("daily", StructType([
        StructField("time", ArrayType(StringType()), True),
        StructField("precipitation_sum", ArrayType(DoubleType()), True),
        StructField("temperature_2m_max", ArrayType(DoubleType()), True),
        StructField("wind_speed_10m_max", ArrayType(DoubleType()), True),
    ]), True),
])
weather_raw = (read_bronze("weather")
    .withColumn("api_json", F.from_json("raw_json", weather_schema)))
weather_arrays = F.arrays_zip(
    F.col("api_json.daily.time").alias("time"),
    F.col("api_json.daily.precipitation_sum").alias("precipitation_sum"),
    F.col("api_json.daily.temperature_2m_max").alias("temperature_2m_max"),
    F.col("api_json.daily.wind_speed_10m_max").alias("wind_speed_10m_max"),
)
weather = (weather_raw
    .withColumn("day", F.explode(weather_arrays))
    .select(
        F.upper(F.regexp_extract("source_file", r"weather_([A-Z]{2})\.json$", 1)).alias("weather_state"),
        F.to_date(F.col("day.time")).alias("weather_date"),
        F.col("day.precipitation_sum").cast("double").alias("precipitation_mm"),
        F.col("day.temperature_2m_max").cast("double").alias("max_temperature_c"),
        F.col("day.wind_speed_10m_max").cast("double").alias("max_wind_kmh"),
    ).dropDuplicates(["weather_state", "weather_date"]))

# Nager.Date v4 returns a JSON array per yearly file. Keep the fields used here
# and create a small three-day-before/after holiday window.
holiday_record_schema = StructType([
    StructField("date", StringType(), True),
    StructField("name", StringType(), True),
    StructField("countryCode", StringType(), True),
    StructField("global", BooleanType(), True),
    StructField("counties", ArrayType(StringType()), True),
])
holiday_raw = (read_bronze("holidays")
    .withColumn("api_holidays", F.from_json("raw_json", ArrayType(holiday_record_schema))))
holidays = (holiday_raw
    .withColumn("holiday", F.explode("api_holidays"))
    .select(
        F.to_date("holiday.date").alias("holiday_date"),
        F.col("holiday.name").alias("holiday_name"),
        F.col("holiday.countryCode").alias("country_code"),
        F.col("holiday.global").cast("boolean").alias("national_holiday"),
        F.col("holiday.counties").alias("subdivision_codes"),
    ).where(F.col("country_code") == "BR")
    .dropDuplicates(["holiday_date", "holiday_name", "national_holiday", "subdivision_codes"]))

# Apply national holidays to every state, but state holidays only to the state
# named in Nager's ISO subdivision code (for example BR-SP).
brazilian_states = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]
holiday_state_array = F.when(
    F.col("national_holiday"),
    F.array(*[F.lit(state) for state in brazilian_states]),
).otherwise(
    F.transform(
        F.col("subdivision_codes"),
        lambda code: F.regexp_extract(code, r"BR-([A-Z]{2})", 1),
    )
)
holiday_dates = (holidays
    .withColumn("customer_state", F.explode(holiday_state_array))
    .where(F.length("customer_state") == 2)
    .select("holiday_date", "customer_state")
    .distinct())
holiday_matches = holiday_dates.select(
    F.col("holiday_date").alias("delivery_date"), "customer_state"
)
holiday_windows = (holiday_dates
    .withColumn("window_date", F.explode(F.sequence(
        F.date_sub("holiday_date", 3), F.date_add("holiday_date", 3)
    )))
    .select(
        "customer_state",
        F.col("window_date").alias("delivery_date"),
    ).distinct())

# Join ZIP-prefix averages to customers and sellers before joining to orders.
customer_geo = customers.join(
    geo.select(
        F.col("zip_prefix").alias("customer_geo_zip"),
        F.col("latitude").alias("customer_latitude"),
        F.col("longitude").alias("customer_longitude"),
    ),
    customers.customer_zip_prefix == F.col("customer_geo_zip"), "left"
)
seller_geo = sellers.join(
    geo.select(
        F.col("zip_prefix").alias("seller_geo_zip"),
        F.col("latitude").alias("seller_latitude"),
        F.col("longitude").alias("seller_longitude"),
    ),
    sellers.seller_zip_prefix == F.col("seller_geo_zip"), "left"
)

fact = (item_seller
    .join(orders, "order_id", "left")
    .join(customer_geo, "customer_id", "left")
    .join(seller_geo, "seller_id", "left")
    .join(payments, "order_id", "left")
    .join(reviews, "order_id", "left")
    .withColumn("delivery_date", F.to_date("order_delivered_ts"))
    .withColumn("delivery_days", F.datediff("delivery_date", F.to_date("order_purchase_ts")).cast("int"))
    .withColumn("estimated_days", F.datediff(F.to_date("order_estimated_ts"), F.to_date("order_purchase_ts")).cast("int"))
    .withColumn("delay_days", F.datediff("delivery_date", F.to_date("order_estimated_ts")).cast("int"))
    .withColumn(
        "is_late",
        F.when(F.col("delivery_date").isNull() | F.col("order_estimated_ts").isNull(), F.lit(None).cast("int"))
         .when(F.col("delay_days") > 0, F.lit(1)).otherwise(F.lit(0)),
    )
    .withColumn(
        "approval_hours",
        (F.unix_timestamp("order_approved_ts") - F.unix_timestamp("order_purchase_ts")) / 3600.0,
    )
    .withColumn(
        "seller_dispatch_days",
        F.datediff(F.to_date("order_carrier_ts"), F.to_date("order_approved_ts")).cast("int"),
    )
    .withColumn("freight_ratio", F.when(F.col("item_price") > 0, F.col("freight_value") / F.col("item_price")))
    .withColumn("weight_kg", F.col("weight_g") / 1000.0)
)

# Haversine is a useful straight-line estimate, not road distance.
radians = F.lit(0.017453292519943295)
lat_delta = (F.col("customer_latitude") - F.col("seller_latitude")) * radians
lon_delta = (F.col("customer_longitude") - F.col("seller_longitude")) * radians
hav_a = (
    F.pow(F.sin(lat_delta / 2), 2)
    + F.cos(F.col("seller_latitude") * radians)
    * F.cos(F.col("customer_latitude") * radians)
    * F.pow(F.sin(lon_delta / 2), 2)
)
fact = fact.withColumn(
    "approx_distance_km",
    F.when(
        F.col("customer_latitude").isNotNull() & F.col("seller_latitude").isNotNull(),
        2.0 * 6371.0 * F.asin(F.sqrt(F.least(F.lit(1.0), hav_a))),
    ),
)

fact = (fact
    .join(weather, (fact.customer_state == weather.weather_state) & (fact.delivery_date == weather.weather_date), "left")
    .drop("weather_state", "weather_date")
    .join(holiday_matches.withColumn("is_holiday", F.lit(1)), ["delivery_date", "customer_state"], "left")
    .join(holiday_windows.withColumn("is_holiday_week", F.lit(1)), ["delivery_date", "customer_state"], "left")
    .withColumn("is_holiday", F.coalesce("is_holiday", F.lit(0)))
    .withColumn("is_holiday_week", F.coalesce("is_holiday_week", F.lit(0)))
)

# Quality is visible and repeatable. A failing row goes into quarantine as JSON
# so the team can inspect it without losing the original detail.
fact = fact.withColumn(
    "quality_reasons",
    F.concat_ws(
        "|",
        F.when(F.col("order_id").isNull(), "missing_order_id"),
        F.when(F.col("order_purchase_ts").isNull(), "missing_purchase_timestamp"),
        F.when(F.col("customer_state").isNull(), "missing_customer_dimension"),
        F.when(F.col("seller_state").isNull(), "missing_seller_dimension"),
        F.when(F.col("delivery_days") < 0, "negative_delivery_days"),
        F.when(F.col("approval_hours") < 0, "negative_approval_hours"),
        F.when(F.col("seller_dispatch_days") < 0, "negative_seller_dispatch_days"),
        F.when(F.col("review_score_avg").isNotNull() & ~F.col("review_score_avg").between(1, 5), "review_out_of_range"),
    ),
)
invalid_fact = fact.where(F.length("quality_reasons") > 0)
quarantine_fact = invalid_fact.select(
    F.lit("silver_shipments").alias("source_table"),
    F.coalesce("order_id", F.lit("UNKNOWN")).alias("record_key"),
    F.explode(F.split("quality_reasons", r"\|")).alias("check_name"),
    F.col("quality_reasons").alias("reason"),
    F.to_json(F.struct(*[F.col(c) for c in fact.columns if c != "quality_reasons"])).alias("record_json"),
    F.current_timestamp().alias("checked_at"),
)

# This child-table check catches order items whose parent order is absent.
orphan_items = (items.select("order_id", "order_item_id")
    .join(orders.select("order_id"), "order_id", "left_anti")
    .select(
        F.lit("bronze_order_items").alias("source_table"),
        F.concat_ws(":", "order_id", F.col("order_item_id").cast("string")).alias("record_key"),
        F.lit("orphan_order_item").alias("check_name"),
        F.lit("order_id is missing from bronze_orders").alias("reason"),
        F.to_json(F.struct("order_id", "order_item_id")).alias("record_json"),
        F.current_timestamp().alias("checked_at"),
    ))

# order_id alone repeats by design, so duplicates are checked using the item key.
bronze_items = read_bronze("order_items")
duplicate_keys = (bronze_items.groupBy("order_id", "order_item_id")
    .count().where(F.col("count") > 1).select("order_id", "order_item_id"))
duplicate_items = duplicate_keys.count()
duplicate_item_rows = (bronze_items.join(duplicate_keys, ["order_id", "order_item_id"], "inner")
    .select(
        F.lit("bronze_order_items").alias("source_table"),
        F.concat_ws(":", "order_id", "order_item_id").alias("record_key"),
        F.lit("duplicate_order_item_key").alias("check_name"),
        F.lit("order_id and order_item_id appear more than once").alias("reason"),
        F.to_json(F.struct(*bronze_items.columns)).alias("record_json"),
        F.current_timestamp().alias("checked_at"),
    ))
quarantine = (quarantine_fact
    .unionByName(orphan_items)
    .unionByName(duplicate_item_rows))

(fact.drop("quality_reasons").write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{DB}.silver_shipments"))
(quarantine.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{DB}.silver_quarantine"))

checks = [
    ("missing_order_id", fact.where(F.col("order_id").isNull()).count()),
    ("missing_purchase_timestamp", fact.where(F.col("order_purchase_ts").isNull()).count()),
    ("missing_customer_dimension", fact.where(F.col("customer_state").isNull()).count()),
    ("missing_seller_dimension", fact.where(F.col("seller_state").isNull()).count()),
    ("negative_delivery_days", fact.where(F.col("delivery_days") < 0).count()),
    ("negative_approval_hours", fact.where(F.col("approval_hours") < 0).count()),
    ("negative_seller_dispatch_days", fact.where(F.col("seller_dispatch_days") < 0).count()),
    ("review_out_of_range", fact.where(
        F.col("review_score_avg").isNotNull() & ~F.col("review_score_avg").between(1, 5)
    ).count()),
    ("orphan_order_item", orphan_items.count()),
    ("duplicate_order_item_key", duplicate_items),
    ("missing_weather_match", fact.where(F.col("delivery_date").isNotNull() & F.col("precipitation_mm").isNull()).count()),
]
quality_summary = spark.createDataFrame(
    [(name, int(count), "PASS" if count == 0 else "REVIEW") for name, count in checks],
    ["check_name", "failing_rows", "status"],
).withColumn("checked_at", F.current_timestamp())
(quality_summary.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{DB}.silver_quality_summary"))

print(f"Silver shipment rows: {fact.count():,}")
print(f"Quarantined check failures: {quarantine.count():,}")
display(quality_summary.orderBy("status", "check_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Important interpretation notes
# MAGIC
# MAGIC Delivery days are calendar days from purchase to customer delivery.
# MAGIC Delay days compare actual delivery date with the estimated date. A missing
# MAGIC actual delivery date has no late flag. Distances are straight-line
# MAGIC approximations based on ZIP-prefix averages. Weather is one capital point
# MAGIC for each state, so it is contextual and not a shipment-level measurement.
