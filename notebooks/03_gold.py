# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: shape the dashboard tables
# MAGIC
# MAGIC Gold tables contain business summaries. We overwrite them on every run so
# MAGIC reruns do not double-count. Counts use distinct orders because Silver can
# MAGIC have multiple rows for a multi-seller order.
# MAGIC
# MAGIC Olist does not identify a carrier. The freight table compares freight-value
# MAGIC and distance bands; it must not be described as carrier performance.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "logistics"
DB = f"{CATALOG}.{SCHEMA}"
shipments = spark.table(f"{DB}.silver_shipments")

def save_gold(df, table_name, comment):
    (df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{DB}.{table_name}"))
    escaped_comment = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {DB}.{table_name} IS '{escaped_comment}'")

order_level = (shipments.groupBy("order_id")
    .agg(
        F.max("is_late").alias("is_late"),
        F.max("delivery_days").alias("delivery_days"),
        F.max("estimated_days").alias("estimated_days"),
        F.max("delay_days").alias("delay_days"),
        F.sum("freight_value").alias("freight_value"),
        F.sum("weight_kg").alias("weight_kg"),
        F.max("approx_distance_km").alias("approx_distance_km"),
        F.max("review_score_avg").alias("review_score_avg"),
        F.max("delivery_date").alias("delivery_date"),
        F.max("is_holiday").alias("is_holiday"),
        F.max("is_holiday_week").alias("is_holiday_week"),
    ))

kpi = order_level.agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.countDistinct(F.when(F.col("is_late").isNotNull(), F.col("order_id"))).alias("delivered_orders"),
    F.countDistinct(F.when(F.col("is_late") == 0, F.col("order_id"))).alias("on_time_orders"),
    F.countDistinct(F.when(F.col("is_late") == 1, F.col("order_id"))).alias("late_orders"),
    F.avg("delivery_days").alias("avg_delivery_days"),
    F.avg(F.when(F.col("is_late") == 1, F.col("delay_days"))).alias("avg_delay_days"),
    F.avg("freight_value").alias("avg_order_freight"),
    F.avg("review_score_avg").alias("avg_review_score"),
).withColumn(
    "on_time_pct",
    F.when(F.col("delivered_orders") > 0, F.col("on_time_orders") / F.col("delivered_orders")),
)
save_gold(kpi, "gold_kpi_summary", "One-row overall logistics KPIs; orders are distinct and on-time rate uses delivered orders.")

def grouped_delivery_metrics(source_df, group_columns):
    result = source_df.groupBy(*group_columns).agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(F.when(F.col("is_late").isNotNull(), F.col("order_id"))).alias("delivered_orders"),
        F.countDistinct(F.when(F.col("is_late") == 1, F.col("order_id"))).alias("late_orders"),
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.avg(F.when(F.col("is_late") == 1, F.col("delay_days"))).alias("avg_delay_days"),
        F.avg("freight_value").alias("avg_freight_value"),
    )
    return result.withColumn(
        "on_time_pct",
        F.when(F.col("delivered_orders") > 0,
               (F.col("delivered_orders") - F.col("late_orders")) / F.col("delivered_orders")),
    )

route_source = (shipments.groupBy("order_id", "seller_state", "customer_state")
    .agg(
        F.max("is_late").alias("is_late"),
        F.max("delivery_days").alias("delivery_days"),
        F.max("delay_days").alias("delay_days"),
        F.sum("freight_value").alias("freight_value"),
    ))
route = grouped_delivery_metrics(route_source, ["seller_state", "customer_state"])
save_gold(route, "gold_otd_by_route", "Delivery metrics by seller state to customer state route. OTD means on-time delivery.")

freight_bands = (shipments
    .withColumn(
        "freight_band",
        F.when(F.col("freight_value").isNull(), "Unknown")
         .when(F.col("freight_value") < 10, "Under 10")
         .when(F.col("freight_value") < 25, "10 to 24.99")
         .when(F.col("freight_value") < 50, "25 to 49.99")
         .otherwise("50 or more"),
    )
    .withColumn(
        "weight_band",
        F.when(F.col("weight_kg").isNull(), "Unknown")
         .when(F.col("weight_kg") < 1, "Under 1 kg")
         .when(F.col("weight_kg") < 5, "1 to 4.99 kg")
         .when(F.col("weight_kg") < 10, "5 to 9.99 kg")
         .otherwise("10 kg or more"),
    )
    .withColumn(
        "distance_band",
        F.when(F.col("approx_distance_km").isNull(), "Unknown")
         .when(F.col("approx_distance_km") < 100, "Under 100 km")
         .when(F.col("approx_distance_km") < 500, "100 to 499 km")
         .when(F.col("approx_distance_km") < 1000, "500 to 999 km")
         .otherwise("1000 km or more"),
    ))
freight = (freight_bands.groupBy("freight_band", "weight_band", "distance_band")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(F.when(F.col("is_late").isNotNull(), F.col("order_id"))).alias("delivered_orders"),
        F.countDistinct(F.when(F.col("is_late") == 1, F.col("order_id"))).alias("late_orders"),
        F.avg("freight_value").alias("avg_freight_value"),
        F.avg("freight_ratio").alias("avg_freight_ratio"),
        F.avg("approx_distance_km").alias("avg_distance_km"),
        F.avg("weight_kg").alias("avg_weight_kg"),
        F.avg("delivery_days").alias("avg_delivery_days"),
    ).withColumn(
        "on_time_pct",
        F.when(F.col("delivered_orders") > 0,
               (F.col("delivered_orders") - F.col("late_orders")) / F.col("delivered_orders")),
    ))
save_gold(freight, "gold_carrier_or_freight_performance", "Freight-value, product-weight and estimated-distance band comparison; Olist has no carrier identifier, so this is not carrier performance.")

seller = (shipments.groupBy("seller_id", "seller_state", "seller_city")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(F.when(F.col("is_late").isNotNull(), F.col("order_id"))).alias("delivered_orders"),
        F.countDistinct(F.when(F.col("is_late") == 1, F.col("order_id"))).alias("late_orders"),
        F.avg("seller_dispatch_days").alias("avg_seller_dispatch_days"),
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.avg(F.when(F.col("is_late") == 1, F.col("delay_days"))).alias("avg_delay_days"),
        F.avg("review_score_avg").alias("avg_review_score"),
        F.avg("freight_value").alias("avg_freight_value"),
    ).withColumn(
        "on_time_pct",
        F.when(F.col("delivered_orders") > 0,
               (F.col("delivered_orders") - F.col("late_orders")) / F.col("delivered_orders")),
    ))
save_gold(seller, "gold_seller_performance", "Seller delivery performance; seller is a warehouse proxy, not a measured warehouse facility.")

order_analysis = (shipments.groupBy("order_id")
    .agg(
        F.max("is_late").alias("is_late"),
        F.max("delay_days").alias("delay_days"),
        F.max("delivery_days").alias("delivery_days"),
        F.max("delivery_date").alias("delivery_date"),
        F.max("precipitation_mm").alias("precipitation_mm"),
        F.max("approx_distance_km").alias("approx_distance_km"),
        F.max("is_holiday_week").alias("is_holiday_week"),
        F.sum("freight_value").alias("freight_value"),
        F.max("review_score_avg").alias("review_score_avg"),
    ))

root_base = (order_analysis
    .withColumn(
        "rain_band",
        F.when(F.col("precipitation_mm").isNull(), "Unknown")
         .when(F.col("precipitation_mm") == 0, "No rain")
         .when(F.col("precipitation_mm") < 10, "Light rain")
         .otherwise("Heavy rain"),
    )
    .withColumn(
        "distance_band",
        F.when(F.col("approx_distance_km").isNull(), "Unknown")
         .when(F.col("approx_distance_km") < 100, "Under 100 km")
         .when(F.col("approx_distance_km") < 500, "100 to 499 km")
         .when(F.col("approx_distance_km") < 1000, "500 to 999 km")
         .otherwise("1000 km or more"),
    )
    .withColumn("holiday_week", F.when(F.col("is_holiday_week") == 1, "Within 3 days of holiday").otherwise("Other days"))
    .withColumn("month", F.date_format("delivery_date", "yyyy-MM"))
    .withColumn(
        "season",
        F.when(F.month("delivery_date").isin(12, 1, 2), "Summer")
         .when(F.month("delivery_date").isin(3, 4, 5), "Autumn")
         .when(F.month("delivery_date").isin(6, 7, 8), "Winter")
         .when(F.month("delivery_date").isin(9, 10, 11), "Spring")
         .otherwise("Unknown"),
    ))

def cause_summary(source_df, cause_type, cause_column):
    # Use two grouped outputs so the average is computed only over late fact rows,
    # while order counts and OTD use unique delivered orders.
    counts = source_df.groupBy(F.lit(cause_type).alias("cause_type"), F.col(cause_column).alias("cause_band")).agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(F.when(F.col("is_late").isNotNull(), F.col("order_id"))).alias("delivered_orders"),
        F.countDistinct(F.when(F.col("is_late") == 1, F.col("order_id"))).alias("late_orders"),
    )
    delays = (source_df.where(F.col("is_late") == 1)
        .groupBy(F.lit(cause_type).alias("cause_type"), F.col(cause_column).alias("cause_band"))
        .agg(F.avg("delay_days").alias("avg_delay_days")))
    return (counts.join(delays, ["cause_type", "cause_band"], "left")
        .withColumn(
            "on_time_pct",
            F.when(F.col("delivered_orders") > 0,
                   (F.col("delivered_orders") - F.col("late_orders")) / F.col("delivered_orders")),
        ))

root_causes = (
    cause_summary(root_base, "Rain", "rain_band")
    .unionByName(cause_summary(root_base, "Distance", "distance_band"))
    .unionByName(cause_summary(root_base, "Holiday week", "holiday_week"))
    .unionByName(cause_summary(root_base, "Month", "month"))
    .unionByName(cause_summary(root_base, "Brazil season", "season"))
)
save_gold(root_causes, "gold_delay_root_causes", "Delay summaries by rain, estimated distance, holiday window, delivery month, and Brazilian season. These are associations.")

monthly = (order_analysis.where(F.col("delivery_date").isNotNull())
    .withColumn("year_month", F.date_format("delivery_date", "yyyy-MM"))
    .groupBy("year_month")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(F.when(F.col("is_late").isNotNull(), F.col("order_id"))).alias("delivered_orders"),
        F.countDistinct(F.when(F.col("is_late") == 1, F.col("order_id"))).alias("late_orders"),
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.avg(F.when(F.col("is_late") == 1, F.col("delay_days"))).alias("avg_delay_days"),
        F.avg("freight_value").alias("avg_freight_value"),
        F.avg("review_score_avg").alias("avg_review_score"),
    ).withColumn(
        "on_time_pct",
        F.when(F.col("delivered_orders") > 0,
               (F.col("delivered_orders") - F.col("late_orders")) / F.col("delivered_orders")),
    ))
save_gold(monthly, "gold_monthly_trends", "Monthly delivery trend summaries based on delivered customer date.")

for table_name in [
    "gold_kpi_summary", "gold_otd_by_route", "gold_carrier_or_freight_performance",
    "gold_seller_performance", "gold_delay_root_causes", "gold_monthly_trends",
]:
    print(f"{table_name}: {spark.table(f'{DB}.{table_name}').count():,} rows")
