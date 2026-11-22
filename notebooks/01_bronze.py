# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: keep the source data
# MAGIC
# MAGIC Bronze is our landing layer. We do not repair source values here because
# MAGIC a faithful first copy lets us repeat and explain later transformations.
# MAGIC Run this after the raw Volume contains all Olist CSVs and both API folders.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "logistics"
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

# These are the real filenames from the Olist Kaggle download. We list them
# explicitly so a typo or missing upload fails with a helpful message.
olist_files = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

def save_bronze(df, table_name):
    """Add audit columns, then replace the table for safe reruns."""
    final_df = (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_name", F.lit(table_name))
    )
    (final_df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_{table_name}"))
    return final_df.count()

# CSV values stay strings in Bronze. Silver decides the correct types.
counts = {}
for table_name, filename in olist_files.items():
    path = f"{RAW}/olist/{filename}"
    try:
        raw_df = (spark.read.option("header", "true")
                  .option("inferSchema", "false")
                  .option("encoding", "UTF-8")
                  .option("multiLine", "true")
                  .csv(path))
        if not raw_df.columns:
            raise ValueError("the file has no header row")
        counts[table_name] = save_bronze(raw_df, table_name)
    except Exception as exc:
        raise RuntimeError(
            f"Could not load {path}. Upload the original Olist CSV with this exact name. "
            f"Details: {exc}"
        ) from exc

# Each API file is one JSON document. wholetext keeps the complete provider
# response in one Bronze raw_json value, including the holiday API's top-level
# array. The source filename preserves state/year context.
for table_name, folder in [("weather", "weather"), ("holidays", "holidays")]:
    path = f"{RAW}/{folder}/*.json"
    try:
        raw_df = (spark.read.option("wholetext", "true").text(path)
                  .withColumnRenamed("value", "raw_json")
                  .withColumn("source_file", F.input_file_name()))
        if not raw_df.columns:
            raise ValueError("no JSON records were found")
        counts[table_name] = save_bronze(raw_df, table_name)
    except Exception as exc:
        raise RuntimeError(
            f"Could not load JSON from {RAW}/{folder}. Upload the API JSON files first. "
            f"Details: {exc}"
        ) from exc

print("Bronze rows written (one table per source):")
for name, row_count in counts.items():
    print(f"bronze_{name}: {row_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this cell should show
# MAGIC
# MAGIC Nine Olist table counts plus weather and holiday counts. A repeat run
# MAGIC replaces each Bronze table; it does not append a second copy.
