# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Run the full pipeline
# MAGIC
# MAGIC First check that the Volume contains all raw inputs. Then run Bronze,
# MAGIC Silver and Gold in order. Put this notebook beside notebooks 01, 02 and 03
# MAGIC in the same Databricks workspace folder before using the Run All button.

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "logistics"
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

required_csvs = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]

def list_names(path):
    try:
        return {item.name for item in dbutils.fs.ls(path)}
    except Exception as exc:
        raise RuntimeError(
            f"Cannot open {path}. Create workspace.logistics.raw and upload inputs. Details: {exc}"
        ) from exc

missing_csvs = sorted(set(required_csvs) - list_names(f"{RAW}/olist/"))
weather_files = sorted(name for name in list_names(f"{RAW}/weather/") if name.endswith(".json"))
holiday_files = sorted(name for name in list_names(f"{RAW}/holidays/") if name.endswith(".json"))

if missing_csvs or len(weather_files) < 27 or len(holiday_files) < 3:
    raise ValueError(
        "Raw input check failed. Missing Olist CSVs: "
        f"{missing_csvs}. Weather JSON files found: {len(weather_files)} of 27. "
        f"Holiday JSON files found: {len(holiday_files)} of 3. "
        "Upload the files, then run this notebook again."
    )

print("Raw input check passed.")
print(f"Olist CSV files: {len(required_csvs)}")
print(f"Weather state files: {len(weather_files)}")
print(f"Holiday year files: {len(holiday_files)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze
# MAGIC
# MAGIC This keeps the raw CSV and JSON fields in Delta with source audit columns.

# COMMAND ----------

# MAGIC %run ./01_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver
# MAGIC
# MAGIC This standardizes types, creates shipment measures and records quality failures.

# COMMAND ----------

# MAGIC %run ./02_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold
# MAGIC
# MAGIC This creates the six dashboard tables.

# COMMAND ----------

# MAGIC %run ./03_gold

# COMMAND ----------

print("All four run stages completed: input check, Bronze, Silver, Gold.")
