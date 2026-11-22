# Innov8 Logistics Pipeline — Project Report

**Team:** Innov8 · **Course:** B.Tech CSE (AI/ML)<br>
**Project area:** Logistics, supply chain and delivery optimization

## Executive summary

This project builds a small end-to-end data pipeline to study shipment and delivery performance in Brazil. It uses Olist e-commerce order records as the main source, then adds historical weather and public-holiday information as supporting context. Apache Spark on Databricks cleans and combines the data in Bronze, Silver and Gold layers. The Gold outputs support analysis of delivery delays, seller performance, delivery routes and freight efficiency.

The purpose is to help a logistics team ask clear questions about shipment outcomes. The data can show patterns and associations. It cannot prove that a particular weather condition or holiday caused a delay.

## Introduction and problem statement

Delivery operations involve many linked steps: a seller prepares an order, the shipment travels across a route, and the customer receives it. Late deliveries can affect customer experience and make it harder to plan supply-chain operations. A data pipeline can bring shipment records into one place, standardize them, and make performance easier to compare.

**Problem statement:** Logistics — Supply Chain and Delivery Optimization: analyze shipment records, warehouse (seller) performance, delivery delays and transportation efficiency.

The project uses Olist seller information as a **warehouse proxy** because the dataset identifies sellers but does not provide warehouse facility records. Olist also does not provide carrier names, so this project compares freight-value, product-weight and estimated-distance bands instead of claiming to measure individual carrier performance.

## Objectives and questions

The pipeline is designed to answer these questions:

1. How many orders were delivered, and what share arrived on or before the estimated date?
2. How do delivery time and delays vary by seller and seller-to-customer state route?
3. How do freight value, product weight and approximate distance relate to shipment outcomes?
4. As supporting context, how do observed weather and nearby public holidays align with delay patterns?

## Data sources and acknowledgements

1. **Olist Brazilian E-Commerce Public Dataset** — [Kaggle dataset page](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). This is the primary source for orders, order items, payments, reviews, customers, sellers, products and geolocation. Kaggle lists the dataset as CC BY-NC-SA 4.0. Credit Olist and follow the non-commercial and share-alike terms when redistributing adapted data. Check the dataset page terms before reuse.
2. **Open-Meteo Historical Weather API** — [historical API documentation](https://open-meteo.com/en/docs/historical-weather-api) and [terms](https://open-meteo.com/en/terms). Weather is collected as one representative point per Brazilian state, not at each buyer's address. Follow Open-Meteo's attribution and license terms, including attribution to the underlying reanalysis provider where required.
3. **Nager.Date public holidays API** — [API project](https://github.com/nager/Nager.Date) and [Brazil public-holiday endpoint example](https://date.nager.at/api/v3/PublicHolidays/2018/BR). The project requests Brazil holidays for 2016–2018. Credit Nager.Date and check its current service terms before redistribution; a separate reuse license for each returned holiday record was not confirmed.

The two APIs provide supporting context. Olist shipment records remain the primary basis for delivery and seller analysis.

## Architecture

~~~mermaid
flowchart LR
  O[Olist CSV files] --> V[Unity Catalog raw Volume]
  W[Open-Meteo API] --> V
  H[Nager.Date API] --> V
  V --> B[Bronze Delta tables]
  B --> S[Silver cleaned and joined shipments]
  S --> G[Gold analysis tables]
  G --> P[Power BI report, maintained separately]
~~~

The raw files are stored in a Unity Catalog Volume. Databricks reads those files into Bronze Delta tables, cleans and joins them into Silver, then writes dashboard-ready Gold tables. The Power BI report was created separately and is not included in this GitHub repository.

## Pipeline design

### Extraction and raw storage

The extraction scripts request daily weather for one point in each Brazilian state and Brazil's public holidays for 2016–2018. They save API responses as raw JSON files. If a Databricks session cannot reach the public internet, the scripts can run on a computer with internet access and the JSON files can then be uploaded to the Volume.

The Olist CSV files are downloaded from Kaggle and uploaded without changing their original column names. Raw inputs are kept separate from transformed tables so the team can trace results back to source files.

### Bronze layer

The Bronze notebook reads the Olist CSV and API JSON inputs and writes Delta tables. It keeps source fields as close to the original form as possible and adds an ingestion timestamp and source name. This layer provides a durable record of what was received and when it was loaded.

### Silver layer

The Silver notebook fixes data types and timestamps, removes duplicates, standardizes state and city text, and joins the order, item, customer, seller, payment and review records. It uses ZIP-prefix geolocation averages to estimate seller-to-customer distance with the haversine formula. This distance is an estimate, not a road or carrier-tracking distance.

The notebook derives delivery days, estimated days, delay days, late flags, approval hours, seller dispatch days and freight ratio. It joins representative state weather and holiday-window flags for supporting analysis. Rows that fail defined checks are written to quarantine so questionable records can be reviewed instead of silently treated as valid.

### Gold layer

The Gold notebook writes the following dashboard-ready Delta tables in overwrite mode. Overwrite makes reruns repeatable and prevents old summaries from accumulating.

| Gold table | What it summarizes |
|---|---|
| `gold_kpi_summary` | Overall order, delivery, delay and freight KPIs |
| `gold_otd_by_route` | Delivery performance by seller state and customer state |
| `gold_carrier_or_freight_performance` | Order outcomes across freight, weight and estimated-distance bands; not carrier-specific results |
| `gold_seller_performance` | Seller delivery and dispatch performance; seller is a warehouse proxy |
| `gold_delay_root_causes` | Delay patterns by rain, distance, holiday window, month and Brazilian season |
| `gold_monthly_trends` | Monthly order and delivery trends |

## Data quality

The pipeline checks for missing required identifiers and dates, negative delivery durations, invalid ranges, duplicate records and broken links between orders and related tables. Failing Silver rows are saved in a quarantine table. A quality summary records the checks and their outcomes.

These checks reduce common data errors, but they do not make the source dataset complete. Some dates, reviews, geolocation points or API observations can be missing. Weather is represented by a state-level point, and distance is estimated from ZIP-prefix averages.

## Power BI visualization

A five-page Power BI report was built separately from this repository using the Gold tables:

1. Executive Overview
2. Seller-to-Customer Route Performance
3. Freight & Shipping Efficiency
4. Delay Root Causes
5. Seller Performance

The report uses order, on-time, delay and freight summaries, plus route, band and seller breakdowns. The PBIX file is intentionally not committed here; it can be shared separately with the team or evaluator.

## Results and insights

Replace every bracketed item below with values confirmed from the completed Databricks run and Power BI report. Do not copy estimates from a development screenshot without checking the final tables.

- Total orders analyzed: **[INSERT VERIFIED VALUE]**
- Delivered orders included in the on-time calculation: **[INSERT VERIFIED VALUE]**
- On-time delivery rate: **[INSERT VERIFIED VALUE]**
- Average delivery time: **[INSERT VERIFIED VALUE] days**
- Average delay among late orders: **[INSERT VERIFIED VALUE] days**
- Freight or distance pattern observed: **[INSERT VERIFIED INSIGHT]**
- Seller or route pattern observed: **[INSERT VERIFIED INSIGHT]**
- Weather or holiday association observed: **[INSERT VERIFIED INSIGHT; describe as association, not cause]**

**[INSERT SCREENSHOT: Databricks run showing Bronze, Silver and Gold completed]**

**[INSERT SCREENSHOT: Power BI Executive Overview page]**

**[INSERT SCREENSHOT: Power BI route or seller analysis page]**

## Challenges and solutions

- **Internet access in the cloud environment:** API calls can fail when a notebook session cannot reach the public internet. The extraction scripts can be run locally, then their raw JSON outputs can be uploaded to the Volume.
- **Different source formats:** Olist CSV files and API JSON responses have different structures. Bronze preserves source inputs, while Silver converts types and standardizes names before joining.
- **Missing carrier and warehouse identifiers:** The project reports freight, weight and estimated-distance bands. It treats seller as a warehouse proxy and does not claim to measure a specific carrier or warehouse facility.
- **Approximate distance and weather:** ZIP-prefix averages and one representative weather point per state are practical for a student project, but are not precise route tracking or address-level weather.
- **Reproducible runs:** Each layer overwrites its own Delta output so students can rerun the notebooks without manually deleting previous results.

## Conclusion

This project demonstrates a practical lakehouse pipeline for logistics analysis. It brings public shipment, weather and holiday data into raw storage, applies PySpark transformations in Databricks, checks data quality, and produces Gold summaries for reporting. The outputs help compare delivery outcomes by route, seller, freight band and time. The results should be interpreted as patterns in the available sample, with care around estimated distances, representative weather and missing carrier information.

## Future work

- Use carrier scan events and actual transport-provider identifiers.
- Use warehouse facility and inventory records instead of treating seller as a proxy.
- Replace ZIP-prefix distance estimates with actual route distance and travel time.
- Add address-level or route-level weather observations where permitted.
- Add more recent data and monitor delivery performance over time.
- Improve confidence intervals and minimum-order thresholds for seller comparisons.
- Automate refreshes and alert the team when late-delivery rates change.

## How to run the code in this repository

1. Create a Databricks Free Edition workspace, the `logistics` schema, and a `raw` Unity Catalog Volume.
2. Use the original files in `data/raw/olist/` and upload the CSVs to `/Volumes/workspace/logistics/raw/olist/`.
3. Upload the JSON files in `data/raw/weather/` and `data/raw/holidays/` to matching folders in the Volume. If you refresh API data and Databricks cannot reach the APIs, run the extraction scripts locally and upload the new JSON files.
4. Import the Python files in `notebooks/` as Databricks notebooks.
5. Run `01_bronze.py`, then `02_silver.py`, then `03_gold.py`. Each notebook should finish before starting the next.
6. Use `04_run_all.py` for a single-notebook demonstration, or create a workflow with Bronze → Silver → Gold as dependent tasks.
7. Review table definitions and quality queries in `sql/`.

The pipeline targets Databricks serverless compute and Unity Catalog Volumes. It does not use DBFS mounts, RDDs or custom cluster configurations.

## Repository contents

~~~text
innov8-logistics-pipeline/
  README.md                 Project report and run instructions
  .gitignore                Skips credentials, duplicate ZIPs and local-only folders
  requirements.txt          Python dependencies for local API extraction
  data/raw/                 Olist CSV and API JSON inputs used by the pipeline
  extraction/               Weather and public-holiday API scripts
  notebooks/                Bronze, Silver, Gold and master notebooks
  sql/                      Gold table reference and data-quality queries
~~~

The raw `data/raw/` inputs are included so teammates can run the pipeline. Duplicate ZIP archives are excluded because the extracted source files are already present. The local `docs/` and `dashboard/` folders are not part of this GitHub submission. Never commit credentials or access tokens.

## References

- Olist Brazilian E-Commerce Public Dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- Open-Meteo Historical Weather API: https://open-meteo.com/en/docs/historical-weather-api
- Open-Meteo terms: https://open-meteo.com/en/terms
- Nager.Date project: https://github.com/nager/Nager.Date
- Apache Spark: https://spark.apache.org/docs/latest/
- Delta Lake: https://docs.delta.io/
- Databricks Unity Catalog Volumes: https://docs.databricks.com/en/volumes/
