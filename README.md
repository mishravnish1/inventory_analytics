## Demand Forecasting Pipeline (DBFS Storage)

This repository contains an end-to-end pipeline for forecasting product demand for the next 12 or 24 hours per pin code, using purchase, click, and cart data.
The pipeline is built using Apache Spark on Databricks, with Delta Lake tables stored in the Databricks File System (DBFS).
It follows a medallion architecture (Bronze, Silver, Gold, Analytics layers) and uses a weighted historical averages approach, enhanced by click and cart ratios, without machine learning.
Pipeline Architecture

## The pipeline follows a medallion architecture with four layers:

**Bronze Layer**: Raw, unprocessed data ingested from sources.

**Silver Layer**: Cleaned, deduplicated, and enriched data with derived features (e.g., hour_of_day).

**Gold Layer**: Aggregated datasets for analytics (e.g., hourly purchases, weighted averages).

**Analytics Layer**: Final business logic for demand forecasting.
