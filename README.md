# 🛒 PriceCatcher Data Engineering End-to-End Project

## Objective
---
In this project, I designed and implemented an end-to-end data pipeline to ingest, process, and model Malaysia’s PriceCatcher price monitoring data for analytical use. The pipeline covers the full data lifecycle, from raw data extraction to an analytics-ready data warehouse.

The project consists of the following stages:
1. Extracted PriceCatcher datasets from Malaysia’s open data portal (data.gov.my), including lookup and price data published in Parquet format.
2. Transformed and modeled the data using dimensional modeling principles, producing clean and standardized dimension and fact tables.
3. Orchestrated the pipeline using Apache Airflow, supporting both full load and daily incremental load.
4. Developed a dashboard that helps users make informed decisions when purchasing their everyday goods in their local area.

The sections below will explain additional details on the system.

## Table of Content
---
- [Dataset Used](#dataset-used)
- [Technologies](#technologies)
- [Architecture Overview](#architecture-overview)
- [Data Modeling](#data-modeling)
- [Data Loading Strategies](#data-loading-strategies)
- [Dashboard](#dashboard)

## Dataset Used
---
This project uses the PriceCatcher dataset from Malaysia’s Open Data Portal, which contains retail price information for everyday goods collected from retail premises nationwide. The dataset is accompanied by lookup tables that provide item and premise reference information.

More information about the datasets can be found in the following links:
- **PriceCatcher Dataset:** https://data.gov.my/data-catalogue/pricecatcher
- **Lookup Item Dataset:** https://data.gov.my/data-catalogue/lookup_item
- **Lookup Premise Dataset:** https://data.gov.my/data-catalogue/lookup_premise

## Technologies
---
The following technologies are used to build this project:
- **Language:** Python, SQL
- **Data Extraction & Transformation:** Python (Pandas)
- **Orchestration:** Apache Airflow
- **Storage:** Postgres (Staging & Data Warehouse)
- **Containerization:** Docker
- **Visualization:** Power BI

## Architecture Overview
---
<img width="1000" alt="Architecture Overview" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/Architecture%20Overview.png">

The PriceCatcher Data Pipeline is designed to extract, transform, and load (ETL) price monitoring data from Malaysia’s public open data portal into an analytic-ready dimensional data warehouse.

The pipeline contains 3 layers:

1. **Source Data layer:**
   - The source data layer represents the external origin of all datasets used in the pipeline.
   - In this project, all data is retrieved directly from Malaysia’s open data portal (data.gov.my), which provides monthly price information and lookup reference files in Parquet format.

2. **Staging layer:**
   - The staging layer acts as a landing zone for raw ingestion.
   - Data is loaded exactly as received, with no cleaning, transformation, or business logic applied.

3. **Datawarehouse layer:**
   - This is the transformed layer where the data becomes analytics-ready.
   - It contains dimensional and fact tables derived from the staging layer.
   - Power BI connects directly to this layer for dashboard development.

## Data Modeling
---
The datasets are designed using the principles of dimensional data modeling concepts.

<img width="500" alt="Data Model Diagram" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/DataModel.png">

## Data Loading Strategies
---
The pipeline supports two loading modes to ensure both reliability and efficiency in maintaining the data warehouse: Full Load and Daily Load.

### 1. Full Load

<img width="1000" alt="Full Load Diagram" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/Full%20Load%20Overview.png">

- **Purpose:** The Full Load process performs a complete end-to-end refresh of the pipeline. It is used for initial setup or when a full rebuild is required due to data quality issues, corruption, or structural changes.
- **Design:**
  - The process begins by extracting all available source files from Malaysia’s open data portal, including lookup items/premises and PriceCatcher datasets.
  - Each dataset is first loaded into the staging layer as raw tables, preserving the original structure and content.
  - Once staging is completed, the data is transformed and loaded into the data warehouse layer, where dimension and fact tables are fully rebuilt based on the full-loaded data sources.

### 2. Daily Load

<img width="1000" alt="Daily Load Diagram" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/Daily%20Load%20Overview.png">

- **Purpose:** The Daily Load process handles routine updates by ingesting only the latest data without reloading the entire history data every time. It runs on a daily schedule or can be triggered on demand.
- **Design:**
  - **For Lookup Datasets:** The Daily Load follows the same approach as the Full Load process. These datasets are relatively small and do not change frequently, making a full reload efficient and low-risk.
  - **For PriceCatcher Dataset:** The Daily Load applies a **delete-and-reload strategy** for the most recent 10 days of data. Instead of reprocessing the entire historical dataset, existing records within the defined date window are removed and reloaded from the source.
  - This approach balances data accuracy and performance, allowing latest arriving data or source data corrections to be captured without the overhead of a full historical reload.

## Dashboard
---
After completing the analysis, I loaded the relevant tables into Power BI and created a dashboard.

### Power BI Data Model

<img width="800" alt="Power BI Data Model" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/PowerBIDataModel.png">

### Dashboard Design

<img width="1000" alt="Dashboard Overview" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/Dashboard1.png">

The dashboard is designed with a primary goal: helping users make informed decisions when purchasing everyday goods in their local area by providing up-to-date price information and comparisons.

<img width="1000" alt="Key Price Indicators" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/Dashboard2.png">

Users can filter by State and District to focus on their selected area, and choose the Item they are interested in purchasing. 

By adjusting these filters, all visuals on the dashboard will update accordingly, ensuring the information displayed remains relevant to the selected location and item.

<img width="1000" alt="Key Metrics" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/Dashboard3.png">

The key price indicator cards provide a quick summary of price levels and recent changes for the selected item and area. These indicators are designed to help users understand the current pricing situation at a glance.

The cards show the average price for the most recent week, alongside the overall average price and the average price from the previous week. This allows users to easily compare current prices with both the general price level and last week’s prices.

To help users understand how prices are changing over time, the dashboard also includes week-over-week price changes, displayed both as a value and as a percentage. These indicators highlight whether prices are increasing or decreasing, and how significant the change is.

Together, these key price indicators give users a clear and immediate view of current prices and short-term price movements, supporting more informed purchasing decisions.


<img width="1000" alt="Premises Catured" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/Dsahboard4.png">

The premises captured information (this week vs last week) is also included to help users better interpret the price indicators. Because in some cases, changes in the average price may not fully reflect actual market movements, but may instead be influenced by differences in data coverage, for example, when fewer premises report prices in one week compared to another.

With this information, users are provided with additional context that helps them assess whether the observed price information is more reliable or should be interpreted with caution.

<img width="1000" alt="Trend Analysis" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/Dashboard5.png">

The District vs National price trend chart helps users compare how prices in their selected district change over time compared to the national average for the past 12 weeks. This helps users understand whether the trend they are seeing is a wider market pattern or specific to their local area.

<img width="1000" alt="Premise Table" src="https://github.com/Seowyongtao/PriceCatcher_DataPipeline/blob/613b51282be67cab261d642178443f5228b24fb6/img/Dashboard6.png">

The Latest Price per Premise table acts as the final step for users when deciding where to buy the selected item. It helps users identify which premise offers the lowest price within the selected area. The table is sorted by the latest captured price, from cheapest to most expensive, making it easy for users to quickly spot the most affordable options.

The latest captured date for each premise is also included, as a lower price may not always reflect the current situation if it was captured some time ago. By showing the capture date, users can better assess how recent and reliable the price information is before making a purchasing decision.
