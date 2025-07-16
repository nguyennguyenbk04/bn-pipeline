# DE_project: Real-Time Data Lakehouse with MySQL, Debezium, Kafka, Spark, and Azure

## Overview
This project implements a modern data lakehouse pipeline for an online store, enabling real-time Change Data Capture (CDC) from MySQL to Azure Data Lake Storage (ADLS) using Debezium, Kafka, and Apache Spark. The architecture supports both batch and streaming ETL, with a layered approach (Bronze, Silver, Gold) for scalable analytics and data warehousing.

---

## Architecture
- **Source Database:** MySQL (OLTP for online store)
- **CDC:** Debezium MySQL Connector (Dockerized)
- **Streaming Platform:** Apache Kafka (Dockerized, dual listener for host and container access)
- **Bronze Layer:** Raw CDC events stored in ADLS Gen2 (abfss) via Spark Structured Streaming
- **Silver Layer:** Cleaned, merged, and deduplicated tables (Parquet)
- **Gold Layer:** Data warehouse star schema (Fact & Dimension tables) for analytics
- **Orchestration:** Python scripts and Jupyter notebooks

---

## Key Components

### 1. Database Schema
- See `dw_design/online_store.sql` and `dw_design/dw_design.sql` for full OLTP and DW schemas.
- Supports multi-seller orders, product categories, payments, reviews, and more.

### 2. CDC & Streaming
- **Debezium** captures row-level changes from MySQL and publishes to Kafka topics (one per table).
- **Kafka** acts as the streaming backbone for all table changes.
- **Bronze Streaming (scripts/streaming/bronze_stream_standalone.py):**
  - Consumes all table topics from Kafka
  - Parses Debezium CDC JSON (nested payload)
  - Writes raw change events to ADLS Gen2, partitioned by ingestion time
  - Supports both Docker and standalone Spark execution

### 3. Data Lake Layers
- **Bronze:** Raw CDC events (per table, partitioned by ingestion time)
- **Silver:** Upsert/merge logic to apply CDC to base tables (notebooks/scripts)
- **Gold:** Star schema for analytics (FactSales, DimProduct, etc.)

### 4. ETL & Analytics
- **Jupyter Notebooks (scripts/dw_load.ipynb, table_filter.ipynb, parquet_converter.ipynb):**
  - Read from Bronze/Silver
  - Merge CDC changes (upsert/delete)
  - Build Fact and Dimension tables
  - Load to MySQL DW or write to Parquet/CSV

---

## How to Run

### 1. Start CDC Infrastructure
```bash
cd debezium-mysql-connector
# Start Kafka, Zookeeper, Debezium, MySQL
docker compose up -d
```

### 2. Register Debezium Connector
```bash
docker cp ./config/connector.json debezium-mysql-connector-debezium-1:/connector.json
docker exec -it debezium-mysql-connector-debezium-1 curl -X POST -H "Content-Type: application/json" --data @/connector.json http://localhost:8083/connectors
```

### 3. Start Bronze Streaming
```bash
cd scripts/streaming
python bronze_stream_standalone.py
```

### 4. Process Silver/Gold Layers
- Use notebooks in `scripts/` to merge, clean, and build analytics tables.

---

## Project Structure
```
DE_project/
├── debezium-mysql-connector/   # Docker Compose, Debezium, Kafka configs
├── dw_design/                  # SQL schemas for OLTP and DW
├── scripts/
│   ├── streaming/              # Spark streaming apps, JARs
│   ├── dw_load.ipynb           # DW ETL notebook
│   ├── table_filter.ipynb      # Bronze to Silver logic
│   └── parquet_converter.ipynb # Parquet utilities
├── README.md                   # Project documentation
└── requirements.txt            # Python dependencies
```

---

## Features
- Real-time CDC from MySQL to Azure Data Lake
- Handles all major OLTP tables (Customers, Orders, Products, etc.)
- Schema evolution and multi-table support
- Partitioned, scalable storage in ADLS Gen2
- Modular ETL for Silver/Gold layers
- Ready for analytics, BI, and ML workloads

---

## Useful Links
- **Kafka UI:** http://localhost:8081
- **Debezium Connect API:** http://localhost:8083
- **Azure Storage Explorer:** (for ADLS browsing)

---
