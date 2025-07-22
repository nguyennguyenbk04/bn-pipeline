

# Real-time MySQL to Azure Data Lake E-commerce Pipeline with Delta Lake

A complete real-time data pipeline that captures MySQL database changes and streams them to Azure Data Lake using **Debezium CDC**, **Apache Kafka**, **Apache Spark**, and **Delta Lake**. Features ACID transactions, time travel, and chronological event processing across Bronze, Silver, and Gold layers for enterprise-grade data consistency.

**Key Features:**
- Real-time CDC from MySQL to Azure Data Lake with **Delta Lake format**
- **ACID transactions** and **time travel** capabilities
- Chronological event processing (INSERT → UPDATE → DELETE order)
- Multi-layer Delta Lake architecture (Bronze/Silver/Gold)
- **SCD Type 4** dimension tables with change history tracking
- Dockerized infrastructure (MySQL, Kafka, Zookeeper, Debezium)
- Production-ready e-commerce schema with sample data

---

## Architecture Overview

```
MySQL Database (OLTP)
    ↓ (Debezium CDC)
Apache Kafka
    ↓ (Spark Streaming + Delta Lake)
Azure Data Lake Storage Gen2
    ├── Bronze-Delta Layer (Raw CDC events with ACID)
    ├── Silver-Delta Layer (Cleaned data with versioning)
    └── Gold-Delta Layer (SCD Type 4 dimensions with history)
```

**Pipeline Stages:**
1. **Bronze-Delta Layer**: Raw CDC events with ACID transactions and audit trail
2. **Silver-Delta Layer**: Cleaned, deduplicated operational data with schema evolution  
3. **Gold-Delta Layer**: SCD Type 4 dimensions with change history and time travel

---

## Environment Setup

### Prerequisites

1. **Docker & Docker Compose**
   ```bash
   # Check installation
   docker --version
   docker compose version
   ```

2. **Python 3.8+**
   ```bash
   python --version
   # Recommended: Create virtual environment
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   ```

3. **Java 8 or 11** (for Spark)
   ```bash
   java -version
   ```

4. **Azure Data Lake Storage Gen2**
   - Create storage accounts: `mybronze`, `mysilver`, `mygold`
   - Create containers: `bronze-delta`, `silver-delta`, `gold-delta`
   - Update credentials in streaming scripts

5. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

---

## Quick Start

### 1. Clone and Setup
```bash
git clone <repository-url>
cd DE_project
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Start Infrastructure
```bash
cd debezium-mysql-connector
docker compose up -d
```
Wait for all services to be healthy (~30 seconds).

### 3. Create Sample Database
```bash
# Load the e-commerce schema and sample data
docker exec -i debezium-mysql-connector-mysql-1 mysql -u root -prootpassword < ../dw_design/online_store.sql
```

### 4. Register Debezium Connector
```bash
# Configure CDC monitoring
bash start_debezium.sh
```

### 5. Start Real-time Streaming (Choose your layer)

**Bronze-Delta Layer** (Raw CDC events with ACID transactions):
```bash
cd ../scripts/streaming-delta
python stream-bronze-delta.py
```

**Silver-Delta Layer** (Cleaned operational data with versioning):
```bash
python stream-silver-delta.py
```

**Gold-Delta Layer** (SCD Type 4 dimensions with change history):
```bash
python stream-gold-delta.py
```

**Legacy Parquet Streaming** (if needed):
```bash
cd ../scripts/streaming
python stream-bronze.py  # Parquet format
python stream-silver.py  # Parquet format
python stream-gold.py    # Parquet format
```

### 6. Test the Pipeline
```sql
-- Connect to MySQL and make changes
docker exec -it debezium-mysql-connector-mysql-1 mysql -u root -prootpassword online_store

-- Test chronological processing with Delta Lake ACID transactions
INSERT INTO Customers (Name, Email, PhoneNumber) VALUES ('Test User', 'test@example.com', '555-0123');
UPDATE Customers SET Name = 'Updated User' WHERE Email = 'test@example.com';
DELETE FROM Customers WHERE Email = 'test@example.com';
```

Watch the streaming logs to see:
- **ACID transactions** in action
- Events processed in chronological order
- **Delta Lake MERGE operations** for efficient updates
- **Time travel** capabilities for data versioning

---

## Project Structure



```
DE_project/
├── debezium-mysql-connector/      # Infrastructure: MySQL, Kafka, Debezium
│   ├── docker-compose.yml         # Container orchestration
│   ├── start_debezium.sh          # Debezium connector setup
│   └── config/                    # Debezium connector configs
├── dw_design/                     # Database schemas & sample data
│   ├── online_store.sql           # E-commerce OLTP schema
│   ├── dw_design.sql              # Data warehouse schema
│   └── fake_db.py                 # Data generation utilities
├── scripts/
│   ├── streaming-delta/           # Delta Lake real-time streaming (RECOMMENDED)
│   │   ├── stream-bronze-delta.py # Bronze layer CDC (Delta Lake)
│   │   ├── stream-silver-delta.py # Silver layer cleaning (Delta Lake)
│   │   ├── stream-gold-delta.py   # Gold layer SCD Type 4 (Delta Lake)
│   │   └── requirements-delta.txt # Delta Lake dependencies
│   ├── batch-delta/               # Delta Lake batch ETL & utilities
│   │   ├── table_filter_delta.ipynb # Delta Lake data filtering
│   │   ├── dw_load_delta.ipynb      # Delta Lake data warehouse ETL
│   │   └── csv_to_delta_converter.ipynb # CSV to Delta conversion
│   ├── jars/                      # Shared Spark dependencies
├── DB_mig/                        # Migration utilities
│   ├── raw_storage_migration      # Data migration scripts
│   └── single_table_mig           # Single table migration
```


## JAR Dependencies Setup

The streaming applications require specific JAR files for Azure Storage, Kafka connectivity, and **Delta Lake support**. These are automatically managed but you may need to download them manually if needed.

### Required JAR Files
```
scripts/jars/                          # Main JAR directory
├── azure-storage-8.6.6.jar           # Azure Storage connectivity
├── hadoop-azure-3.3.6.jar            # Hadoop Azure integration
├── hadoop-common-3.3.6.jar           # Hadoop core libraries
├── jetty-client-9.4.43.v20210629.jar # HTTP client for Azure
├── jetty-http-9.4.43.v20210629.jar   # HTTP protocol support
├── jetty-io-9.4.43.v20210629.jar     # I/O utilities
├── jetty-util-9.4.43.v20210629.jar   # Jetty utilities
├── jetty-util-ajax-9.4.43.v20210629.jar # AJAX utilities
├── kafka-clients-3.5.0.jar           # Kafka client libraries
├── mysql-connector-j-9.3.0.jar       # MySQL JDBC driver
└── spark-sql-kafka-0-10_2.12-3.5.0.jar # Spark-Kafka integration

streaming-delta/jars/                   # Delta Lake specific JARs
└── (same JARs as above + Delta dependencies automatically managed)
```

**Note**: Delta Lake dependencies (`io.delta:delta-spark_2.12:3.0.0`) are automatically downloaded via Spark packages configuration.

### Manual JAR Download (if needed)
If the JARs are not automatically downloaded, you can get them from:

1. **Maven Central Repository**: https://mvnrepository.com/
2. **Apache Spark**: https://spark.apache.org/downloads.html
3. **Azure Storage**: https://github.com/Azure/azure-storage-java

Place all JAR files in both:
- `scripts/jars/` (main location)
- `scripts/streaming/jars/` (Parquet streaming copy)
- `scripts/streaming-delta/jars/` (Delta Lake streaming copy - optional, uses packages)

**Note**: JAR files are excluded from Git tracking (see `.gitignore`) to keep the repository clean.

---

## Monitoring & Management

### Service Health Check
```bash
# Check all containers
docker ps

# View logs
docker logs debezium-mysql-connector-kafka-1
docker logs debezium-mysql-connector-debezium-1
```

### Kafka Management
- **Kafka UI**: http://localhost:8081
- **Debezium Connect API**: http://localhost:8083
- **Connector Status**: http://localhost:8083/connectors

### Azure Storage
Use [Azure Storage Explorer](https://azure.microsoft.com/en-us/products/storage/storage-explorer/) to browse your Data Lake output.

**Browse Delta Tables:**
- Browse to your storage accounts: `mybronze`, `mysilver`, `mygold`
- Navigate to containers: `bronze-delta`, `silver-delta`, `gold-delta`
- Delta tables will show as directories with `_delta_log` folders containing transaction logs

---

## Configuration

### Azure Storage Credentials
Update the storage account keys in your streaming scripts:

**Delta Lake Streaming** (Recommended):
```python
# In stream-bronze-delta.py, stream-silver-delta.py, stream-gold-delta.py
spark.conf.set(
    "fs.azure.account.key.mybronze.dfs.core.windows.net",
    "YOUR_BRONZE_STORAGE_KEY"
)
```

**Legacy Parquet Streaming**:
```python
# In stream-bronze.py, stream-silver.py, stream-gold.py
spark.conf.set(
    "fs.azure.account.key.mybronze.dfs.core.windows.net",
    "YOUR_BRONZE_STORAGE_KEY"
)
```

### Checkpoint Management
**Delta Lake Streaming**:
```bash
# Clear Delta Lake checkpoints for fresh start
rm -rf /tmp/spark-checkpoints-bronze/cdc_bronze_delta_*
rm -rf /tmp/spark-checkpoints-silver/cdc_silver_delta_*
rm -rf /tmp/spark-checkpoints-gold/cdc_scd4_delta_*
```

**Legacy Parquet Streaming**:
```bash
# Clear Parquet checkpoints for fresh start
rm -rf /tmp/checkpoints/cdc_*
rm -rf /tmp/checkpoints/*
```

---

## Troubleshooting

### Common Issues

**Connection Errors:**
```bash
# Ensure all services are running
docker ps
# Restart if needed
docker compose down && docker compose up -d
```

**Kafka Connectivity:**
- Check if Kafka is accessible on port 9093
- Verify Debezium connector is registered: `curl http://localhost:8083/connectors`

**Azure Storage Issues:**
- Verify storage account keys are correct
- Ensure Data Lake Gen2 is enabled
- Check firewall settings

**Delta Lake Issues:**
- Verify Delta Lake packages are being downloaded: `io.delta:delta-spark_2.12:3.0.0`
- Check Delta table transaction logs in Azure Storage (`_delta_log` folders)
- Ensure ACID transaction consistency by checking Delta Lake logs

**Out of Order Processing:**
- The pipeline now automatically handles chronological ordering
- Events are sorted by timestamp within each batch
- **Delta Lake MERGE operations** ensure data consistency even with out-of-order events

### Debug Mode
Enable detailed logging in streaming scripts:
```python
spark.sparkContext.setLogLevel("DEBUG")
```

---

## Delta Lake Features

### ACID Transactions
- **Atomicity**: All operations complete successfully or are rolled back
- **Consistency**: Data integrity maintained across all operations
- **Isolation**: Concurrent operations don't interfere with each other
- **Durability**: Committed changes are permanently stored

### Time Travel & Versioning
```python
# Read previous versions of Delta tables
df = spark.read.format("delta").option("versionAsOf", 0).load("path/to/delta/table")
df = spark.read.format("delta").option("timestampAsOf", "2025-01-01").load("path/to/delta/table")
```

### Schema Evolution
Delta Lake automatically handles schema changes without breaking existing pipelines.

### SCD Type 4 Implementation
- **Current Tables**: Latest version of each dimension record
- **History Tables**: Complete change tracking with operation types
- **Change Types**: INSERT, UPDATE, DELETE with timestamps

---

## Sample E-commerce Schema

The project includes a complete e-commerce database with:
- **Customers**: Customer information and profiles
- **Sellers**: Vendor and supplier data
- **Products**: Product catalog with categories
- **Orders**: Order lifecycle and status tracking
- **OrderItems**: Detailed line items
- **Payments**: Payment processing records
- **Reasons**: Return and refund tracking

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with the provided e-commerce schema
5. Submit a pull request

---

## License

This project is open source and available under the MIT License.
