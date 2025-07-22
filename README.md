

# Real-time MySQL to Azure Data Lake E-commerce Pipeline

A complete real-time data pipeline that captures MySQL database changes and streams them to Azure Data Lake using **Debezium CDC**, **Apache Kafka**, and **Apache Spark**. Features chronological event processing across Bronze, Silver, and Gold layers for robust data consistency.

**Key Features:**
- Real-time CDC from MySQL to Azure Data Lake (Bronze/Silver/Gold layers)
- Chronological event processing (INSERT → UPDATE → DELETE order)
- Multi-layer architecture with smart partitioning
- Dockerized infrastructure (MySQL, Kafka, Zookeeper, Debezium)
- Production-ready e-commerce schema with sample data

---

## Architecture Overview

```
MySQL Database (OLTP)
    ↓ (Debezium CDC)
Apache Kafka
    ↓ (Spark Streaming)
Azure Data Lake Storage Gen2
    ├── Bronze Layer (Raw CDC events)
    ├── Silver Layer (Cleaned data)
    └── Gold Layer (Analytics-ready)
```

**Pipeline Stages:**
1. **Bronze Layer**: Raw CDC events with full audit trail
2. **Silver Layer**: Cleaned, deduplicated operational data  
3. **Gold Layer**: Aggregated, analytics-ready datasets

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

**Bronze Layer** (Raw CDC events):
```bash
cd ../scripts/streaming
python stream-bronze.py
```

**Silver Layer** (Cleaned operational data):
```bash
python stream-silver.py
```

**Gold Layer** (Analytics aggregations):
```bash
python stream-gold.py
```

### 6. Test the Pipeline
```sql
-- Connect to MySQL and make changes
docker exec -it debezium-mysql-connector-mysql-1 mysql -u root -prootpassword online_store

-- Test chronological processing
INSERT INTO Customers (Name, Email, PhoneNumber) VALUES ('Test User', 'test@example.com', '555-0123');
UPDATE Customers SET Name = 'Updated User' WHERE Email = 'test@example.com';
DELETE FROM Customers WHERE Email = 'test@example.com';
```

Watch the streaming logs to see events processed in chronological order!

---

## Project Structure

```
DE_project/
├── debezium-mysql-connector/      # Infrastructure setup
│   ├── docker-compose.yml         # MySQL, Kafka, Debezium containers
│   ├── start_debezium.sh          # Automated connector setup
│   └── config/                    # Debezium connector configuration
├── dw_design/                     # Database schemas
│   ├── online_store.sql           # E-commerce OLTP schema + sample data
│   ├── dw_design.sql             # Data warehouse schema
│   └── fake_db.py                # Data generation utilities
├── scripts/
│   ├── streaming/                 # Real-time processing
│   │   ├── stream-bronze.py       # Bronze layer CDC processing
│   │   ├── stream-silver.py       # Silver layer data cleaning
│   │   ├── stream-gold.py         # Gold layer aggregations
│   │   └── jars/                  # Spark dependencies (auto-downloaded)
│   ├── dw_load.ipynb             # Batch ETL notebook
│   ├── table_filter.ipynb        # Data filtering utilities
│   └── parquet_converter.ipynb   # Format conversion tools
├── DB_mig/                       # Migration utilities
│   ├── raw_storage_migration     # Data migration scripts
│   └── single_table_mig         # Single table migration
├── README.md                     # This documentation
├── requirements.txt              # Python dependencies
└── .gitignore                   # Git ignore rules
```

---

## Key Features Explained

### 🔄 Chronological CDC Processing
The pipeline ensures data consistency by processing CDC events in chronological order:
1. **INSERT** operations (oldest events first)
2. **UPDATE** operations (middle events)  
3. **DELETE** operations (newest events last)

This prevents data inconsistencies that occur when events are processed out of order.

### 🏗️ Multi-Layer Architecture

**Bronze Layer (`stream-bronze.py`)**:
- Raw CDC events with full audit trail
- Preserves all database changes
- Includes operation type and timestamps

**Silver Layer (`stream-silver.py`)**:  
- Deduplicated operational data
- Cleaned and validated records
- Business-ready datasets

**Gold Layer (`stream-gold.py`)**:
- Aggregated analytics datasets
- SCD Type 4 dimension tracking
- Pre-computed metrics

### ⚡ Smart Partitioning
Automatic partitioning based on data volume:
- Small datasets: Single file
- Medium datasets: 2-4 files
- Large datasets: Up to 10 files

### 🔧 Production Features
- Automatic cache management
- Error recovery and retry logic
- Configurable checkpoint locations
- Background process monitoring

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

---

## Configuration

### Azure Storage Credentials
Update the storage account keys in your streaming scripts:

```python
# In stream-bronze.py, stream-silver.py, stream-gold.py
spark.conf.set(
    "fs.azure.account.key.mybronze.dfs.core.windows.net",
    "YOUR_BRONZE_STORAGE_KEY"
)
```

### Checkpoint Management
```bash
# Clear checkpoints for fresh start
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

**Out of Order Processing:**
- The pipeline now automatically handles chronological ordering
- Events are sorted by timestamp within each batch

### Debug Mode
Enable detailed logging in streaming scripts:
```python
spark.sparkContext.setLogLevel("DEBUG")
```

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
