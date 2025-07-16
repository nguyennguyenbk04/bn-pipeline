

# DE_project: Real-Time Data Lakehouse Pipeline

**DE_project** is a ready-to-use data pipeline for real-time analytics. It captures changes from a MySQL database and streams them to Azure Data Lake using Debezium, Kafka, and Apache Spark. The project is organized for easy setup and analytics, even if you’re new to the stack.

---

## Environment Setup (Before You Start)

Follow these steps to prepare your environment before running the project:

1. **Install Docker & Docker Compose**
   - Windows/Mac: [Docker Desktop](https://www.docker.com/products/docker-desktop/)
   - Linux: `sudo apt install docker.io`
   - Check: `docker --version` and `docker compose version`

2. **Install Python 3.8 or newer**
   - [Download Python](https://www.python.org/downloads/)
   - Check: `python --version`
   - (Recommended) Create a virtual environment:
     ```bash
     python -m venv venv
     source venv/bin/activate  # On Windows: venv\Scripts\activate
     ```

3. **Install Java (for Spark)**
   - Java 8 or 11 recommended
   - Check: `java -version`

4. **Install Apache Spark**
   - [Download Spark](https://spark.apache.org/downloads.html) and follow the install guide
   - Or install PySpark: `pip install pyspark`

5. **Install Python dependencies**
   - Run: `pip install -r requirements.txt`

6. **Azure Data Lake Storage Gen2 Account**
   - Make sure you have an Azure Storage account with Data Lake Gen2 enabled
   - Update credentials in the config files or as environment variables as needed

7. **(Optional) Azure Storage Explorer**
   - [Download](https://azure.microsoft.com/en-us/products/storage/storage-explorer/) to browse your Data Lake output

---


## Setup & Run: Step-by-Step

### Prerequisites

1. **Install Docker**
   - [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows/Mac)
   - Or `sudo apt install docker.io` (Linux)
   - Make sure Docker Compose is available (`docker compose version`)

2. **Install Python 3.8+**
   - [Python Downloads](https://www.python.org/downloads/)
   - Recommended: Use a virtual environment (`python -m venv venv`)

3. **Install Java (for Spark)**
   - Java 8 or 11 recommended (`java -version`)

4. **Install Apache Spark (if running locally)**
   - [Spark Downloads](https://spark.apache.org/downloads.html)
   - Or use PySpark via pip: `pip install pyspark`

5. **Azure Storage Account**
   - You need an Azure Data Lake Storage Gen2 account and credentials (update configs as needed).

---

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd DE_project
```

### 2. Set up Python environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Start the data infrastructure (MySQL, Kafka, Debezium)
```bash
cd debezium-mysql-connector
docker compose up -d
```
This will start MySQL, Kafka, Zookeeper, and Debezium in Docker containers. Wait a few seconds for all services to be healthy.

### 4. Register the Debezium MySQL connector
This step tells Debezium which MySQL database to monitor for changes.
```bash
docker cp ./config/connector.json debezium-mysql-connector-debezium-1:/connector.json
docker exec -it debezium-mysql-connector-debezium-1 curl -X POST -H "Content-Type: application/json" --data @/connector.json http://localhost:8083/connectors
```
You should see a JSON response confirming the connector is created.

### 5. Start the Spark streaming job
This job reads changes from Kafka and writes them to Azure Data Lake.
```bash
cd ../scripts/streaming
python bronze_stream_standalone.py
```
You should see log messages like `[READY] Listening for changes on table: ...`.

### 6. Explore and analyze the data
- Use the notebooks in `scripts/` (like `dw_load.ipynb` and `table_filter.ipynb`) to process and analyze your data.
- You can use Azure Storage Explorer to browse the output in your Data Lake.

---

### Troubleshooting
- If you get connection errors, make sure all Docker containers are running (`docker ps`).
- If Spark cannot connect to Azure, check your credentials in the config and environment variables.
- For more help, see the comments in each script and notebook.

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
- Real-time sync from MySQL to Azure Data Lake
- Easy setup with Docker and Python
- Modular notebooks for analytics and ETL
- Handles all major tables (Customers, Orders, Products, etc.)
- Scalable, partitioned storage

---

## Helpful Links
- **Kafka UI:** http://localhost:8081
- **Debezium Connect API:** http://localhost:8083
- **Azure Storage Explorer:** (for browsing ADLS)

---
