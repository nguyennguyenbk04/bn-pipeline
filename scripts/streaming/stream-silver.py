import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType

def create_spark_session():
    # Set up local JAR paths for Azure storage components
    jar_dir = "/home/bnguyen/Desktop/DE_project/scripts/jars"
    azure_jars = [
        f"{jar_dir}/hadoop-common-3.3.6.jar",
        f"{jar_dir}/hadoop-azure-3.3.6.jar", 
        f"{jar_dir}/azure-storage-8.6.6.jar",
        f"{jar_dir}/jetty-client-9.4.43.v20210629.jar",
        f"{jar_dir}/jetty-http-9.4.43.v20210629.jar",
        f"{jar_dir}/jetty-io-9.4.43.v20210629.jar",
        f"{jar_dir}/jetty-util-9.4.43.v20210629.jar",
        f"{jar_dir}/jetty-util-ajax-9.4.43.v20210629.jar"
    ]
    
    # Filter only existing JAR files
    existing_jars = [jar for jar in azure_jars if os.path.exists(jar)]
    jar_path = ",".join(existing_jars)
    
    return (SparkSession.builder
            .appName("CDC to Silver Layer - Standalone")
            .config("spark.jars", jar_path)  # Use local JARs for Azure storage
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")  # Use package for Kafka
            .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            .config("spark.hadoop.fs.azure.account.key.mysilver.dfs.core.windows.net",
                   "bAthp0pVBfqEtyCvJElSX7MeI7ejSLa6cjuPoMz0Gg/69uzEW01y4URMDXsdFCrkpc9M54cDHnXs+AStj1gExQ==")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
            .config("spark.sql.streaming.minBatchesToRetain", "10")
            .config("spark.sql.streaming.pollingDelay", "1s")
            .config("spark.master", "local[*]")  # Run locally with all available cores
            .getOrCreate())

# Define Debezium message schemas for all tables

# Base structure for common fields
def create_base_fields():
    return [
        StructField("CreatedAt", StringType(), True),
        StructField("UpdatedAt", StringType(), True)
    ]

# Customers schema
debezium_customer_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("CustomerID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("PhoneNumber", StringType(), True)
        ] + create_base_fields()), True),
        StructField("after", StructType([
            StructField("CustomerID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("PhoneNumber", StringType(), True)
        ] + create_base_fields()), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)
    ]), True)
])

# Sellers schema
debezium_seller_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("SellerID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("PhoneNumber", StringType(), True)
        ] + create_base_fields()), True),
        StructField("after", StructType([
            StructField("SellerID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("PhoneNumber", StringType(), True)
        ] + create_base_fields()), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)
    ]), True)
])

# ProductCategories schema
debezium_product_categories_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("CategoryID", IntegerType(), True),
            StructField("CategoryName", StringType(), True),
            StructField("CategoryDescription", StringType(), True)
        ] + create_base_fields()), True),
        StructField("after", StructType([
            StructField("CategoryID", IntegerType(), True),
            StructField("CategoryName", StringType(), True),
            StructField("CategoryDescription", StringType(), True)
        ] + create_base_fields()), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)
    ]), True)
])

# Products schema
debezium_products_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("ProductID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Price", DecimalType(), True),
            StructField("Cost", DecimalType(), True),
            StructField("CategoryID", IntegerType(), True),
            StructField("SellerID", IntegerType(), True)
        ] + create_base_fields()), True),
        StructField("after", StructType([
            StructField("ProductID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Price", DecimalType(), True),
            StructField("Cost", DecimalType(), True),
            StructField("CategoryID", IntegerType(), True),
            StructField("SellerID", IntegerType(), True)
        ] + create_base_fields()), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)
    ]), True)
])

# OrderStatus schema
debezium_order_status_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("StatusID", IntegerType(), True),
            StructField("StatusName", StringType(), True),
            StructField("StatusDescription", StringType(), True)
        ] + create_base_fields()), True),
        StructField("after", StructType([
            StructField("StatusID", IntegerType(), True),
            StructField("StatusName", StringType(), True),
            StructField("StatusDescription", StringType(), True)
        ] + create_base_fields()), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)
    ]), True)
])

# Orders schema
debezium_orders_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("OrderID", IntegerType(), True),
            StructField("OrderNumber", StringType(), True),
            StructField("TotalAmount", DecimalType(), True),
            StructField("StatusID", IntegerType(), True),
            StructField("CustomerID", IntegerType(), True)
        ] + create_base_fields()), True),
        StructField("after", StructType([
            StructField("OrderID", IntegerType(), True),
            StructField("OrderNumber", StringType(), True),
            StructField("TotalAmount", DecimalType(), True),
            StructField("StatusID", IntegerType(), True),
            StructField("CustomerID", IntegerType(), True)
        ] + create_base_fields()), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)
    ]), True)
])

# OrderItems schema
debezium_order_items_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("OrderItemID", IntegerType(), True),
            StructField("OrderID", IntegerType(), True),
            StructField("ProductID", IntegerType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("CurrentPrice", DecimalType(), True)
        ] + create_base_fields()), True),
        StructField("after", StructType([
            StructField("OrderItemID", IntegerType(), True),
            StructField("OrderID", IntegerType(), True),
            StructField("ProductID", IntegerType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("CurrentPrice", DecimalType(), True)
        ] + create_base_fields()), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)
    ]), True)
])

# Reasons schema
debezium_reasons_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("ReasonID", IntegerType(), True),
            StructField("OrderID", IntegerType(), True),
            StructField("ReasonType", StringType(), True),
            StructField("ReasonDescription", StringType(), True)
        ] + create_base_fields()), True),
        StructField("after", StructType([
            StructField("ReasonID", IntegerType(), True),
            StructField("OrderID", IntegerType(), True),
            StructField("ReasonType", StringType(), True),
            StructField("ReasonDescription", StringType(), True)
        ] + create_base_fields()), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)
    ]), True)
])

# Payments schema
debezium_payments_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("PaymentID", IntegerType(), True),
            StructField("OrderID", IntegerType(), True),
            StructField("PaymentMethodID", IntegerType(), True),
            StructField("Amount", DecimalType(), True)
        ] + create_base_fields()), True),
        StructField("after", StructType([
            StructField("PaymentID", IntegerType(), True),
            StructField("OrderID", IntegerType(), True),
            StructField("PaymentMethodID", IntegerType(), True),
            StructField("Amount", DecimalType(), True)
        ] + create_base_fields()), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)
    ]), True)
])


# Schema mapping for all tables
table_schemas = {
    "Customers": debezium_customer_schema,
    "Sellers": debezium_seller_schema,
    "ProductCategories": debezium_product_categories_schema,
    "Products": debezium_products_schema,
    "OrderStatus": debezium_order_status_schema,
    "Orders": debezium_orders_schema,
    "OrderItems": debezium_order_items_schema,
    "Reasons": debezium_reasons_schema,
    "Payments": debezium_payments_schema,
}

def read_from_kafka(spark, kafka_topic, stream_type="default"):
    """
    Read from Kafka with different checkpoint locations for different stream types
    stream_type: 'silver-final' for CDC processing to silver layer
    """
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9093") # Spark local, if Spark in container use kafka:9093
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "latest")  # Only process NEW messages after starting
            .option("failOnDataLoss", "false")  # Don't fail if some data is lost
            .load())

def clear_checkpoints_if_needed(table_name, clear_checkpoints=False):
    """
    Clear checkpoint directories if requested - updated for silver-final
    """
    if clear_checkpoints:
        import shutil
        silver_final_checkpoint = f"/tmp/checkpoints/cdc_silver_{table_name}"
        
        for checkpoint_path in [silver_final_checkpoint]:
            try:
                if os.path.exists(checkpoint_path):
                    shutil.rmtree(checkpoint_path)
                    print(f"[CLEANUP] Cleared checkpoint: {checkpoint_path}")
            except Exception as e:
                print(f"[WARNING] Could not clear checkpoint {checkpoint_path}: {e}")

def smart_partitioning_write(df, path, table_name):
    """
    Intelligently partition data based on size to balance between 
    file count and performance
    """
    record_count = df.count()
    
    # Define partitioning strategy
    if record_count < 50000:
        # Small data: single file
        partitions = 1
        strategy = "Single file (small dataset)"
    elif record_count < 500000:
        # Medium data: 2-4 files  
        partitions = min(4, max(2, record_count // 100000))
        strategy = f"Medium dataset: {partitions} files"
    else:
        # Large data: more partitions, but cap at reasonable number
        partitions = min(10, max(4, record_count // 200000))
        strategy = f"Large dataset: {partitions} files"
    
    print(f"[PARTITION] {table_name}: {record_count:,} records -> {strategy}")
    
    # Apply partitioning and write
    partitioned_df = df.coalesce(partitions)
    partitioned_df.write.mode("overwrite").format("parquet").save(path)
    
    return partitions

def process_cdc_to_table(df, silver_final_path, table_name, schema=None, spark=None):
    """
    Process CDC events and apply them to the silver-final table with smart partitioning
    """
    # Select value from Kafka, cast string
    df_processed = (df.selectExpr("CAST(value AS STRING) as json_data", 
                                  "timestamp as kafka_timestamp"))
    
    # Parse JSON if schema is provided 
    if schema:
        df_parsed = df_processed.withColumn("parsed", from_json(col("json_data"), schema))
        
        def process_batch(batch_df, batch_id):
            if batch_df.count() == 0:
                return
            
            # Clear cache at the beginning of each batch to prevent stale file references
            spark.catalog.clearCache()
            print(f"[DEBUG] Cleared Spark cache for batch {batch_id}")
                
            # Extract CDC data based on table type - exclude operation, kafka_timestamp, ingestion_timestamp
            if table_name == "Customers":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CustomerID")).otherwise(col("parsed.payload.before.CustomerID")).alias("CustomerID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("Name"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Email")).otherwise(col("parsed.payload.before.Email")).alias("Email"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.PhoneNumber")).otherwise(col("parsed.payload.before.PhoneNumber")).alias("PhoneNumber"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation")
                )
            elif table_name == "Sellers":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.SellerID")).otherwise(col("parsed.payload.before.SellerID")).alias("SellerID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("Name"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Email")).otherwise(col("parsed.payload.before.Email")).alias("Email"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.PhoneNumber")).otherwise(col("parsed.payload.before.PhoneNumber")).alias("PhoneNumber"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation")
                )
            elif table_name == "ProductCategories":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryID")).otherwise(col("parsed.payload.before.CategoryID")).alias("CategoryID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryName")).otherwise(col("parsed.payload.before.CategoryName")).alias("CategoryName"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryDescription")).otherwise(col("parsed.payload.before.CategoryDescription")).alias("CategoryDescription"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation")
                )
            elif table_name == "Products":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ProductID")).otherwise(col("parsed.payload.before.ProductID")).alias("ProductID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("Name"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Description")).otherwise(col("parsed.payload.before.Description")).alias("Description"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Price")).otherwise(col("parsed.payload.before.Price")).alias("Price"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Cost")).otherwise(col("parsed.payload.before.Cost")).alias("Cost"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryID")).otherwise(col("parsed.payload.before.CategoryID")).alias("CategoryID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.SellerID")).otherwise(col("parsed.payload.before.SellerID")).alias("SellerID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation")
                )
            elif table_name == "OrderStatus":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusID")).otherwise(col("parsed.payload.before.StatusID")).alias("StatusID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusName")).otherwise(col("parsed.payload.before.StatusName")).alias("StatusName"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusDescription")).otherwise(col("parsed.payload.before.StatusDescription")).alias("StatusDescription"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation")
                )
            elif table_name == "Orders":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderID")).otherwise(col("parsed.payload.before.OrderID")).alias("OrderID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderNumber")).otherwise(col("parsed.payload.before.OrderNumber")).alias("OrderNumber"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.TotalAmount")).otherwise(col("parsed.payload.before.TotalAmount")).alias("TotalAmount"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusID")).otherwise(col("parsed.payload.before.StatusID")).alias("StatusID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CustomerID")).otherwise(col("parsed.payload.before.CustomerID")).alias("CustomerID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation")
                )
            elif table_name == "OrderItems":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderItemID")).otherwise(col("parsed.payload.before.OrderItemID")).alias("OrderItemID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderID")).otherwise(col("parsed.payload.before.OrderID")).alias("OrderID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ProductID")).otherwise(col("parsed.payload.before.ProductID")).alias("ProductID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Quantity")).otherwise(col("parsed.payload.before.Quantity")).alias("Quantity"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CurrentPrice")).otherwise(col("parsed.payload.before.CurrentPrice")).alias("CurrentPrice"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation")
                )
            elif table_name == "Reasons":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ReasonID")).otherwise(col("parsed.payload.before.ReasonID")).alias("ReasonID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderID")).otherwise(col("parsed.payload.before.OrderID")).alias("OrderID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ReasonType")).otherwise(col("parsed.payload.before.ReasonType")).alias("ReasonType"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ReasonDescription")).otherwise(col("parsed.payload.before.ReasonDescription")).alias("ReasonDescription"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation")
                )
            elif table_name == "Payments":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.PaymentID")).otherwise(col("parsed.payload.before.PaymentID")).alias("PaymentID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderID")).otherwise(col("parsed.payload.before.OrderID")).alias("OrderID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.PaymentMethodID")).otherwise(col("parsed.payload.before.PaymentMethodID")).alias("PaymentMethodID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Amount")).otherwise(col("parsed.payload.before.Amount")).alias("Amount"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation")
                )
            else:
                return  # Skip unknown tables
            
            # Get primary key column name for each table
            pk_columns = {
                "Customers": "CustomerID",
                "Sellers": "SellerID", 
                "ProductCategories": "CategoryID",
                "Products": "ProductID",
                "OrderStatus": "StatusID",
                "Orders": "OrderID",
                "OrderItems": "OrderItemID",
                "Reasons": "ReasonID",
                "Payments": "PaymentID"
            }
            
            pk_col = pk_columns.get(table_name)
            if not pk_col:
                return
            
            # Ensure cdc_df is initialized
            if 'cdc_df' not in locals():
                return
            
            # Process each operation type - initialize outside try block
            inserts = cdc_df.filter(col("operation") == "c")  # CREATE
            updates = cdc_df.filter(col("operation") == "u")  # UPDATE
            deletes = cdc_df.filter(col("operation") == "d")  # DELETE
                
            try:
                # Check if silver-final path has any parquet files
                try:
                    # First try to read the entire directory (for initial data)
                    existing_df = spark.read.format("parquet").load(silver_final_path)
                    table_exists = True
                except Exception:
                    try:
                        # If that fails, try to read only part-* files (for previously processed CDC data)
                        existing_df = spark.read.format("parquet").load(f"{silver_final_path}/part-*")
                        table_exists = True
                    except Exception:
                        # No parquet files exist, table doesn't exist yet
                        table_exists = False
                        existing_df = None

                if table_exists:
                    # Start with existing data and CACHE it to prevent race conditions
                    result_df = existing_df.cache()  # Cache to materialize the data
                    print(f"[DEBUG] Cached existing data for {table_name}: {result_df.count()} records")

                    # Handle deletes: remove records
                    if deletes.count() > 0:
                        delete_ids = deletes.select(pk_col).collect()
                        delete_values = [row[pk_col] for row in delete_ids]
                        result_df = result_df.filter(~col(pk_col).isin(delete_values))

                    # Handle updates: remove old records first
                    if updates.count() > 0:
                        update_ids = updates.select(pk_col).collect()
                        update_values = [row[pk_col] for row in update_ids]
                        result_df = result_df.filter(~col(pk_col).isin(update_values))
                        # Add updated records (remove operation column)
                        updates_clean = updates.drop("operation")
                        result_df = result_df.union(updates_clean)

                    # Handle inserts: add new records
                    if inserts.count() > 0:
                        inserts_clean = inserts.drop("operation")
                        result_df = result_df.union(inserts_clean)

                    # Use smart partitioning instead of fixed coalesce(1)
                    partitions_used = smart_partitioning_write(result_df, silver_final_path, table_name)
                    
                    # Unpersist the cached DataFrame to free memory
                    existing_df.unpersist()
                    
                    # Clear cache after writing to prevent stale references
                    spark.catalog.clearCache()
                    
                    print(f"[CDC] Applied {batch_df.count()} changes to {table_name} in silver-final (Total records: {result_df.count()}) - {partitions_used} parquet file(s)")
                else:
                    # Table doesn't exist, create it with inserts/updates only
                    if inserts.count() > 0 or updates.count() > 0:
                        new_records = inserts.union(updates) if inserts.count() > 0 and updates.count() > 0 else (inserts if inserts.count() > 0 else updates)
                        new_records_clean = new_records.drop("operation")
                        # Use smart partitioning for new tables too
                        partitions_used = smart_partitioning_write(new_records_clean, silver_final_path, table_name)
                        
                        # Clear cache after creating new table
                        spark.catalog.clearCache()
                        
                        print(f"[CDC] Created new table {table_name} in silver-final with {new_records_clean.count()} records - {partitions_used} parquet file(s)")
                
            except Exception as e:
                print(f"[ERROR] Failed to process table {table_name}: {e}")
                # If there's any other error, try to create table with inserts/updates only
                if inserts.count() > 0 or updates.count() > 0:
                    new_records = inserts.union(updates) if inserts.count() > 0 and updates.count() > 0 else (inserts if inserts.count() > 0 else updates)
                    new_records_clean = new_records.drop("operation")
                    # Use smart partitioning for error recovery too
                    partitions_used = smart_partitioning_write(new_records_clean, silver_final_path, table_name)
                    
                    # Clear cache after error recovery write
                    spark.catalog.clearCache()
                    
                    print(f"[CDC] Created new table {table_name} in silver-final with {new_records_clean.count()} records - {partitions_used} parquet file(s)")
    
        # Write stream using foreachBatch
        query = (df_parsed.writeStream
                .foreachBatch(process_batch)
                .trigger(processingTime="10 seconds")
                .option("checkpointLocation", f"/tmp/checkpoints/cdc_silver_{silver_final_path.split('/')[-1]}")
                .start())
        
        return query
    
    return None

def main():
    # Create Spark session
    spark = create_spark_session()
    
    # Configure Azure storage
    storage_account_silver = "mysilver"
    silver_final = "silver-final"
    
    # Set Azure credentials
    spark.conf.set(
        "fs.azure.account.key.mysilver.dfs.core.windows.net",
        "bAthp0pVBfqEtyCvJElSX7MeI7ejSLa6cjuPoMz0Gg/69uzEW01y4URMDXsdFCrkpc9M54cDHnXs+AStj1gExQ=="
    )
    
    # OPTION: Set to True if you want to clear checkpoints and start fresh
    # This ensures only NEW CDC events after starting the code are processed
    CLEAR_CHECKPOINTS = True  # Set to False to resume from where you left off
    
    if CLEAR_CHECKPOINTS:
        print("[CONFIG] Starting with fresh checkpoints - only NEW CDC events will be processed")
    else:
        print("[CONFIG] Resuming from existing checkpoints - may process some old events")
    
    # List of topics 
    topics = [
        "online_store.online_store.Customers",
        "online_store.online_store.Sellers",
        "online_store.online_store.ProductCategories", 
        "online_store.online_store.Products",
        "online_store.online_store.OrderStatus",
        "online_store.online_store.Orders",
        "online_store.online_store.OrderItems",
        "online_store.online_store.Reasons",
        "online_store.online_store.Payments",
    ]
    
    # Start streaming for each topic
    queries = []
    for topic in topics:
        # Extract table name from topic
        table_name = topic.split(".")[-1]
        silver_final_path = f"abfss://{silver_final}@{storage_account_silver}.dfs.core.windows.net/{table_name}"
        
        # Clear checkpoints if requested (ensures fresh start)
        clear_checkpoints_if_needed(table_name, CLEAR_CHECKPOINTS)
        
        # Get schema for this table
        schema = table_schemas.get(table_name)
        
        # Process CDC events and apply to silver-final table
        kafka_df_cdc = read_from_kafka(spark, topic, "silver-final")
        query_cdc = process_cdc_to_table(kafka_df_cdc, silver_final_path, table_name, schema, spark)
        if query_cdc:
            print(f"[READY] Processing CDC events for table: {table_name} to silver-final at {silver_final_path}")
            queries.append(query_cdc)
    
    # Wait for all queries to terminate
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()
    
    # Manual checkpoint cleanup commands (if needed):
    # rm -rf /tmp/checkpoints/cdc_silver_Customers
    # rm -rf /tmp/checkpoints/cdc_silver_Sellers
    # rm -rf /tmp/checkpoints/cdc_silver_Products
