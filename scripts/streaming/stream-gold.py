import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when, lit
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
            .appName("CDC to Gold Layer - SCD Type 4 Dimension Tables")
            .config("spark.jars", jar_path)  # Use local JARs for Azure storage
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")  # Use package for Kafka
            .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            .config("spark.hadoop.fs.azure.account.key.mygold.dfs.core.windows.net",
                   "wRPXTwWCVxWwUpavEh62A5wzLdUvRTGeB3tZKP3eRbig7ca8ZN51l0kWS32kcbH/ddQ/jNXBzqDC+AStOzXlyw==")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints_gold")
            .config("spark.sql.streaming.minBatchesToRetain", "10")
            .config("spark.sql.streaming.pollingDelay", "1s")
            .config("spark.master", "local[*]")  # Run locally with all available cores
            .getOrCreate())

# Define Debezium message schemas for tables we need for gold layer

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

# Schema mapping for tables relevant to gold layer dimensions
table_schemas = {
    "Customers": debezium_customer_schema,
    "Sellers": debezium_seller_schema,
    "ProductCategories": debezium_product_categories_schema,
    "Products": debezium_products_schema,
    "OrderStatus": debezium_order_status_schema,
}

def read_from_kafka(spark, kafka_topic, stream_type="gold"):
    """
    Read from Kafka with different checkpoint locations for different stream types
    stream_type: 'gold' for CDC processing to gold layer dimensions
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
    Clear checkpoint directories if requested - for SCD Type 4 gold layer
    """
    if clear_checkpoints:
        gold_checkpoint = f"/tmp/checkpoints_gold/cdc_scd4_{table_name}"
        
        for checkpoint_path in [gold_checkpoint]:
            try:
                if os.path.exists(checkpoint_path):
                    import shutil
                    shutil.rmtree(checkpoint_path)
                    print(f"[SCD4] Cleared checkpoint: {checkpoint_path}")
            except Exception as e:
                print(f"[WARNING] Could not clear SCD4 checkpoint {checkpoint_path}: {e}")

def smart_partitioning_write(df, path, table_name, mode="overwrite"):
    """
    Intelligently partition data based on size to balance between 
    file count and performance. Support both overwrite and append modes.
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
    
    print(f"[PARTITION] {table_name}: {record_count:,} records -> {strategy} (mode: {mode})")
    
    # Apply partitioning and write
    partitioned_df = df.coalesce(partitions)
    partitioned_df.write.mode(mode).format("parquet").save(path)
    
    return partitions

def process_cdc_to_table_scd4(df, gold_path, table_name, schema=None, spark=None):
    """
    Process CDC events and apply them to gold tables using SCD Type 4 (History + Current tables)
    - Current table: always contains latest version of each record
    - History table: captures all changes (UPDATEs and DELETEs) with ChangeType column
    - CHRONOLOGICAL ORDER: Sorts events by ts_ms within each batch to ensure INSERT->UPDATE->DELETE sequence
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
            print(f"[SCD4] Cleared Spark cache for batch {batch_id}")
            
            # CRITICAL: Sort batch by timestamp to ensure chronological processing
            # This ensures INSERT -> UPDATE -> DELETE order even if Kafka delivers out of order
            batch_df = batch_df.orderBy(col("parsed.payload.ts_ms").asc())
            print(f"[SCD4] Ordered batch {batch_id} by timestamp for chronological processing")
                
            # Extract CDC data based on table type with both BEFORE and AFTER values for SCD Type 4
            if table_name == "Customers":
                # Map to DimCustomer format - capture both before and after for history
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CustomerID")).otherwise(col("parsed.payload.before.CustomerID")).alias("CustomerID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("CustomerName"),
                    # For history table - capture BEFORE values for UPDATE/DELETE
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.CustomerID")).alias("CustomerID_Before"),
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.Name")).alias("CustomerName_Before"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp"),  # Preserve timestamp for ordering
                    current_timestamp().alias("ChangeTimestamp")
                ).filter(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CustomerID")).otherwise(col("parsed.payload.before.CustomerID")).isNotNull()
                ).orderBy(col("event_timestamp").asc())  # Maintain chronological order
                
            elif table_name == "Sellers":
                # Map to DimSeller format - capture both before and after for history
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.SellerID")).otherwise(col("parsed.payload.before.SellerID")).alias("SellerID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("SellerName"),
                    # For history table - capture BEFORE values for UPDATE/DELETE
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.SellerID")).alias("SellerID_Before"),
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.Name")).alias("SellerName_Before"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp"),  # Preserve timestamp for ordering
                    current_timestamp().alias("ChangeTimestamp")
                ).filter(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.SellerID")).otherwise(col("parsed.payload.before.SellerID")).isNotNull()
                ).orderBy(col("event_timestamp").asc())  # Maintain chronological order
                
            elif table_name == "ProductCategories":
                # Map to DimCategory format - capture both before and after for history
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryID")).otherwise(col("parsed.payload.before.CategoryID")).alias("CategoryID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryName")).otherwise(col("parsed.payload.before.CategoryName")).alias("CategoryName"),
                    # For history table - capture BEFORE values for UPDATE/DELETE
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.CategoryID")).alias("CategoryID_Before"),
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.CategoryName")).alias("CategoryName_Before"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp"),  # Preserve timestamp for ordering
                    current_timestamp().alias("ChangeTimestamp")
                ).filter(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryID")).otherwise(col("parsed.payload.before.CategoryID")).isNotNull()
                ).orderBy(col("event_timestamp").asc())  # Maintain chronological order
                
            elif table_name == "Products":
                # Map to DimProduct format - capture both before and after for history
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ProductID")).otherwise(col("parsed.payload.before.ProductID")).alias("ProductID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("ProductName"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryID")).otherwise(col("parsed.payload.before.CategoryID")).alias("CategoryID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.SellerID")).otherwise(col("parsed.payload.before.SellerID")).alias("SellerID"),
                    # For history table - capture BEFORE values for UPDATE/DELETE
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.ProductID")).alias("ProductID_Before"),
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.Name")).alias("ProductName_Before"),
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.CategoryID")).alias("CategoryID_Before"),
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.SellerID")).alias("SellerID_Before"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp"),  # Preserve timestamp for ordering
                    current_timestamp().alias("ChangeTimestamp")
                ).filter(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ProductID")).otherwise(col("parsed.payload.before.ProductID")).isNotNull()
                ).orderBy(col("event_timestamp").asc())  # Maintain chronological order
                
            elif table_name == "OrderStatus":
                # Map to DimOrderStatus format - capture both before and after for history
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusID")).otherwise(col("parsed.payload.before.StatusID")).alias("StatusID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusName")).otherwise(col("parsed.payload.before.StatusName")).alias("StatusName"),
                    # For history table - capture BEFORE values for UPDATE/DELETE
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.StatusID")).alias("StatusID_Before"),
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.StatusName")).alias("StatusName_Before"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp"),  # Preserve timestamp for ordering
                    current_timestamp().alias("ChangeTimestamp")
                ).filter(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusID")).otherwise(col("parsed.payload.before.StatusID")).isNotNull()
                ).orderBy(col("event_timestamp").asc())  # Maintain chronological order
                
            else:
                print(f"[WARNING] Unknown table: {table_name}")
                return
            
            # Get primary key column name for each table
            pk_columns = {
                "Customers": "CustomerID",
                "Sellers": "SellerID", 
                "ProductCategories": "CategoryID",
                "Products": "ProductID",
                "OrderStatus": "StatusID"
            }
            
            pk_col = pk_columns.get(table_name)
            if not pk_col:
                print(f"[ERROR] No primary key defined for {table_name}")
                return
            
            # Ensure cdc_df is initialized
            if 'cdc_df' not in locals():
                print(f"[ERROR] CDC dataframe not initialized for {table_name}")
                return
            
            # CRITICAL: Process CDC events sequentially in chronological order
            # Instead of grouping by operation type, process each event in timestamp order
            print(f"[SCD4] Processing {cdc_df.count()} CDC events sequentially in chronological order")
            
            # Define paths for current and history tables
            current_path = gold_path
            history_path = gold_path.replace("/Dim", "/DimHistory_")
            
            # =========================
            # PROCESS CURRENT TABLE (latest records only) - UPDATE EXISTING TABLE ONLY
            # =========================
            # Read existing current table and cache it (table must already exist)
            current_df = spark.read.format("parquet").load(current_path)
            current_df.cache()
            print(f"[SCD4] Loaded existing Current {table_name} with {current_df.count()} records")
            
            result_current = current_df
            history_records_list = []
            
            # Process each CDC event sequentially in chronological order
            cdc_events = cdc_df.collect()  # Collect to process sequentially
            
            for event in cdc_events:
                operation = event["operation"]
                print(f"[SCD4] Processing {operation} event for {pk_col}={event[pk_col]}")
                
                if operation == "d":  # DELETE
                    # Remove from current table
                    result_current = result_current.filter(col(pk_col) != event[pk_col])
                    print(f"[SCD4] Deleted {pk_col}={event[pk_col]} from Current {table_name}")
                    
                    # Add to history (BEFORE values for DELETE)
                    if table_name == "Customers":
                        history_record = {
                            "CustomerID": event["CustomerID_Before"],
                            "CustomerName": event["CustomerName_Before"],
                            "ChangeType": "DELETE",
                            "ChangeTimestamp": event["ChangeTimestamp"]
                        }
                    elif table_name == "Sellers":
                        history_record = {
                            "SellerID": event["SellerID_Before"],
                            "SellerName": event["SellerName_Before"],
                            "ChangeType": "DELETE",
                            "ChangeTimestamp": event["ChangeTimestamp"]
                        }
                    elif table_name == "ProductCategories":
                        history_record = {
                            "CategoryID": event["CategoryID_Before"],
                            "CategoryName": event["CategoryName_Before"],
                            "ChangeType": "DELETE",
                            "ChangeTimestamp": event["ChangeTimestamp"]
                        }
                    elif table_name == "Products":
                        history_record = {
                            "ProductID": event["ProductID_Before"],
                            "ProductName": event["ProductName_Before"],
                            "CategoryID": event["CategoryID_Before"],
                            "SellerID": event["SellerID_Before"],
                            "ChangeType": "DELETE",
                            "ChangeTimestamp": event["ChangeTimestamp"]
                        }
                    elif table_name == "OrderStatus":
                        history_record = {
                            "StatusID": event["StatusID_Before"],
                            "StatusName": event["StatusName_Before"],
                            "ChangeType": "DELETE",
                            "ChangeTimestamp": event["ChangeTimestamp"]
                        }
                    
                    if history_record and event[f"{pk_col}_Before"] is not None:
                        history_records_list.append(history_record)
                        
                elif operation == "u":  # UPDATE
                    # Remove old record from current table
                    result_current = result_current.filter(col(pk_col) != event[pk_col])
                    
                    # Add updated record to current table (AFTER values)
                    if table_name == "Customers":
                        new_record = spark.createDataFrame([{
                            "CustomerID": event["CustomerID"],
                            "CustomerName": event["CustomerName"]
                        }])
                        history_record = {
                            "CustomerID": event["CustomerID_Before"],
                            "CustomerName": event["CustomerName_Before"],
                            "ChangeType": "UPDATE",
                            "ChangeTimestamp": event["ChangeTimestamp"]
                        }
                    elif table_name == "Sellers":
                        new_record = spark.createDataFrame([{
                            "SellerID": event["SellerID"],
                            "SellerName": event["SellerName"]
                        }])
                        history_record = {
                            "SellerID": event["SellerID_Before"],
                            "SellerName": event["SellerName_Before"],
                            "ChangeType": "UPDATE",
                            "ChangeTimestamp": event["ChangeTimestamp"]
                        }
                    elif table_name == "ProductCategories":
                        new_record = spark.createDataFrame([{
                            "CategoryID": event["CategoryID"],
                            "CategoryName": event["CategoryName"]
                        }])
                        history_record = {
                            "CategoryID": event["CategoryID_Before"],
                            "CategoryName": event["CategoryName_Before"],
                            "ChangeType": "UPDATE",
                            "ChangeTimestamp": event["ChangeTimestamp"]
                        }
                    elif table_name == "Products":
                        new_record = spark.createDataFrame([{
                            "ProductID": event["ProductID"],
                            "ProductName": event["ProductName"],
                            "CategoryID": event["CategoryID"],
                            "SellerID": event["SellerID"]
                        }])
                        history_record = {
                            "ProductID": event["ProductID_Before"],
                            "ProductName": event["ProductName_Before"],
                            "CategoryID": event["CategoryID_Before"],
                            "SellerID": event["SellerID_Before"],
                            "ChangeType": "UPDATE",
                            "ChangeTimestamp": event["ChangeTimestamp"]
                        }
                    elif table_name == "OrderStatus":
                        new_record = spark.createDataFrame([{
                            "StatusID": event["StatusID"],
                            "StatusName": event["StatusName"]
                        }])
                        history_record = {
                            "StatusID": event["StatusID_Before"],
                            "StatusName": event["StatusName_Before"],
                            "ChangeType": "UPDATE",
                            "ChangeTimestamp": event["ChangeTimestamp"]
                        }
                    
                    result_current = result_current.union(new_record)
                    print(f"[SCD4] Updated {pk_col}={event[pk_col]} in Current {table_name}")
                    
                    if history_record and event[f"{pk_col}_Before"] is not None:
                        history_records_list.append(history_record)
                        
                elif operation == "c":  # INSERT
                    # Add new record to current table (AFTER values)
                    if table_name == "Customers":
                        new_record = spark.createDataFrame([{
                            "CustomerID": event["CustomerID"],
                            "CustomerName": event["CustomerName"]
                        }])
                    elif table_name == "Sellers":
                        new_record = spark.createDataFrame([{
                            "SellerID": event["SellerID"],
                            "SellerName": event["SellerName"]
                        }])
                    elif table_name == "ProductCategories":
                        new_record = spark.createDataFrame([{
                            "CategoryID": event["CategoryID"],
                            "CategoryName": event["CategoryName"]
                        }])
                    elif table_name == "Products":
                        new_record = spark.createDataFrame([{
                            "ProductID": event["ProductID"],
                            "ProductName": event["ProductName"],
                            "CategoryID": event["CategoryID"],
                            "SellerID": event["SellerID"]
                        }])
                    elif table_name == "OrderStatus":
                        new_record = spark.createDataFrame([{
                            "StatusID": event["StatusID"],
                            "StatusName": event["StatusName"]
                        }])
                    
                    result_current = result_current.union(new_record)
                    print(f"[SCD4] Inserted {pk_col}={event[pk_col]} to Current {table_name}")

            # Write updated current table (overwrite mode)
            partitions_current = smart_partitioning_write(result_current, current_path, f"Current_{table_name}", "overwrite")
            current_df.unpersist()
            
            # =========================
            # PROCESS HISTORY TABLE (captures UPDATE and DELETE changes only)
            # =========================
            
            if history_records_list:
                # Create DataFrame from history records list
                history_records = spark.createDataFrame(history_records_list)
                
                try:
                    # Check if history table exists
                    existing_history = spark.read.format("parquet").load(history_path)
                    # History table exists, append new records
                    partitions_history = smart_partitioning_write(history_records, history_path, f"History_{table_name}", "append")
                    print(f"[SCD4] Appended {len(history_records_list)} change records to existing History {table_name}")
                    
                except Exception as e:
                    # History table doesn't exist, create new one
                    partitions_history = smart_partitioning_write(history_records, history_path, f"History_{table_name}", "append")
                    print(f"[SCD4] Created new History {table_name} with {len(history_records_list)} change records")
            
            # Clear cache after processing
            spark.catalog.clearCache()
            
            total_changes = batch_df.count()
            print(f"[SCD4] Completed processing {total_changes} CDC events for {table_name}")
            print(f"[SCD4] Current table: {current_path}")
            print(f"[SCD4] History table: {history_path}")
    
        # Write stream using foreachBatch
        query = (df_parsed.writeStream
                .foreachBatch(process_batch)
                .trigger(processingTime="10 seconds")
                .option("checkpointLocation", f"/tmp/checkpoints_gold/cdc_scd4_{gold_path.split('/')[-1]}")
                .start())
        
        return query
    
    return None

def main():
    # Create Spark session
    spark = create_spark_session()
    
    # Configure Azure storage for gold layer
    storage_account_gold = "mygold"
    gold_container = "gold-final"
    
    # Set Azure credentials
    spark.conf.set(
        "fs.azure.account.key.mygold.dfs.core.windows.net",
        "wRPXTwWCVxWwUpavEh62A5wzLdUvRTGeB3tZKP3eRbig7ca8ZN51l0kWS32kcbH/ddQ/jNXBzqDC+AStOzXlyw=="
    )
    
    # OPTION: Set to True if you want to clear checkpoints and start fresh
    # This ensures only NEW CDC events after starting the code are processed
    CLEAR_CHECKPOINTS = True  # Set to False to resume from where you left off
    
    if CLEAR_CHECKPOINTS:
        print("[SCD4] Starting with fresh checkpoints - only NEW CDC events will be processed")
        print("[SCD4] SCD Type 4 Implementation:")
        print("[SCD4] - Current tables: Latest version of each record")
        print("[SCD4] - History tables: All UPDATE and DELETE changes with ChangeType column")
    else:
        print("[SCD4] Resuming from existing checkpoints - may process some old events")
    
    # List of topics that affect dimension tables - map to SCD Type 4 dimension names
    dimension_topics = {
        "online_store.online_store.Customers": "DimCustomer",
        "online_store.online_store.Sellers": "DimSeller",
        "online_store.online_store.ProductCategories": "DimCategory", 
        "online_store.online_store.Products": "DimProduct",
        "online_store.online_store.OrderStatus": "DimOrderStatus"
    }
    
    # Start streaming for each dimension-relevant topic
    queries = []
    for topic, dim_table in dimension_topics.items():
        # Extract source table name from topic
        source_table = topic.split(".")[-1]
        gold_path = f"abfss://{gold_container}@{storage_account_gold}.dfs.core.windows.net/{dim_table}"
        
        # Clear checkpoints if requested (ensures fresh start)
        clear_checkpoints_if_needed(source_table, CLEAR_CHECKPOINTS)
        
        # Get schema for this table
        schema = table_schemas.get(source_table)
        
        # Process CDC events and apply to gold dimension tables using SCD Type 4
        kafka_df_cdc = read_from_kafka(spark, topic, "gold")
        query_cdc = process_cdc_to_table_scd4(kafka_df_cdc, gold_path, source_table, schema, spark)
        if query_cdc:
            print(f"[SCD4] Processing CDC events for {source_table} -> {dim_table} (Current + History)")
            print(f"[SCD4] Current table: {gold_path}")
            print(f"[SCD4] History table: {gold_path.replace('/Dim', '/DimHistory_')}")
            queries.append(query_cdc)
    
    # Wait for all queries to terminate
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()
    
    # Manual SCD Type 4 checkpoint cleanup commands (if needed):
    # rm -rf /tmp/checkpoints_gold/cdc_scd4_DimCustomer
    # rm -rf /tmp/checkpoints_gold/cdc_scd4_DimSeller
    # rm -rf /tmp/checkpoints_gold/cdc_scd4_DimCategory
    # rm -rf /tmp/checkpoints_gold/cdc_scd4_DimProduct
    # rm -rf /tmp/checkpoints_gold/cdc_scd4_DimOrderStatus
