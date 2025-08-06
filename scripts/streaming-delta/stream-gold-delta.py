import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from delta import *
from delta.tables import DeltaTable

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
    
    # Create dedicated directories for Spark
    import tempfile
    
    # Create persistent temp directories
    spark_temp_dir = "/tmp/spark-workspace-gold"
    checkpoint_dir = "/tmp/spark-checkpoints-gold"
    
    os.makedirs(spark_temp_dir, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    return (SparkSession.builder
            .appName("CDC to Gold Delta Layer - SCD Type 4 Dimensions")
            .config("spark.jars", jar_path)  # Use local JARs for Azure storage
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "io.delta:delta-spark_2.12:3.0.0")  # Updated Delta Lake package
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            .config("spark.hadoop.fs.azure.account.key.mygold.dfs.core.windows.net",
                   "YOUR_GOLD_STORAGE_ACCOUNT_KEY")  # Replace with your actual key
            .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
            .config("spark.sql.streaming.minBatchesToRetain", "10")
            .config("spark.sql.streaming.pollingDelay", "1s")
            .config("spark.local.dir", spark_temp_dir)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.unsafe", "false")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")  # Allow schema evolution
            .config("spark.sql.parquet.enableVectorizedReader", "false")  # Disable vectorized reader to avoid type conflicts
            .config("spark.master", "local[*]")  # Run locally with all available cores
            .getOrCreate())

# Define Debezium message schemas for tables we need for gold layer

# Base structure for common fields
def create_base_fields():
    return [
        StructField("CreatedAt", StringType(), True),  # Debezium sends timestamps as strings
        StructField("UpdatedAt", StringType(), True)   # Debezium sends timestamps as strings
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

def read_from_kafka(spark, kafka_topic, stream_type="gold-delta"):
    """
    Read from Kafka with different checkpoint locations for different stream types
    stream_type: 'gold-delta' for CDC processing to gold Delta layer dimensions
    """
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "YOUR_KAFKA_HOST:9093") # Spark local, if Spark in container use kafka:9093
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "latest")  # Only process NEW messages after starting
            .option("failOnDataLoss", "false")  # Don't fail if some data is lost
            .load())

def clear_checkpoints_if_needed(table_name, clear_checkpoints=False):
    """
    Clear checkpoint directories if requested - for SCD Type 4 gold Delta layer
    """
    if clear_checkpoints:
        import shutil
        gold_delta_checkpoint = f"/tmp/spark-checkpoints-gold/cdc_scd4_delta_{table_name}"
        
        for checkpoint_path in [gold_delta_checkpoint]:
            try:
                if os.path.exists(checkpoint_path):
                    shutil.rmtree(checkpoint_path)
                    print(f"[SCD4-DELTA] Cleared checkpoint: {checkpoint_path}")
            except Exception as e:
                print(f"[WARNING] Could not clear SCD4 Delta checkpoint {checkpoint_path}: {e}")

def process_cdc_to_delta_table_scd4(df, gold_delta_path, table_name, schema=None, spark=None, recreate_tables=False):
    """
    Process CDC events and apply them to Delta Lake gold tables using SCD Type 4 (Current + History tables)
    - Current table: always contains latest version of each record using Delta MERGE
    - History table: captures all changes (UPDATEs and DELETEs) with ChangeType column using Delta APPEND
    - ACID TRANSACTIONS: All operations are atomic and consistent
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
            
            print(f"[SCD4-DELTA] Processing batch {batch_id} with {batch_df.count()} events")
            
            # CRITICAL: Sort batch by timestamp to ensure chronological processing
            batch_df = batch_df.orderBy(col("parsed.payload.ts_ms").asc())
            print(f"[SCD4-DELTA] Ordered batch {batch_id} by timestamp for chronological processing")
                
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
                    col("parsed.payload.ts_ms").alias("event_timestamp"),
                    current_timestamp().alias("ChangeTimestamp")
                ).filter(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CustomerID")).otherwise(col("parsed.payload.before.CustomerID")).isNotNull()
                ).orderBy(col("event_timestamp").asc())
                
            elif table_name == "Sellers":
                # Map to DimSeller format - capture both before and after for history
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.SellerID")).otherwise(col("parsed.payload.before.SellerID")).alias("SellerID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("SellerName"),
                    # For history table - capture BEFORE values for UPDATE/DELETE
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.SellerID")).alias("SellerID_Before"),
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.Name")).alias("SellerName_Before"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp"),
                    current_timestamp().alias("ChangeTimestamp")
                ).filter(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.SellerID")).otherwise(col("parsed.payload.before.SellerID")).isNotNull()
                ).orderBy(col("event_timestamp").asc())
                
            elif table_name == "ProductCategories":
                # Map to DimCategory format - capture both before and after for history
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryID")).otherwise(col("parsed.payload.before.CategoryID")).alias("CategoryID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryName")).otherwise(col("parsed.payload.before.CategoryName")).alias("CategoryName"),
                    # For history table - capture BEFORE values for UPDATE/DELETE
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.CategoryID")).alias("CategoryID_Before"),
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.CategoryName")).alias("CategoryName_Before"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp"),
                    current_timestamp().alias("ChangeTimestamp")
                ).filter(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryID")).otherwise(col("parsed.payload.before.CategoryID")).isNotNull()
                ).orderBy(col("event_timestamp").asc())
                
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
                    col("parsed.payload.ts_ms").alias("event_timestamp"),
                    current_timestamp().alias("ChangeTimestamp")
                ).filter(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ProductID")).otherwise(col("parsed.payload.before.ProductID")).isNotNull()
                ).orderBy(col("event_timestamp").asc())
                
            elif table_name == "OrderStatus":
                # Map to DimOrderStatus format - capture both before and after for history
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusID")).otherwise(col("parsed.payload.before.StatusID")).alias("StatusID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusName")).otherwise(col("parsed.payload.before.StatusName")).alias("StatusName"),
                    # For history table - capture BEFORE values for UPDATE/DELETE
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.StatusID")).alias("StatusID_Before"),
                    when(col("parsed.payload.before").isNotNull(), col("parsed.payload.before.StatusName")).alias("StatusName_Before"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp"),
                    current_timestamp().alias("ChangeTimestamp")
                ).filter(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusID")).otherwise(col("parsed.payload.before.StatusID")).isNotNull()
                ).orderBy(col("event_timestamp").asc())
                
            else:
                print(f"[WARNING] Table {table_name} not yet implemented for Delta SCD4 processing")
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
                print(f"[ERROR] No primary key defined for table {table_name}")
                return
            
            # Define paths for current and history Delta tables
            current_path = gold_delta_path
            history_path = gold_delta_path.replace("/Dim", "/DimHistory_")
            
            try:
                # =========================
                # PROCESS CURRENT DELTA TABLE (latest records only)
                # =========================
                
                # Check if current Delta table exists
                try:
                    current_delta_table = DeltaTable.forPath(spark, current_path)
                    current_exists = True
                    current_count = current_delta_table.toDF().count()
                    print(f"[SCD4-DELTA] Found existing Current {table_name} with {current_count} records")
                    
                    # Check if we should recreate the table
                    if recreate_tables:
                        print(f"[WARNING] RECREATE_DELTA_TABLES=True - This will DELETE {current_count} existing records in Current {table_name}!")
                        if current_count > 0:
                            print(f"[SAFETY] Current table {table_name} has {current_count} records that will be lost!")
                        
                        # Delete existing data and recreate
                        current_delta_table.delete("1=1")  # Delete all records
                        current_exists = False  # Treat as new table
                        print(f"[SCD4-DELTA] Recreated Current {table_name} Delta table - all existing data deleted")
                    else:
                        print(f"[SAFE] Preserving existing Current {table_name} Delta table with {current_count} records")
                        
                except Exception:
                    current_exists = False
                    print(f"[SCD4-DELTA] Creating new Current {table_name} Delta table at {current_path}")
                
                # Check if history Delta table exists
                try:
                    history_delta_table = DeltaTable.forPath(spark, history_path)
                    history_exists = True
                    history_count = history_delta_table.toDF().count()
                    print(f"[SCD4-DELTA] Found existing History {table_name} with {history_count} records")
                    
                    # Check if we should recreate the table
                    if recreate_tables:
                        print(f"[WARNING] RECREATE_DELTA_TABLES=True - This will DELETE {history_count} existing records in History {table_name}!")
                        if history_count > 0:
                            print(f"[SAFETY] History table {table_name} has {history_count} records that will be lost!")
                        
                        # Delete existing data and recreate
                        history_delta_table.delete("1=1")  # Delete all records
                        history_exists = False  # Treat as new table
                        print(f"[SCD4-DELTA] Recreated History {table_name} Delta table - all existing data deleted")
                    else:
                        print(f"[SAFE] Preserving existing History {table_name} Delta table with {history_count} records")
                        
                except Exception:
                    history_exists = False
                    print(f"[SCD4-DELTA] Creating new History {table_name} Delta table at {history_path}")
                
                # Process each operation type sequentially in chronological order
                cdc_events = cdc_df.collect()  # Collect to process sequentially
                history_records_list = []
                
                for event in cdc_events:
                    operation = event["operation"]
                    print(f"[SCD4-DELTA] Processing {operation} event for {pk_col}={event[pk_col]}")
                    
                    if operation == "d":  # DELETE
                        if current_exists:
                            # Remove from current Delta table using Delta DELETE
                            current_delta_table.delete(f"{pk_col} = {event[pk_col]}")
                            print(f"[SCD4-DELTA] Deleted {pk_col}={event[pk_col]} from Current {table_name}")
                        
                        # Add to history (BEFORE values for DELETE)
                        if event[f"{pk_col}_Before"] is not None:
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
                            
                            history_records_list.append(history_record)
                            
                    elif operation == "u":  # UPDATE
                        # Prepare update data (AFTER values)
                        if table_name == "Customers":
                            update_data = spark.createDataFrame([{
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
                            update_data = spark.createDataFrame([{
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
                            update_data = spark.createDataFrame([{
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
                            update_data = spark.createDataFrame([{
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
                            update_data = spark.createDataFrame([{
                                "StatusID": event["StatusID"],
                                "StatusName": event["StatusName"]
                            }])
                            history_record = {
                                "StatusID": event["StatusID_Before"],
                                "StatusName": event["StatusName_Before"],
                                "ChangeType": "UPDATE",
                                "ChangeTimestamp": event["ChangeTimestamp"]
                            }
                        
                        if current_exists:
                            # Update in current Delta table using Delta MERGE
                            current_delta_table.alias("target").merge(
                                update_data.alias("source"),
                                f"target.{pk_col} = source.{pk_col}"
                            ).whenMatchedUpdateAll()\
                             .execute()
                        else:
                            # Create new current Delta table with this record
                            update_data.write.format("delta").mode("overwrite").save(current_path)
                            current_delta_table = DeltaTable.forPath(spark, current_path)
                            current_exists = True
                            
                        print(f"[SCD4-DELTA] Updated {pk_col}={event[pk_col]} in Current {table_name}")
                        
                        if event[f"{pk_col}_Before"] is not None:
                            history_records_list.append(history_record)
                            
                    elif operation == "c":  # INSERT
                        # Prepare insert data (AFTER values)
                        if table_name == "Customers":
                            insert_data = spark.createDataFrame([{
                                "CustomerID": event["CustomerID"],
                                "CustomerName": event["CustomerName"]
                            }])
                        elif table_name == "Sellers":
                            insert_data = spark.createDataFrame([{
                                "SellerID": event["SellerID"],
                                "SellerName": event["SellerName"]
                            }])
                        elif table_name == "ProductCategories":
                            insert_data = spark.createDataFrame([{
                                "CategoryID": event["CategoryID"],
                                "CategoryName": event["CategoryName"]
                            }])
                        elif table_name == "Products":
                            insert_data = spark.createDataFrame([{
                                "ProductID": event["ProductID"],
                                "ProductName": event["ProductName"],
                                "CategoryID": event["CategoryID"],
                                "SellerID": event["SellerID"]
                            }])
                        elif table_name == "OrderStatus":
                            insert_data = spark.createDataFrame([{
                                "StatusID": event["StatusID"],
                                "StatusName": event["StatusName"]
                            }])
                        
                        if current_exists:
                            # Insert into current Delta table using Delta MERGE (upsert)
                            current_delta_table.alias("target").merge(
                                insert_data.alias("source"),
                                f"target.{pk_col} = source.{pk_col}"
                            ).whenMatchedUpdateAll()\
                             .whenNotMatchedInsertAll()\
                             .execute()
                        else:
                            # Create new current Delta table with this record
                            insert_data.write.format("delta").mode("overwrite").save(current_path)
                            current_delta_table = DeltaTable.forPath(spark, current_path)
                            current_exists = True
                            
                        print(f"[SCD4-DELTA] Inserted {pk_col}={event[pk_col]} to Current {table_name}")

                # =========================
                # PROCESS HISTORY DELTA TABLE (captures UPDATE and DELETE changes only)
                # =========================
                
                if history_records_list:
                    # Create DataFrame from history records list
                    history_records = spark.createDataFrame(history_records_list)
                    
                    if history_exists:
                        # History Delta table exists, append new records using Delta MERGE
                        history_delta_table.alias("target").merge(
                            history_records.alias("source"),
                            "1=0"  # Always insert (never match) for history records
                        ).whenNotMatchedInsertAll()\
                         .execute()
                        print(f"[SCD4-DELTA] Appended {len(history_records_list)} change records to existing History {table_name}")
                    else:
                        # History Delta table doesn't exist, create new one
                        history_records.write.format("delta").mode("overwrite").save(history_path)
                        print(f"[SCD4-DELTA] Created new History {table_name} Delta table with {len(history_records_list)} change records")
                
                # Get final record counts
                if current_exists:
                    final_current = DeltaTable.forPath(spark, current_path)
                    final_current_count = final_current.toDF().count()
                    print(f"[SCD4-DELTA] Current {table_name} Delta table now has {final_current_count} records")
                
                try:
                    final_history = DeltaTable.forPath(spark, history_path)
                    final_history_count = final_history.toDF().count()
                    print(f"[SCD4-DELTA] History {table_name} Delta table now has {final_history_count} records")
                except:
                    print(f"[SCD4-DELTA] History {table_name} Delta table not yet created")
                
                total_changes = batch_df.count()
                print(f"[SCD4-DELTA] Completed processing {total_changes} CDC events for {table_name}")
                print(f"[SCD4-DELTA] Current Delta table: {current_path}")
                print(f"[SCD4-DELTA] History Delta table: {history_path}")
                
            except Exception as e:
                print(f"[ERROR] Delta SCD4 operation failed for {table_name}: {e}")
                import traceback
                traceback.print_exc()
    
        # Write stream using foreachBatch
        query = (df_parsed.writeStream
                .foreachBatch(process_batch)
                .trigger(processingTime="10 seconds")
                .option("checkpointLocation", f"/tmp/spark-checkpoints-gold/cdc_scd4_delta_{table_name}")
                .start())
        
        return query
    
    return None

def check_existing_gold_delta_tables(spark, storage_account_gold, gold_delta_container, dimension_topics):
    """
    Check if gold-delta tables already exist and show their record counts
    """
    print("\n" + "="*60)
    print("GOLD DELTA SCD TYPE 4 TABLE STATUS CHECK")
    print("="*60)
    
    existing_tables = {}
    for topic, dim_table in dimension_topics.items():
        # Check Current table
        current_path = f"abfss://{gold_delta_container}@{storage_account_gold}.dfs.core.windows.net/{dim_table}"
        history_path = current_path.replace("/Dim", "/DimHistory_")
        
        try:
            current_delta = DeltaTable.forPath(spark, current_path)
            current_count = current_delta.toDF().count()
            existing_tables[f"{dim_table}_Current"] = current_count
            print(f" {dim_table} (Current): {current_count} records")
        except Exception:
            existing_tables[f"{dim_table}_Current"] = 0
            print(f" {dim_table} (Current): Table does not exist")
        
        try:
            history_delta = DeltaTable.forPath(spark, history_path)
            history_count = history_delta.toDF().count()
            existing_tables[f"{dim_table}_History"] = history_count
            print(f" {dim_table} (History): {history_count} records")
        except Exception:
            existing_tables[f"{dim_table}_History"] = 0
            print(f" {dim_table} (History): Table does not exist")
    
    total_records = sum(existing_tables.values())
    print("-" * 60)
    print(f"TOTAL EXISTING RECORDS: {total_records}")
    
    if total_records > 0:
        print("\n  WARNING: You have existing data in gold-delta SCD4 tables!")
        print("If RECREATE_DELTA_TABLES=True, ALL this data will be DELETED!")
        print("Set RECREATE_DELTA_TABLES=False to preserve existing data.")
    else:
        print("\n No existing data found - safe to recreate tables")
        
    print("="*60 + "\n")
    return existing_tables

def main():
    # Create Spark session with Delta Lake support
    spark = create_spark_session()
    
    try:
        # Configure Azure storage for gold-delta layer
        storage_account_gold = "mygold"
        gold_delta_container = "gold-delta"
        
        # Set Azure credentials
        spark.conf.set(
            "fs.azure.account.key.mygold.dfs.core.windows.net",
            "YOUR_GOLD_STORAGE_ACCOUNT_KEY"
        )
        
        # OPTION: Set to True if you want to clear checkpoints and start fresh
        CLEAR_CHECKPOINTS = False  # Changed to False to avoid re-processing old events
        # OPTION: Set to True ONLY if you want to delete existing Delta tables and recreate them
        # WARNING: This will DELETE ALL your existing gold-delta data!
        RECREATE_DELTA_TABLES = False  # Changed to False to preserve existing data
        
        if CLEAR_CHECKPOINTS:
            print("[SCD4-DELTA] Starting with fresh checkpoints - only NEW CDC events will be processed")
        else:
            print("[SCD4-DELTA] Resuming from existing checkpoints - may process some old events")
            
        if RECREATE_DELTA_TABLES:
            print("[WARNING] Will recreate gold-delta SCD4 tables if they exist - THIS WILL DELETE YOUR EXISTING DATA!")
        else:
            print("[SAFE] Preserving existing gold-delta SCD4 tables - will only add new CDC events")
        
        print("[SCD4-DELTA] SCD Type 4 Implementation with Delta Lake:")
        print("[SCD4-DELTA] - Current tables: Latest version of each record with ACID transactions")
        print("[SCD4-DELTA] - History tables: All UPDATE and DELETE changes with ChangeType column")
        print("[SCD4-DELTA] - ACID COMPLIANCE: All operations are atomic and consistent")
        print("[SCD4-DELTA] - TIME TRAVEL: Full version history available")
        
        # List of topics that affect dimension tables - map to SCD Type 4 dimension names
        dimension_topics = {
            "online_store.online_store.Customers": "DimCustomer",
            "online_store.online_store.Sellers": "DimSeller",
            "online_store.online_store.ProductCategories": "DimCategory", 
            "online_store.online_store.Products": "DimProduct",
            "online_store.online_store.OrderStatus": "DimOrderStatus"
        }
        
        # SAFETY CHECK: Check existing gold-delta SCD4 tables before proceeding
        existing_data = check_existing_gold_delta_tables(spark, storage_account_gold, gold_delta_container, dimension_topics)
        
        # Additional safety check
        total_existing = sum(existing_data.values())
        if RECREATE_DELTA_TABLES and total_existing > 0:
            print(f"\n DANGER: RECREATE_DELTA_TABLES=True will delete {total_existing} existing records!")
            print("This will permanently destroy your gold-delta SCD4 data!")
            print("To cancel: Press Ctrl+C now")
            print("To preserve data: Set RECREATE_DELTA_TABLES=False in the code")
            print("\nContinuing in 10 seconds...")
            import time
            time.sleep(10)
        
        # Start streaming for each dimension-relevant topic
        queries = []
        for topic, dim_table in dimension_topics.items():
            # Extract source table name from topic
            source_table = topic.split(".")[-1]
            gold_delta_path = f"abfss://{gold_delta_container}@{storage_account_gold}.dfs.core.windows.net/{dim_table}"
            
            # Clear checkpoints if requested (ensures fresh start)
            clear_checkpoints_if_needed(source_table, CLEAR_CHECKPOINTS)
            
            # Get schema for this table
            schema = table_schemas.get(source_table)
            
            # Process CDC events and apply to gold dimension Delta tables using SCD Type 4
            kafka_df_cdc = read_from_kafka(spark, topic, "gold-delta")
            query_cdc = process_cdc_to_delta_table_scd4(kafka_df_cdc, gold_delta_path, source_table, schema, spark, RECREATE_DELTA_TABLES)
            if query_cdc:
                print(f"[SCD4-DELTA] Processing CDC events for {source_table} -> {dim_table} (Current + History)")
                print(f"[SCD4-DELTA] Current Delta table: {gold_delta_path}")
                print(f"[SCD4-DELTA] History Delta table: {gold_delta_path.replace('/Dim', '/DimHistory_')}")
                queries.append(query_cdc)
        
        # Wait for all queries to terminate
        for query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Received interrupt signal, stopping streams...")
        for query in queries:
            query.stop()
        print("[SHUTDOWN] All streams stopped")
    except Exception as e:
        print(f"[ERROR] Main execution failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("[CLEANUP] Stopping Spark session...")
        spark.stop()
        print("[CLEANUP] Spark session stopped")

if __name__ == "__main__":
    main()
    
    # Manual SCD Type 4 Delta checkpoint cleanup commands (if needed):
    # rm -rf /tmp/spark-checkpoints-gold/cdc_scd4_delta_*
    # Or run: python3 setup_directories.py
