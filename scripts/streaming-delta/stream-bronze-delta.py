import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from delta import *

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
    spark_temp_dir = "/tmp/spark-workspace"
    checkpoint_dir = "/tmp/spark-checkpoints"
    
    os.makedirs(spark_temp_dir, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    return (SparkSession.builder
            .appName("CDC to Bronze Delta Layer")
            .config("spark.jars", jar_path)  # Use local JARs for Azure storage
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "io.delta:delta-spark_2.12:3.0.0")  # Updated Delta Lake package
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            .config("spark.hadoop.fs.azure.account.key.mybronze.dfs.core.windows.net",
                   "c5etqTidViezB/4ukOAALy23HeMBsJJ8g+2nFaIdbC7E9PhLw0y2YIA1ItjutpqS1/8Ga8fw40mR+ASt2T+/sw==")
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

# Define Debezium message schemas for all tables

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
    """
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9093")
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load())

def clear_checkpoints_if_needed(table_name, clear_checkpoints=False):
    """
    Clear checkpoint directories if requested
    """
    if clear_checkpoints:
        import shutil
        delta_checkpoint = f"/tmp/spark-checkpoints/delta_{table_name}"
        
        for checkpoint_path in [delta_checkpoint]:
            try:
                if os.path.exists(checkpoint_path):
                    shutil.rmtree(checkpoint_path)
                    print(f"[CLEANUP] Cleared checkpoint: {checkpoint_path}")
            except Exception as e:
                print(f"[WARNING] Could not clear checkpoint {checkpoint_path}: {e}")

def process_cdc_to_delta_table(df, delta_path, table_name, schema=None, spark=None, recreate_tables=False):
    """
    Process CDC events and apply them to Delta Lake table using MERGE operations
    This is MUCH more efficient than overwriting entire files!
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
            
            print(f"[DEBUG] Processing batch {batch_id} with {batch_df.count()} events")
            
            # CRITICAL: Sort batch by timestamp to ensure chronological processing
            batch_df = batch_df.orderBy(col("parsed.payload.ts_ms").asc())
            print(f"[DEBUG] Ordered batch {batch_id} by timestamp for chronological processing")
                
            # Extract CDC data based on table type
            if table_name == "Customers":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CustomerID")).otherwise(col("parsed.payload.before.CustomerID")).alias("CustomerID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("Name"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Email")).otherwise(col("parsed.payload.before.Email")).alias("Email"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.PhoneNumber")).otherwise(col("parsed.payload.before.PhoneNumber")).alias("PhoneNumber"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp")
                ).orderBy(col("event_timestamp").asc())
            elif table_name == "Sellers":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.SellerID")).otherwise(col("parsed.payload.before.SellerID")).alias("SellerID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("Name"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Email")).otherwise(col("parsed.payload.before.Email")).alias("Email"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.PhoneNumber")).otherwise(col("parsed.payload.before.PhoneNumber")).alias("PhoneNumber"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp")
                ).orderBy(col("event_timestamp").asc())
            elif table_name == "ProductCategories":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryID")).otherwise(col("parsed.payload.before.CategoryID")).alias("CategoryID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryName")).otherwise(col("parsed.payload.before.CategoryName")).alias("CategoryName"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CategoryDescription")).otherwise(col("parsed.payload.before.CategoryDescription")).alias("CategoryDescription"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp")
                ).orderBy(col("event_timestamp").asc())
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
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp")
                ).orderBy(col("event_timestamp").asc())
            elif table_name == "OrderStatus":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusID")).otherwise(col("parsed.payload.before.StatusID")).alias("StatusID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusName")).otherwise(col("parsed.payload.before.StatusName")).alias("StatusName"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusDescription")).otherwise(col("parsed.payload.before.StatusDescription")).alias("StatusDescription"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp")
                ).orderBy(col("event_timestamp").asc())
            elif table_name == "Orders":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderID")).otherwise(col("parsed.payload.before.OrderID")).alias("OrderID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderNumber")).otherwise(col("parsed.payload.before.OrderNumber")).alias("OrderNumber"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.TotalAmount")).otherwise(col("parsed.payload.before.TotalAmount")).alias("TotalAmount"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusID")).otherwise(col("parsed.payload.before.StatusID")).alias("StatusID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CustomerID")).otherwise(col("parsed.payload.before.CustomerID")).alias("CustomerID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp")
                ).orderBy(col("event_timestamp").asc())
            elif table_name == "OrderItems":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderItemID")).otherwise(col("parsed.payload.before.OrderItemID")).alias("OrderItemID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderID")).otherwise(col("parsed.payload.before.OrderID")).alias("OrderID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ProductID")).otherwise(col("parsed.payload.before.ProductID")).alias("ProductID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Quantity")).otherwise(col("parsed.payload.before.Quantity")).alias("Quantity"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CurrentPrice")).otherwise(col("parsed.payload.before.CurrentPrice")).alias("CurrentPrice"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp")
                ).orderBy(col("event_timestamp").asc())
            elif table_name == "Reasons":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ReasonID")).otherwise(col("parsed.payload.before.ReasonID")).alias("ReasonID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderID")).otherwise(col("parsed.payload.before.OrderID")).alias("OrderID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ReasonType")).otherwise(col("parsed.payload.before.ReasonType")).alias("ReasonType"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ReasonDescription")).otherwise(col("parsed.payload.before.ReasonDescription")).alias("ReasonDescription"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp")
                ).orderBy(col("event_timestamp").asc())
            elif table_name == "Payments":
                cdc_df = batch_df.select(
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.PaymentID")).otherwise(col("parsed.payload.before.PaymentID")).alias("PaymentID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderID")).otherwise(col("parsed.payload.before.OrderID")).alias("OrderID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.PaymentMethodID")).otherwise(col("parsed.payload.before.PaymentMethodID")).alias("PaymentMethodID"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Amount")).otherwise(col("parsed.payload.before.Amount")).alias("Amount"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                    when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                    col("parsed.payload.op").alias("operation"),
                    col("parsed.payload.ts_ms").alias("event_timestamp")
                ).orderBy(col("event_timestamp").asc())
            else:
                print(f"[WARNING] Table {table_name} not yet implemented for Delta processing")
                return
            
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
                print(f"[ERROR] No primary key defined for table {table_name}")
                return
            
            try:
                # Process each operation type in chronological order
                inserts = cdc_df.filter(col("operation") == "c").drop("operation", "event_timestamp")
                updates = cdc_df.filter(col("operation") == "u").drop("operation", "event_timestamp")
                deletes = cdc_df.filter(col("operation") == "d").select(pk_col)
                
                # Check if Delta table exists
                from delta.tables import DeltaTable
                
                try:
                    delta_table = DeltaTable.forPath(spark, delta_path)
                    table_exists = True
                    current_count = delta_table.toDF().count()
                    print(f"[DELTA] Found existing Delta table at {delta_path} with {current_count} records")
                    
                    # Check if we should recreate the table
                    if recreate_tables:
                        print(f"[WARNING] RECREATE_DELTA_TABLES=True - This will DELETE {current_count} existing records in {table_name}!")
                        if current_count > 0:
                            print(f"[SAFETY] Table {table_name} has {current_count} records that will be lost!")
                            print(f"[SAFETY] If this includes your initial batch data, you should set RECREATE_DELTA_TABLES=False")
                            # Uncomment these lines if you want a safety delay
                            # print("[SAFETY] Sleeping 5 seconds to allow cancellation...")
                            # import time
                            # time.sleep(5)
                        
                        # Delete existing data and recreate
                        delta_table.delete("1=1")  # Delete all records
                        table_exists = False  # Treat as new table
                        print(f"[DELTA] Recreated Delta table {table_name} - all existing data deleted")
                    else:
                        print(f"[SAFE] Preserving existing {table_name} table with {current_count} records")
                        
                except Exception:
                    table_exists = False
                    print(f"[DELTA] Creating new Delta table at {delta_path}")
                
                if table_exists:
                    # EFFICIENT DELTA OPERATIONS - No full table rewrites!
                    
                    # 1. Handle INSERTS using Delta MERGE (upsert)
                    if inserts.count() > 0:
                        print(f"[DELTA] Merging {inserts.count()} inserts")
                        delta_table.alias("target").merge(
                            inserts.alias("source"),
                            f"target.{pk_col} = source.{pk_col}"
                        ).whenMatchedUpdateAll()\
                         .whenNotMatchedInsertAll()\
                         .execute()
                    
                    # 2. Handle UPDATES using Delta MERGE
                    if updates.count() > 0:
                        print(f"[DELTA] Merging {updates.count()} updates")
                        delta_table.alias("target").merge(
                            updates.alias("source"),
                            f"target.{pk_col} = source.{pk_col}"
                        ).whenMatchedUpdateAll()\
                         .execute()
                    
                    # 3. Handle DELETES using Delta DELETE
                    if deletes.count() > 0:
                        print(f"[DELTA] Deleting {deletes.count()} records")
                        delete_condition = " OR ".join([f"{pk_col} = {row[pk_col]}" for row in deletes.collect()])
                        delta_table.delete(delete_condition)
                        
                else:
                    # Create new Delta table with initial data (inserts + updates only)
                    if inserts.count() > 0 or updates.count() > 0:
                        initial_data = inserts.union(updates) if inserts.count() > 0 and updates.count() > 0 else (inserts if inserts.count() > 0 else updates)
                        initial_data.write.format("delta").mode("overwrite").save(delta_path)
                        print(f"[DELTA] Created new Delta table {table_name} with {initial_data.count()} records")
                
                # Get final record count
                final_table = DeltaTable.forPath(spark, delta_path)
                final_count = final_table.toDF().count()
                print(f"[DELTA] {table_name} now has {final_count} total records")
                
            except Exception as e:
                print(f"[ERROR] Delta operation failed for {table_name}: {e}")
                import traceback
                traceback.print_exc()
    
        # Write stream using foreachBatch
        query = (df_parsed.writeStream
                .foreachBatch(process_batch)
                .trigger(processingTime="10 seconds")
                .option("checkpointLocation", f"/tmp/spark-checkpoints/delta_{table_name}")
                .start())
        
        return query
    
    return None

def check_existing_delta_tables(spark, storage_account_bronze, bronze_delta, tables_to_check):
    """
    Check if Delta tables already exist and show their record counts
    This helps you see if you have initial batch data that could be lost
    """
    print("\n" + "="*60)
    print("DELTA TABLE STATUS CHECK")
    print("="*60)
    
    existing_tables = {}
    for table_name in tables_to_check:
        delta_path = f"abfss://{bronze_delta}@{storage_account_bronze}.dfs.core.windows.net/{table_name}"
        try:
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(spark, delta_path)
            record_count = delta_table.toDF().count()
            existing_tables[table_name] = record_count
            print(f"✅ {table_name}: {record_count} records")
        except Exception as e:
            existing_tables[table_name] = 0
            print(f"❌ {table_name}: Table does not exist")
    
    total_records = sum(existing_tables.values())
    print("-" * 60)
    print(f"TOTAL EXISTING RECORDS: {total_records}")
    
    if total_records > 0:
        print("\n⚠️  WARNING: You have existing data in Delta tables!")
        print("If RECREATE_DELTA_TABLES=True, ALL this data will be DELETED!")
        print("This likely includes your initial batch-loaded data.")
        print("Set RECREATE_DELTA_TABLES=False to preserve existing data.")
    else:
        print("\n✅ No existing data found - safe to recreate tables")
        
    print("="*60 + "\n")
    return existing_tables

def main():
    # Create Spark session with Delta Lake support
    spark = create_spark_session()
    
    try:
        # Configure Azure storage for Delta tables
        storage_account_bronze = "mybronze"
        bronze_delta = "bronze-delta"
        
        # Set Azure credentials
        spark.conf.set(
            "fs.azure.account.key.mybronze.dfs.core.windows.net",
            "c5etqTidViezB/4ukOAALy23HeMBsJJ8g+2nFaIdbC7E9PhLw0y2YIA1ItjutpqS1/8Ga8fw40mR+ASt2T+/sw=="
        )
        
        # OPTION: Set to True if you want to clear checkpoints and start fresh
        CLEAR_CHECKPOINTS = False  # Changed to False to avoid re-processing old events
        # OPTION: Set to True ONLY if you want to delete existing Delta tables and recreate them
        # WARNING: This will DELETE ALL your batch-loaded initial data!
        RECREATE_DELTA_TABLES = False  # Changed to False to preserve existing data
        
        if CLEAR_CHECKPOINTS:
            print("[CONFIG] Starting with fresh checkpoints - only NEW CDC events will be processed")
        else:
            print("[CONFIG] Resuming from existing checkpoints - may process some old events")
            
        if RECREATE_DELTA_TABLES:
            print("[WARNING] Will recreate Delta tables if they exist - THIS WILL DELETE YOUR INITIAL BATCH DATA!")
            # Safety confirmation - uncomment if you really want this behavior
            # print("[SAFETY] Sleeping 10 seconds - press Ctrl+C to cancel if this was a mistake...")
            # import time
            # time.sleep(10)
        else:
            print("[SAFE] Preserving existing Delta tables - will only add new CDC events")
        
        # List of topics to process
        topics = [
            "online_store.online_store.Customers",
            "online_store.online_store.Sellers", 
            "online_store.online_store.Products",
            # Add more tables as needed
        ]
        
        # Extract table names for checking
        table_names = [topic.split(".")[-1] for topic in topics]
        
        # SAFETY CHECK: Check existing Delta tables before proceeding
        existing_data = check_existing_delta_tables(spark, storage_account_bronze, bronze_delta, table_names)
        
        # Additional safety check
        total_existing = sum(existing_data.values())
        if RECREATE_DELTA_TABLES and total_existing > 0:
            print(f"\n🚨 DANGER: RECREATE_DELTA_TABLES=True will delete {total_existing} existing records!")
            print("This will permanently destroy your initial batch data!")
            print("To cancel: Press Ctrl+C now")
            print("To preserve data: Set RECREATE_DELTA_TABLES=False in the code")
            print("\nContinuing in 10 seconds...")
            import time
            time.sleep(10)
        
        # Start streaming for each topic
        queries = []
        for topic in topics:
            # Extract table name from topic
            table_name = topic.split(".")[-1]
            delta_path = f"abfss://{bronze_delta}@{storage_account_bronze}.dfs.core.windows.net/{table_name}"
            
            # Clear checkpoints if requested
            clear_checkpoints_if_needed(table_name, CLEAR_CHECKPOINTS)
            
            # Get schema for this table
            schema = table_schemas.get(table_name)
            
            # Process CDC events to Delta table
            kafka_df = read_from_kafka(spark, topic, "delta")
            query = process_cdc_to_delta_table(kafka_df, delta_path, table_name, schema, spark, RECREATE_DELTA_TABLES)
            if query:
                print(f"[READY] Processing CDC events for table: {table_name} to Delta at {delta_path}")
                queries.append(query)
        
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
    
    # Manual checkpoint cleanup commands (if needed):
    # rm -rf /tmp/spark-checkpoints/delta_*
    # Or run: python3 setup_directories.py
