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
            .appName("CDC to Bronze Layer - Standalone")
            .config("spark.jars", jar_path)  # Use local JARs for Azure storage
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")  # Use package for Kafka
            .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            .config("spark.hadoop.fs.azure.account.key.mybronze.dfs.core.windows.net",
                   "c5etqTidViezB/4ukOAALy23HeMBsJJ8g+2nFaIdbC7E9PhLw0y2YIA1ItjutpqS1/8Ga8fw40mR+ASt2T+/sw==")
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

def read_from_kafka(spark, kafka_topic):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9093") # Spark local, if Spark in container use kafka:9093
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")  # Don't fail if some data is lost
            .load())

def process_stream(df, bronze_path, table_name, schema=None):
    # Select value from Kafka, cast string
    df_processed = (df.selectExpr("CAST(value AS STRING) as json_data", 
                                  "timestamp as kafka_timestamp")
                    .withColumn("ingestion_timestamp", current_timestamp()))
    
    # Parse JSON if schema is provided 
    if schema:
        df_parsed = df_processed.withColumn("parsed", from_json(col("json_data"), schema))
        
        # Extract data from Debezium format based on table type
        if table_name == "Customers":
            df_processed = df_parsed.select(
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CustomerID")).otherwise(col("parsed.payload.before.CustomerID")).alias("CustomerID"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("Name"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Email")).otherwise(col("parsed.payload.before.Email")).alias("Email"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.PhoneNumber")).otherwise(col("parsed.payload.before.PhoneNumber")).alias("PhoneNumber"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                col("parsed.payload.op").alias("operation"),
                col("kafka_timestamp"),
                col("ingestion_timestamp")
            )
        elif table_name == "Sellers":
            df_processed = df_parsed.select(
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.SellerID")).otherwise(col("parsed.payload.before.SellerID")).alias("SellerID"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Name")).otherwise(col("parsed.payload.before.Name")).alias("Name"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Email")).otherwise(col("parsed.payload.before.Email")).alias("Email"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.PhoneNumber")).otherwise(col("parsed.payload.before.PhoneNumber")).alias("PhoneNumber"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                col("parsed.payload.op").alias("operation"),
                col("kafka_timestamp"),
                col("ingestion_timestamp")
            )
        elif table_name == "Products":
            df_processed = df_parsed.select(
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
                col("kafka_timestamp"),
                col("ingestion_timestamp")
            )
        elif table_name == "Orders":
            df_processed = df_parsed.select(
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderID")).otherwise(col("parsed.payload.before.OrderID")).alias("OrderID"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderNumber")).otherwise(col("parsed.payload.before.OrderNumber")).alias("OrderNumber"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.TotalAmount")).otherwise(col("parsed.payload.before.TotalAmount")).alias("TotalAmount"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.StatusID")).otherwise(col("parsed.payload.before.StatusID")).alias("StatusID"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CustomerID")).otherwise(col("parsed.payload.before.CustomerID")).alias("CustomerID"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                col("parsed.payload.op").alias("operation"),
                col("kafka_timestamp"),
                col("ingestion_timestamp")
            )
        elif table_name == "OrderItems":
            df_processed = df_parsed.select(
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderItemID")).otherwise(col("parsed.payload.before.OrderItemID")).alias("OrderItemID"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.OrderID")).otherwise(col("parsed.payload.before.OrderID")).alias("OrderID"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.ProductID")).otherwise(col("parsed.payload.before.ProductID")).alias("ProductID"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.Quantity")).otherwise(col("parsed.payload.before.Quantity")).alias("Quantity"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CurrentPrice")).otherwise(col("parsed.payload.before.CurrentPrice")).alias("CurrentPrice"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.CreatedAt")).otherwise(col("parsed.payload.before.CreatedAt")).alias("CreatedAt"),
                when(col("parsed.payload.after").isNotNull(), col("parsed.payload.after.UpdatedAt")).otherwise(col("parsed.payload.before.UpdatedAt")).alias("UpdatedAt"),
                col("parsed.payload.op").alias("operation"),
                col("kafka_timestamp"),
                col("ingestion_timestamp")
            )
        else:
            # For other tables, extract all fields generically
            df_processed = df_parsed.select(
                col("parsed.payload.after").alias("after_data"),
                col("parsed.payload.before").alias("before_data"),
                col("parsed.payload.op").alias("operation"),
                col("kafka_timestamp"),
                col("ingestion_timestamp")
            )
    
    # Write the stream to the bronze layer in Parquet format
    query = (df_processed.writeStream
             .format("parquet")
             .option("path", bronze_path)
             .trigger(processingTime="10 seconds")
             .option("checkpointLocation", f"/tmp/checkpoints/{bronze_path.split('/')[-1]}")
             .start())
    
    return query

def main():
    # Create Spark session
    spark = create_spark_session()
    
    # Configure Azure storage
    storage_account_bronze = "mybronze"
    bronze_stream = "bronze-stream"
    
    # Set Azure credentials
    spark.conf.set(
        "fs.azure.account.key.mybronze.dfs.core.windows.net",
        "c5etqTidViezB/4ukOAALy23HeMBsJJ8g+2nFaIdbC7E9PhLw0y2YIA1ItjutpqS1/8Ga8fw40mR+ASt2T+/sw=="
    )
    
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
        bronze_stream_path = f"abfss://{bronze_stream}@{storage_account_bronze}.dfs.core.windows.net/{table_name}"
        # Read from Kafka
        kafka_df = read_from_kafka(spark, topic)
        # Get schema for this table
        schema = table_schemas.get(table_name)
        # Process stream with table-specific handling
        query = process_stream(kafka_df, bronze_stream_path, table_name, schema)
        print(f"[READY] Listening for changes on table: {table_name} (topic: {topic}) and writing to {bronze_stream_path}")
        queries.append(query)
    
    # Wait for all queries to terminate
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()
