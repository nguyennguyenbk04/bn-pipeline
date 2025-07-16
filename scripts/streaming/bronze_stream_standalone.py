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

# Define Debezium message schema for Customers table
debezium_customer_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("CustomerID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("PhoneNumber", StringType(), True),
            StructField("CreatedAt", StringType(), True),
            StructField("UpdatedAt", StringType(), True)
        ]), True),
        StructField("after", StructType([
            StructField("CustomerID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("PhoneNumber", StringType(), True),
            StructField("CreatedAt", StringType(), True),
            StructField("UpdatedAt", StringType(), True)
        ]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True)
    ]), True)
])

def read_from_kafka(spark, kafka_topic):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9093")  # Use external port for standalone
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")  # Don't fail if some data is lost
            .load())

def process_stream(df, bronze_path, schema=None):
    # Select the value column from Kafka and cast it as string
    df_processed = (df.selectExpr("CAST(value AS STRING) as json_data", 
                                  "timestamp as kafka_timestamp")
                    .withColumn("ingestion_timestamp", current_timestamp()))
    
    # Parse JSON if schema is provided (for Debezium CDC format)
    if schema:
        df_parsed = df_processed.withColumn("parsed", from_json(col("json_data"), schema))
        
        # Extract data from Debezium format - use 'after' for inserts/updates, 'before' for deletes
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
    
    # Write the stream to the bronze layer in Parquet format
    query = (df_processed.writeStream
             .format("parquet")
             .option("path", bronze_path)
             .partitionBy("ingestion_timestamp")
             .trigger(processingTime="10 seconds")
             .option("checkpointLocation", f"/tmp/checkpoints/{bronze_path.split('/')[-1]}")
             .start())
    
    return query

def main():
    # Create Spark session
    spark = create_spark_session()
    
    # Configure Azure storage
    storage_account_bronze = "mybronze"
    bronze_container = "bronze-stream"
    
    # Set Azure credentials
    spark.conf.set(
        "fs.azure.account.key.mybronze.dfs.core.windows.net",
        "c5etqTidViezB/4ukOAALy23HeMBsJJ8g+2nFaIdbC7E9PhLw0y2YIA1ItjutpqS1/8Ga8fw40mR+ASt2T+/sw=="
    )
    
    # List of topics to consume (based on your MySQL tables)
    topics = [
        "online_store.online_store.Customers"
        # Add more topics as needed
    ]
    
    # Start streaming for each topic
    queries = []
    for topic in topics:
        # Extract table name from topic
        table_name = topic.split(".")[-1]
        bronze_path = f"abfss://{bronze_container}@{storage_account_bronze}.dfs.core.windows.net/{table_name}"
        
        # Read from Kafka
        kafka_df = read_from_kafka(spark, topic)
        
        # Use schema for Customers, otherwise None
        if table_name == "Customers":
            query = process_stream(kafka_df, bronze_path, debezium_customer_schema)
        else:
            query = process_stream(kafka_df, bronze_path)
        queries.append(query)
    
    # Wait for all queries to terminate
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()
