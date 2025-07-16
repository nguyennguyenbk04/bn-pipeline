from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType

def create_spark_session():
    return (SparkSession.builder
            .appName("CDC to Bronze Layer")
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," +
                    "org.apache.hadoop:hadoop-azure:3.3.4," +
                    "org.apache.hadoop:hadoop-common:3.3.4," +
                    "com.microsoft.azure:azure-storage:8.6.6")
            .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
            .config("spark.hadoop.fs.azure.account.key.mybronze.dfs.core.windows.net",
                   "c5etqTidViezB/4ukOAALy23HeMBsJJ8g+2nFaIdbC7E9PhLw0y2YIA1ItjutpqS1/8Ga8fw40mR+ASt2T+/sw==")
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
            .config("spark.sql.streaming.minBatchesToRetain", "10")
            .config("spark.sql.streaming.pollingDelay", "1s")
            .getOrCreate())

# Define schema for Customers table
customer_schema = StructType([
    StructField("CustomerID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("PhoneNumber", StringType(), True),
    StructField("CreatedAt", TimestampType(), True),
    StructField("UpdatedAt", TimestampType(), True)
])

def read_from_kafka(spark, kafka_topic):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")  # Docker service name
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")  # Don't fail if some data is lost
            .load())

def process_stream(df, bronze_path, schema=None):
    # Select the value column from Kafka and cast it as string
    df_processed = (df.selectExpr("CAST(value AS STRING) as json_data", 
                                  "timestamp as kafka_timestamp")
                    .withColumn("ingestion_timestamp", current_timestamp()))
    
    # Parse JSON if schema is provided
    if schema:
        df_processed = df_processed.withColumn("data", from_json(col("json_data"), schema)) \
                                   .select("data.*", "kafka_timestamp", "ingestion_timestamp")
    
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
    bronze_container = "bronze"
    
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
            query = process_stream(kafka_df, bronze_path, customer_schema)
        else:
            query = process_stream(kafka_df, bronze_path)
        queries.append(query)
    
    # Wait for all queries to terminate
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()