FROM apache/spark:3.5.0

USER root

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip"

# Create directories and set permissions
RUN mkdir -p /app/jars /tmp/checkpoints /home/spark/.ivy2 && \
    chmod -R 777 /app /tmp/checkpoints /home/spark/.ivy2

# Install wget
RUN apt-get update && apt-get install -y wget

# Download Hadoop common JAR
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar -P /app/jars/

WORKDIR /app

# Copy the streaming script and required JARs
COPY bronze_stream.py .
COPY jars/hadoop-azure-3.3.6.jar /app/jars/
COPY jars/azure-storage-8.6.6.jar /app/jars/
COPY jars/jetty-client-9.4.43.v20210629.jar /app/jars/
COPY jars/jetty-http-9.4.43.v20210629.jar /app/jars/
COPY jars/jetty-io-9.4.43.v20210629.jar /app/jars/
COPY jars/jetty-util-9.4.43.v20210629.jar /app/jars/
COPY jars/jetty-util-ajax-9.4.43.v20210629.jar /app/jars/

# Set spark user
ENV SPARK_USER=spark
ENV SPARK_UID=185

USER $SPARK_USER

# Command to run the streaming job
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6,org.apache.hadoop:hadoop-common:3.3.4", \
     "--conf", "spark.hadoop.fs.azure.account.key.mybronze.dfs.core.windows.net=c5etqTidViezB/4ukOAALy23HeMBsJJ8g+2nFaIdbC7E9PhLw0y2YIA1ItjutpqS1/8Ga8fw40mR+ASt2T+/sw==", \
     "--conf", "spark.sql.streaming.checkpointLocation=/tmp/checkpoints", \
     "./bronze_stream.py"]
