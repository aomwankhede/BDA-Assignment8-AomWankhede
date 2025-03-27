# PySpark with Kafka Setup Guide

This guide provides step-by-step instructions to install and configure PySpark, Kafka, and Hadoop, ensuring seamless integration. Follow the steps below to set up the environment and run a PySpark-Kafka application.

## Prerequisites
- **Hadoop 3.x** installed and configured
- **Open JDK 11** installed and configured
- **Python 3.7+** installed
- **Spark 3.5.5** installed
- **Kafka 3.5.2** installed

---

## 1. Install and Set Up PySpark

```sh
pip install pyspark
```

Add PySpark to your environment:
```sh
echo 'export SPARK_HOME=/path/to/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
source ~/.bashrc
```

---

## 2. Start Hadoop Services

Ensure Hadoop is running:
```sh
start-all.sh
```

Verify Hadoop services:
```sh
jps  # Should list Namenode, Datanode, ResourceManager, etc.
```

---

## 3. Start Kafka Services

Start Zookeeper:
```sh
zookeeper-server-start.sh /path/to/kafka/config/zookeeper.properties &
```


---

## 4. Run PySpark with Kafka Integration

Launch PySpark shell:
```sh
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5
```


---

## 5. Sample PySpark Kafka Consumer Code

Save the following Python script as `kafka_spark.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, col , current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StructuredStreamingWordCount") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Define the schema for the incoming data
schema = StructType([
    StructField("value", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read streaming data from the socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()


# Assign a timestamp column to the incoming data
lines_with_timestamp = lines.withColumn("timestamp", current_timestamp())

# Split the lines into words
words = lines_with_timestamp.select(
    explode(split(col("value"), " ")).alias("word"),
    col("timestamp")
)

# Define sliding window parameters
window_duration = "10 seconds"
slide_duration = "10 seconds"

# Perform windowed word count
windowed_word_counts = words.withWatermark("timestamp","1 minutes").groupBy(
    window(col("timestamp"), window_duration, slide_duration),
    col("word")
).count()

# Write the results to the console
console_query = windowed_word_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Write the results to HDFS in Parquet format, partitioned by window start time
hdfs_query = windowed_word_counts.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/home/hdoop/streaming_wordcount_output") \
    .option("checkpointLocation", "/home/hdoop/streaming_wordcount_checkpoint") \
    .partitionBy("window") \
    .start()


# Write the results to a database
def write_to_db(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://your_db_host:your_db_port/your_db_name") \
        .option("dbtable", "your_table_name") \
        .option("user", "your_db_user") \
        .option("password", "your_db_password") \
        .mode("append") \
        .save()


# Await termination of all queries
console_query.awaitTermination()
hdfs_query.awaitTermination()

```

Run the script:
```sh
python3 py_script.py
```

---

## 6. Produce Messages to Kafka
```sh
nc -lk 9999
```

Try writing something on the terminal worcount for the batch will be seen in the other terminal
---

## 7. Stop Services

To stop the running services, use:
```sh
stop-all.sh
stop-yarn.sh
```

---

## 8. Troubleshooting

- Make sure to use compatible kafka version with openjdk

---

We have just created realtime wordcounter using pyspark and kafka.
