from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.mllib.stat import Statistics


### Data Loading

# Create SparkSession
spark = SparkSession.builder.appName("Network Traffic Anomaly Detection").getOrCreate()

# Define schema
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("source_ip", StringType(), True),
    StructField("destination_ip", StringType(), True),
    StructField("port", IntegerType(), True),
    StructField("protocol", StringType(), True),
    StructField("bytes_transferred", LongType(), True)
])

# Load data from CSV
df = spark.read.csv("network_traffic.csv", header=True, schema=schema)


### Basic Preprocessing

# Handle nulls (replace with 0)
df = df.na.fill(0)

# Remove duplicates
df = df.dropDuplicates()

# Filter out ICMP traffic
df = df.filter(df["protocol"] != "ICMP")


### Feature Engineering

# Calculate traffic volume per source IP
traffic_volume_per_ip = df.groupBy("source_ip").count().withColumnRenamed("count", "traffic_volume")

# Calculate total bytes transferred per protocol
bytes_per_protocol = df.groupBy("protocol").sum("bytes_transferred").withColumnRenamed("sum(bytes_transferred)", "total_bytes")

# Calculate average packet size
avg_packet_size = df.agg(avg("bytes_transferred").alias("avg_bytes_transferred"))

# Calculate traffic patterns over time (1-hour windows)
traffic_over_time = df.groupBy(window("timestamp", "1 hour")).count().withColumnRenamed("count", "traffic_count")


### Detecting DDoS Attacks

# High traffic volume from a single source
threshold = 1000  # Adjust as needed
suspicious_ips = traffic_volume_per_ip.filter(col("traffic_volume") > threshold)

# Show suspicious IPs
suspicious_ips.show()

### Intrusion Detection
# Convert to RDD for custom outlier detection (if needed)
traffic_rdd = df.rdd

# Detect outliers in bytes_transferred using 3 standard deviations from the mean
bytes_transferred_rdd = traffic_rdd.map(lambda row: row["bytes_transferred"])
summary_stats = Statistics.colStats(bytes_transferred_rdd)
mean_bytes = summary_stats.mean()
stddev_bytes = summary_stats.stdev()
threshold = 3  # Number of standard deviations

def is_outlier(bytes_transferred):
    return abs(bytes_transferred - mean_bytes) > threshold * stddev_bytes

outliers_rdd = traffic_rdd.filter(lambda row: is_outlier(row["bytes_transferred"]))

# Convert back to DataFrame for further analysis (if needed)
outliers_df = outliers_rdd.toDF(schema)
