from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OptimizedAnalytics") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Print all settings to verify
print("Spark Version:", spark.version)
print("Driver Memory:", spark.conf.get("spark.driver.memory"))
print("Shuffle Partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
print("Adaptive Enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
print("Serializer:", spark.conf.get("spark.serializer"))
print("Coalesce Partitions:", spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))