import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Reduce Windows/PySpark issues
os.environ["PYSPARK_PYTHON"] = "python"

spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# Reduce Spark logs
spark.sparkContext.setLogLevel("ERROR")

orders = spark.read.parquet(
    "data/raw/orders.parquet"
)

products = spark.read.parquet(
    "data/raw/products.parquet"
)

top = orders.join(
    products,
    "product_id"
).withColumn(
    "revenue",
    F.col("quantity") * F.col("price")
).groupBy(
    "customer_id"
).agg(
    F.sum("revenue").alias("total_revenue")
).orderBy(
    F.desc("total_revenue")
).limit(10)

# Format only for display
top = top.withColumn(
    "total_revenue",
    F.format_number("total_revenue", 2)
)

top.show(truncate=False)

spark.stop()