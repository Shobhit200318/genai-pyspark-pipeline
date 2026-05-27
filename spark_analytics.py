from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR, get_logger

logger = get_logger(__name__)

def create_spark_session(app_name: str = "EcommerceAnalytics") -> SparkSession:
    """
    Initializes a Spark Session with configurations for Windows and Java 17+ compatibility.
    """
    # These options are required for Spark to work with Java 17+ on Windows
    # They grant access to internal Java modules that Hadoop requires.
    java_options = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.security.sasl/com.sun.security.sasl=ALL-UNNAMED"
    )

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.extraJavaOptions", java_options) \
        .config("spark.executor.extraJavaOptions", java_options) \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .master("local[*]") \
        .getOrCreate()

def load_data(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Loads raw Parquet data into Spark DataFrames.
    """
    logger.info("Loading raw data from Parquet...")
    customers = spark.read.parquet(str(RAW_DATA_DIR / "customers.parquet"))
    products = spark.read.parquet(str(RAW_DATA_DIR / "products.parquet"))
    orders = spark.read.parquet(str(RAW_DATA_DIR / "orders.parquet"))
    return customers, products, orders

def run_analytics(customers: DataFrame, products: DataFrame, orders: DataFrame) -> DataFrame:
    """
    Performs joins and aggregations to find revenue by category.
    """
    logger.info("Running business analytics...")
    
    # Join orders with products to get prices
    order_details = orders.join(products, "product_id", "inner")
    
    # Calculate total revenue per order item
    order_details = order_details.withColumn("revenue", F.col("quantity") * F.col("price"))
    
    # Aggregate revenue by category
    category_analysis = order_details.groupBy("category") \
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("order_id").alias("order_count")
        ) \
        .orderBy(F.desc("total_revenue"))
    
    return category_analysis

if __name__ == "__main__":
    spark = create_spark_session()
    try:
        customers, products, orders = load_data(spark)
        
        results = run_analytics(customers, products, orders)
        results.show()
        
        # Save results
        output_path = PROCESSED_DATA_DIR / "revenue_by_category"
        results.write.mode("overwrite").parquet(str(output_path))
        logger.info(f"Analysis results saved to {output_path}")
        
    except Exception as e:
        logger.error(f"Error in Spark Pipeline: {e}")
    finally:
        spark.stop()