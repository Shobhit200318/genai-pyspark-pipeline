from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, month, year, round as spark_round
from pyspark.sql.window import Window
import logging
import time
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SalesAnalytics:
    """PySpark analytics class for sales data."""

    def __init__(self):
        self.spark: Optional[SparkSession] = None

    def create_spark_session(self) -> SparkSession:
        """Create configured Spark session."""
        if self.spark is None:
            self.spark = (SparkSession.builder
                .appName("SalesAnalytics")
                .master("local[*]")
                .config("spark.driver.memory", "8g")
                .config("spark.sql.shuffle.partitions", "16")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryoserializer.buffer.max", "1024m")
                .getOrCreate())
            
            self.spark.sparkContext.setLogLevel("ERROR")
            logger.info("Spark session created successfully")
        return self.spark

    def load_parquet(self, path: str):
        """Load parquet file."""
        logger.info(f"Loading parquet from {path}...")
        start = time.time()
        df = self.spark.read.parquet(path)
        logger.info(f"Loaded {df.count():,} rows in {time.time() - start:.2f}s")
        return df

    def top_customers_by_revenue(self, orders_df, products_df, n: int = 10):
        """Top N customers by revenue."""
        logger.info(f"Calculating top {n} customers by revenue...")
        start = time.time()
        
        result = (orders_df.join(products_df, "product_id")
                 .withColumn("revenue", col("quantity") * col("price"))
                 .groupBy("customer_id")
                 .agg(spark_sum("revenue").alias("total_spend"))
                 .orderBy(col("total_spend").desc())
                 .limit(n))
        
        print("\n" + "="*40)
        print(f"REPORT: TOP {n} CUSTOMERS BY REVENUE")
        print("="*40)
        result.show()
        print(f"Execution time: {time.time() - start:.2f}s")
        return result

    def sales_by_category(self, orders_df, products_df):
        """Sales by product category."""
        logger.info("Calculating sales by category...")
        start = time.time()
        
        result = (orders_df.join(products_df, "product_id")
                 .withColumn("revenue", col("quantity") * col("price"))
                 .groupBy("category")
                 .agg(
                     spark_sum("revenue").alias("total_revenue"),
                     spark_sum("quantity").alias("units_sold")
                 )
                 .orderBy(col("total_revenue").desc()))
        
        print("\n" + "="*40)
        print("REPORT: SALES BY CATEGORY")
        print("="*40)
        result.show()
        print(f"Execution time: {time.time() - start:.2f}s")
        return result

    def monthly_trends(self, orders_df, products_df):
        """Month-over-month revenue growth."""
        logger.info("Calculating monthly revenue trends...")
        start = time.time()
        
        sales_df = (orders_df.join(products_df, "product_id")
                   .withColumn("revenue", col("quantity") * col("price"))
                    .withColumn("order_month", year(col("order_date"))*100 + month(col("order_date"))))
        
        monthly_df = (sales_df.groupBy("order_month")
                        .agg(spark_sum("revenue").alias("revenue"))
                        .orderBy("order_month"))
        
        window_spec = Window.orderBy("order_month")
        
        result = (monthly_df
                    .withColumn("prev_revenue", 
                            spark_sum("revenue").over(window_spec.rowsBetween(-1, -1)))
                    .withColumn("growth_percentage",
                            spark_round(
                                (col("revenue") - col("prev_revenue")) / col("prev_revenue") * 100, 2
                            ))
                    .fillna(0.0, subset=["growth_percentage"]))
        
        result = result.withColumn("month",
                                    col("order_month").cast("string").substr(1,4) + "-" + 
                                    col("order_month").cast("string").substr(5,2))
        
        print("\n" + "="*40)
        print("REPORT: MONTHLY REVENUE TRENDS")
        print("="*40)
        result.select("month", "revenue", "prev_revenue", "growth_percentage").show()
        print(f"Execution time: {time.time() - start:.2f}s")
        return result