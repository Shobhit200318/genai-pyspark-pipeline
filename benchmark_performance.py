import os
import time
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.config import RAW_DATA_DIR

# =========================================================
# WINDOWS + PYSPARK CONFIG
# =========================================================

# Ensure PySpark uses the correct Python executable
os.environ["PYSPARK_PYTHON"] = "python"

# Optional Hadoop setup (only if installed)
# os.environ["HADOOP_HOME"] = r"C:\hadoop"

# JVM compatibility options for Java 11/17+
JAVA_OPENS = (
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


# =========================================================
# PANDAS BENCHMARK
# =========================================================

def run_pandas_benchmark(orders_path: str, products_path: str) -> float:
    """
    Benchmarks Pandas performance for:
    - Reading parquet
    - Joining
    - Aggregation
    """

    print("\n[*] Starting Pandas benchmark...")

    start = time.perf_counter()

    # 1. Load parquet files
    df_orders = pd.read_parquet(orders_path)
    df_products = pd.read_parquet(products_path)

    # 2. Join datasets
    merged = df_orders.merge(df_products, on="product_id")

    # 3. Revenue calculation
    merged = merged.assign(
        revenue=merged["quantity"] * merged["price"]
    )

    # 4. Aggregate top customers
    top_10 = (
        merged.groupby("customer_id")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    # Force execution
    _ = top_10.values

    duration = time.perf_counter() - start

    return duration


# =========================================================
# PYSPARK BENCHMARK
# =========================================================

def create_spark_session():
    """
    Creates optimized Spark session for local benchmarking.
    """

    spark = (
        SparkSession.builder
        .appName("FrameworkBenchmark")
        .master("local[*]")

        # JVM Options
        .config("spark.driver.extraJavaOptions", JAVA_OPENS)
        .config("spark.executor.extraJavaOptions", JAVA_OPENS)

        # Memory
        .config("spark.driver.memory", "4g")

        # Reduce shuffle partitions for local machine
        .config("spark.sql.shuffle.partitions", "4")

        # Adaptive Query Execution (IMPORTANT)
        .config("spark.sql.adaptive.enabled", "true")

        # Adaptive shuffle optimization
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Broadcast joins optimization
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")

        # Use Arrow optimization where possible
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


def run_spark_benchmark(orders_path: str, products_path: str) -> float:
    """
    Benchmarks PySpark performance.
    """

    print("\n[*] Initializing Spark Session...")

    spark = create_spark_session()

    try:
        print("[*] Starting PySpark benchmark...")

        start = time.perf_counter()

        # =================================================
        # LOAD DATA
        # =================================================

        sdf_orders = spark.read.parquet(orders_path)
        sdf_products = spark.read.parquet(products_path)

        # =================================================
        # TRANSFORMATIONS
        # =================================================

        top_10 = (
            sdf_orders
            .join(
                F.broadcast(sdf_products),
                "product_id"
            )
            .withColumn(
                "revenue",
                F.col("quantity") * F.col("price")
            )
            .groupBy("customer_id")
            .agg(
                F.sum("revenue").alias("total_revenue")
            )
            .orderBy(F.desc("total_revenue"))
            .limit(10)
        )

        # =================================================
        # ACTION (forces execution)
        # =================================================

        result = top_10.collect()

        duration = time.perf_counter() - start

        print("\n[*] Top Customers:")
        for row in result[:5]:
            print(row)

        return duration

    finally:
        # Clean shutdown
        spark.stop()


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    orders_file = str(RAW_DATA_DIR / "orders.parquet")
    products_file = str(RAW_DATA_DIR / "products.parquet")

    # =====================================================
    # CHECK FILES
    # =====================================================

    if not os.path.exists(orders_file):

        print(
            f"\n[ERROR] Data not found at:\n{orders_file}"
        )

        print(
            "\nPlease run main.py first to generate parquet data."
        )

    else:

        print("\n" + "=" * 60)
        print("        PANDAS vs PYSPARK BENCHMARK")
        print("=" * 60)

        # =================================================
        # RUN BENCHMARKS
        # =================================================

        pandas_time = run_pandas_benchmark(
            orders_file,
            products_file
        )

        spark_time = run_spark_benchmark(
            orders_file,
            products_file
        )

        # =================================================
        # RESULTS
        # =================================================

        print("\n" + "=" * 60)

        print(
            f"{'Framework':<15} | {'Execution Time (seconds)':>25}"
        )

        print("-" * 60)

        print(
            f"{'Pandas':<15} | {pandas_time:>25.4f}"
        )

        print(
            f"{'PySpark':<15} | {spark_time:>25.4f}"
        )

        print("=" * 60)

        # =================================================
        # WINNER
        # =================================================

        speed_diff = abs(pandas_time - spark_time)

        winner = (
            "Pandas"
            if pandas_time < spark_time
            else "PySpark"
        )

        print(
            f"\n[*] Result: {winner} was faster by "
            f"{speed_diff:.4f} seconds."
        )

        # =================================================
        # PERFORMANCE RATIO
        # =================================================

        ratio = spark_time / pandas_time

        print(
            f"[*] PySpark/Pandas Time Ratio: {ratio:.2f}x"
        )

        print("\n[✓] Benchmark completed successfully.")