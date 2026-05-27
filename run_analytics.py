import time
from src.spark_analytics import SalesAnalytics
from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR

def main():
    """
    Main execution script for running Sales Analytics.
    """
    analytics = SalesAnalytics()  # No need to pass spark manually
    
    start_total = time.perf_counter()
    
    try:
        # 1. Create Spark session
        start_session = time.perf_counter()
        analytics.create_spark_session()
        print(f"[*] Spark Session initialized in: {time.perf_counter() - start_session:.2f}s")

        # 2. Load raw data
        start_load = time.perf_counter()
        print("[*] Loading raw data...")
        
        customers = analytics.load_parquet(str(RAW_DATA_DIR / "customers.parquet"))
        products = analytics.load_parquet(str(RAW_DATA_DIR / "products.parquet"))
        orders = analytics.load_parquet(str(RAW_DATA_DIR / "orders.parquet"))
        
        print(f"[*] Data loading took: {time.perf_counter() - start_load:.2f}s")

        # 3. Run Analytics
        analytics.top_customers_by_revenue(orders, products, n=10)
        analytics.sales_by_category(orders, products)
        analytics.monthly_trends(orders, products)

        # Optional: Save processed results
        print("\n[*] Saving processed results...")
        # (You can add .write.parquet() here if needed)

    except Exception as e:
        print(f"\n[!] Error during analytics execution: {e}")
    finally:
        print("\n[*] Stopping Spark session...")
        if hasattr(analytics, 'spark'):
            analytics.spark.stop()
        print(f"[*] Finished in {time.perf_counter() - start_total:.2f}s")


if __name__ == "__main__":
    main()