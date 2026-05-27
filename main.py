import time
import os
from src.data_generator import SyntheticDataGenerator
from src.config import RAW_DATA_DIR, get_logger

logger = get_logger(__name__)

def main():
    """
    Main function to generate synthetic e-commerce data, save it as Parquet files,
    and report generation time and file sizes.
    """
    try:
        logger.info("Starting synthetic data generation process...")
        total_start_time = time.time()
        
        generator = SyntheticDataGenerator()
        stats = {}

        # Generate and save Customers
        logger.info("Generating customer data...")
        customers_df = generator.generate_customers()
        customer_file_path = RAW_DATA_DIR / "customers.parquet"
        customers_df.to_parquet(customer_file_path, index=False)
        stats['customers.parquet'] = os.path.getsize(customer_file_path) / (1024 * 1024)

        # Generate and save Products
        logger.info("Generating product data...")
        products_df = generator.generate_products()
        product_file_path = RAW_DATA_DIR / "products.parquet"
        products_df.to_parquet(product_file_path, index=False)
        stats['products.parquet'] = os.path.getsize(product_file_path) / (1024 * 1024)

        # Generate and save Orders
        logger.info("Generating order data...")
        orders_df = generator.generate_orders(customers_df, products_df)
        order_file_path = RAW_DATA_DIR / "orders.parquet"
        orders_df.to_parquet(order_file_path, index=False)
        stats['orders.parquet'] = os.path.getsize(order_file_path) / (1024 * 1024)

        # Final Output Summary
        total_duration = time.time() - total_start_time
        
        print(f"\n{'='*40}")
        print(f"PIPELINE SUMMARY")
        print(f"{'='*40}")
        print(f"Total Execution Time: {total_duration:.2f} seconds")
        print(f"\nFiles saved to {RAW_DATA_DIR}:")
        for filename, size in stats.items():
            if size < 1:
                print(f"- {filename:<20} | {size * 1024:>8.2f} KB")
            else:
                print(f"- {filename:<20} | {size:>8.2f} MB")
        print(f"{'='*40}")

        logger.info("Pipeline execution finished successfully.")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        print(f"\n[ERROR] Data generation failed: {e}")

if __name__ == "__main__":
    main()