import pandas as pd
import random
import numpy as np
from faker import Faker
from tqdm import tqdm
from src.config import RAW_DATA_DIR, get_logger
from typing import List, Dict, Any

logger = get_logger(__name__)

class SyntheticDataGenerator:
    """
    A class to generate synthetic e-commerce customer, product, and order data.
    """

    def __init__(self,
                num_customers: int = 100_000,
                num_products: int = 10_000,
                num_orders: int = 1_000_000):
        """
        Initializes the SyntheticDataGenerator with specified data volumes.

            num_customers: The total number of customers to generate.
            num_products: The total number of products to generate.
            num_orders: The total number of orders to generate.
        """
        self.fake = Faker()
        self.num_customers = num_customers
        self.num_products = num_products
        self.num_orders = num_orders
        logger.info(f"Initialized SyntheticDataGenerator with "
                    f"{num_customers} customers, {num_products} products, "
                    f"and {num_orders} orders.")

    def generate_customers(self) -> pd.DataFrame:
        """
        Generates fake customer data.

        Returns:
            A Pandas DataFrame containing customer details.
        """
        logger.info(f"Generating {self.num_customers} customers...")
        
        # Vectorize non-faker fields and pool dates for performance
        ages = np.clip(np.random.normal(loc=35, scale=10, size=self.num_customers).astype(int), 18, 80)
        date_pool = [self.fake.date_between(start_date='-5y', end_date='today').isoformat() for _ in range(1000)]
        
        customers_data = []
        for i in tqdm(range(1, self.num_customers + 1), desc="Generating Customers"):
            customers_data.append({
                "customer_id": i,
                "name": self.fake.name(),
                "email": self.fake.email(),
                "age": int(ages[i-1]),
                "city": self.fake.city(),
                "country": self.fake.country(),
                "registration_date": random.choice(date_pool)
            })
        return pd.DataFrame(customers_data)

    def generate_products(self) -> pd.DataFrame:
        """
        Generates fake product data.

        Returns:
            A Pandas DataFrame containing product details.
        """
        logger.info(f"Generating {self.num_products} products...")
        categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
        
        # Vectorized product generation
        df = pd.DataFrame({
            "product_id": np.arange(1, self.num_products + 1),
            "name": [f"Product_{i}" for i in range(1, self.num_products + 1)],
            "category": np.random.choice(categories, size=self.num_products),
            "price": np.round(np.random.uniform(10.0, 500.0, size=self.num_products), 2),
            "stock": np.random.randint(0, 1001, size=self.num_products),
            "rating": np.round(np.random.uniform(1.0, 5.0, size=self.num_products), 1)
        })
        
        logger.info(f"Generated {len(df)} products successfully.")
        return df

    def generate_orders(self, customers_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generates fake order data linking customers and products.

        Args:
            customers_df: DataFrame of existing customers.
            products_df: DataFrame of existing products.

        Returns:
            A Pandas DataFrame containing order details.
        """
        logger.info(f"Generating {self.num_orders} orders with Pareto distribution for customers...")
        orders_data: List[Dict[str, Any]] = []
        
        customer_ids = customers_df["customer_id"].tolist()
        product_ids = products_df["product_id"].tolist()

        # Implement 80/20 rule for customer orders using Zipf distribution (approximates Pareto for discrete ranks)
        # A common 'a' parameter for Zipf distribution to achieve 80/20 rule is around 1.16.
        zipf_alpha = 1.16
        
        # Generate Zipf distributed ranks. These will be mostly small integers (1, 2, 3, ...)
        # representing "more popular" customers.
        zipf_samples = np.random.zipf(a=zipf_alpha, size=self.num_orders)
        
        # Map these ranks to actual customer_ids, ensuring they are within the valid range [1, self.num_customers]
        # The modulo operation ensures the IDs wrap around if the Zipf sample is larger than num_customers,
        # maintaining the skewed distribution.
        customer_ids_for_orders = ((zipf_samples - 1) % self.num_customers) + 1
        
        # Randomly choose product IDs for each order
        product_ids_for_orders = np.random.choice(product_ids, size=self.num_orders)

        for i in tqdm(range(self.num_orders), desc="Generating Orders"):
            orders_data.append({
                "order_id": i + 1,
                "customer_id": int(customer_ids_for_orders[i]),
                "product_id": int(product_ids_for_orders[i]),
                "quantity": random.randint(1, 10),
                "order_date": self.fake.date_between(start_date='-1y', end_date='today').isoformat()
            })
        return pd.DataFrame(orders_data)

if __name__ == "__main__":
    generator = SyntheticDataGenerator()

    # Generate data
    customers_df = generator.generate_customers()
    products_df = generator.generate_products()
    orders_df = generator.generate_orders(customers_df, products_df)

    # Save data
    logger.info("Saving generated data to Parquet files...")
    customers_df.to_parquet(RAW_DATA_DIR / "customers.parquet", index=False)
    products_df.to_parquet(RAW_DATA_DIR / "products.parquet", index=False)
    orders_df.to_parquet(RAW_DATA_DIR / "orders.parquet", index=False)
    logger.info(f"Data generation complete. Files saved to {RAW_DATA_DIR}/") 