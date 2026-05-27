import pandas as pd
import time

start = time.time()

orders = pd.read_parquet("data/raw/orders.parquet")
products = pd.read_parquet("data/raw/products.parquet")

merged = orders.merge(
    products,
    on="product_id"
).copy()

merged.loc[:, "revenue"] = (
    merged["quantity"] * merged["price"]
)

top = (
    merged.groupby("customer_id")["revenue"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)

print("\nTop 10 Customers By Revenue\n")

print(
    top.apply(lambda x: f"${x:,.2f}")
)

print(
    f"\nPandas execution time: "
    f"{time.time() - start:.2f}s"
)