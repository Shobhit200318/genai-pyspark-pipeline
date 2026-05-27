# genai-pyspark-pipeline
Synthetic data generation and PySpark analytics with Vibe Coding

## Project Overview
This project generates synthetic e-commerce data (customers, products, and orders) and uses PySpark to analyze business insights such as revenue by category and top-selling products.

## Features
- **Data Generation**: Uses Faker and NumPy (Zipf/Pareto distribution) for realistic data.
- **Analytics**: PySpark logic for total sales, top products, customer trends, and monthly analysis.
- **Format**: Saves data in Parquet for optimized Spark performance.

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Generate data:
   ```bash
   python src/data_generator.py
   ```
3. Run analytics:
   ```bash
   python src/spark_analytics.py
   ```

## Folder Structure
* `src/`: Core Python logic.
* `data/raw/`: Generated Parquet files.
* `data/processed/`: PySpark output results.
* `tests/`: Scripts for testing logic.
* `notebooks/`: Jupyter Notebooks for prototyping.
