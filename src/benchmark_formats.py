import os
import time
import tracemalloc
from pathlib import Path
import pandas as pd
import numpy as np
from faker import Faker
from typing import Dict, List, Any, Callable, Optional
from src.config import get_logger

logger = get_logger(__name__)

try:
    from tabulate import tabulate
except ImportError:
    logger.warning("The 'tabulate' package is not installed. Table output will be limited.")
    tabulate = None

# Hardware Constants
TDP_WATT = 65  # Estimated CPU Power in Watts
NUM_ROWS = 500_000

def generate_benchmark_data(n: int) -> pd.DataFrame:
    """
    Generates a synthetic dataset for benchmarking.
    """
    fake = Faker()
    logger.info(f"Generating {n:,} rows of synthetic data...")
    
    # Optimization: Generating 500k unique fake names is slow. 
    # We generate a pool and sample to speed up data creation.
    name_pool = [fake.name() for _ in range(1000)]
    email_pool = [fake.email() for _ in range(1000)]

    data = {
        "id": np.arange(n),
        "name": [np.random.choice(name_pool) for _ in range(n)],
        "email": [np.random.choice(email_pool) for _ in range(n)],
        "amount": np.random.uniform(10, 1000, size=n).round(2),
        "date": pd.date_range(start="2024-01-01", periods=n, freq="s"),
        "category": np.random.choice(["Electronics", "Clothing", "Home", "Sports", "Books"], size=n)
    }
    return pd.DataFrame(data)

def benchmark_format(
    name: str, 
    write_fn: Callable[[pd.DataFrame, str], None], 
    read_fn: Callable[[str], pd.DataFrame], 
    df: pd.DataFrame, 
    ext: str
) -> Dict[str, Any]:
    """
    Benchmarks write/read performance and hardware metrics for a specific format.
    """
    filename = Path(f"benchmark_temp.{ext}")
    results = {"Format": name}
    
    # --- Write Benchmark ---
    tracemalloc.start()
    cpu_start = time.process_time()
    wall_start = time.perf_counter()
    
    write_fn(df, str(filename))
    
    wall_end = time.perf_counter()
    cpu_end = time.process_time()
    _, peak_mem_write = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    results["Size_MB"] = round(filename.stat().st_size / (1024 * 1024), 2)
    results["Write_s"] = round(wall_end - wall_start, 2)
    write_cpu = cpu_end - cpu_start
    
    # --- Read Benchmark ---
    tracemalloc.start()
    cpu_start = time.process_time()
    wall_start = time.perf_counter()
    
    _ = read_fn(str(filename))
    
    wall_end = time.perf_counter()
    cpu_end = time.process_time()
    _, peak_mem_read = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    results["Read_s"] = round(wall_end - wall_start, 2)
    read_cpu = cpu_end - cpu_start
    
    # --- Hardware & Energy Metrics ---
    results["Memory_MB"] = round(max(peak_mem_write, peak_mem_read) / (1024 * 1024), 2)
    results["CPU_s"] = round(write_cpu + read_cpu, 2)
    # Energy (Wh) = (Power in Watts * Time in Hours)
    results["Energy_Wh"] = round((results["CPU_s"] * TDP_WATT) / 3600, 4)
    
    # Cleanup
    filename.unlink(missing_ok=True)

    return results

def run_benchmarks():
    """
    Orchestrates the benchmarking process and prints a summary table.
    """
    df = generate_benchmark_data(NUM_ROWS)
    
    formats = [
        ("CSV", lambda d, f: d.to_csv(f, index=False), lambda f: pd.read_csv(f), "csv"),
        ("XLSX", lambda d, f: d.to_excel(f, index=False, engine='openpyxl'), 
         lambda f: pd.read_excel(f, engine='openpyxl'), "xlsx"),
        ("Parquet (pyarrow)", lambda d, f: d.to_parquet(f, index=False, engine='pyarrow', compression='snappy'), 
         lambda f: pd.read_parquet(f, engine='pyarrow'), "pyarrow.parquet"),
        ("Parquet (fastparquet)", lambda d, f: d.to_parquet(f, index=False, engine='fastparquet'), 
         lambda f: pd.read_parquet(f, engine='fastparquet'), "fastparquet.parquet"),
        ("ORC", lambda d, f: d.to_orc(f, index=False), lambda f: pd.read_orc(f), "orc"),
        ("Feather", lambda d, f: d.to_feather(f), lambda f: pd.read_feather(f), "feather"),
    ]
    
    benchmark_data = []
    for name, write, read, ext in formats:
        logger.info(f"Benchmarking format: {name}...")
        try:
            res = benchmark_format(name, write, read, df, ext)
            benchmark_data.append(res)
        except Exception as e:
            logger.error(f"Failed to benchmark {name}: {e}. Ensure required libraries are installed.")
            
    # Calculate Savings vs CSV Baseline
    baseline = next((res for res in benchmark_data if res["Format"] == "CSV"), None)
    
    if baseline:
        for res in benchmark_data:
            size_savings = 100 * (baseline["Size_MB"] - res["Size_MB"]) / baseline["Size_MB"]
            time_savings = 100 * (baseline["Read_s"] - res["Read_s"]) / baseline["Read_s"]
            res["Size_Saving_%"] = round(size_savings, 2)
            res["Read_Speedup_%"] = round(time_savings, 2) if baseline["Read_s"] > 0 else 0
    
    # Formatting the table for output
    if not benchmark_data:
        logger.error("No benchmark data collected.")
        return

    headers = list(benchmark_data[0].keys())
    table_rows = [list(res.values()) for res in benchmark_data]
    
    print("\n" + "="*80)
    print(f"=== File Format Benchmark Results: {NUM_ROWS:,} ROWS ===")
    print("="*80)
    if tabulate:
        print(tabulate(table_rows, headers=headers, tablefmt="fancy_grid", floatfmt=".4f"))
    else:
        print(pd.DataFrame(benchmark_data).to_string(index=False))
    
    if baseline:
        print("\n=== Summary Analysis (vs CSV Baseline) ===")
        for res in benchmark_data:
            if res["Format"] == "CSV":
                continue
            print(f"- {res['Format']:<25}: {res['Size_Saving_%']:>7.2f}% smaller, {res['Read_Speedup_%']:>7.2f}% faster read.")
    print("="*80)

if __name__ == "__main__":
    try:
        run_benchmarks()
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted by user.")
    except Exception as e:
        logger.error(f"Critical error: {e}")