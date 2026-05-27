import os
from pathlib import Path
import logging

# Project Root
ROOT_DIR = Path(__file__).parent.parent

# Data Directories
RAW_DATA_DIR = ROOT_DIR / "data" / "raw"
PROCESSED_DATA_DIR = ROOT_DIR / "data" / "processed"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(ROOT_DIR / "pipeline.log")
    ]
)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)