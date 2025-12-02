from pathlib import Path

# Base project directory (one level above src/)
BASE_DIR = Path(__file__).resolve().parents[1]

# Raw data paths
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
CUSTOMERS_CSV_PATH = RAW_DATA_DIR / "customers.csv"
ORDERS_CSV_PATH = RAW_DATA_DIR / "orders.csv"

# Lakehouse (Delta) directories
LAKEHOUSE_BASE = BASE_DIR / "data" / "lakehouse"
BRONZE_PATH = LAKEHOUSE_BASE / "bronze"
SILVER_PATH = LAKEHOUSE_BASE / "silver"
GOLD_PATH = LAKEHOUSE_BASE / "gold"

# Specific table locations
BRONZE_CUSTOMERS_PATH = BRONZE_PATH / "customers"
BRONZE_ORDERS_PATH = BRONZE_PATH / "orders"

SILVER_CUSTOMERS_PATH = SILVER_PATH / "dim_customers"
SILVER_ORDERS_PATH = SILVER_PATH / "fact_orders"

GOLD_ORDERS_METRICS_PATH = GOLD_PATH / "agg_orders_daily"
