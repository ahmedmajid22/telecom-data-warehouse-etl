# src/pipeline.py

import logging
import os
import pandas as pd
import sys

# Ensure the module can find sibling files when run in different environments
from src.extract import read_csv_file
from src.transform import clean_customers, clean_sim_cards, clean_transactions
from src.warehouse import build_dim_date, build_fact_table, create_warehouse_schema
from src.db import get_engine, load_data_to_db, run_query, get_max_transaction_date

# ✅ LOGGING CONFIGURATION
# We use a StreamHandler so logs appear in Airflow UI task logs AND the file.
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler('/opt/airflow/logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Get the absolute path to project root (/opt/airflow)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_telecom_etl():
    """
    Main ETL orchestration function.
    Orchestrates Extract, Transform, Load, and Analytics.
    """

    logger.info("🚀 Starting Telecom ETL Pipeline")

    # 1️⃣ Setup Paths & Column Definitions
    customers_columns = ["customer_id", "full_name",
                         "phone_number", "city", "registration_date"]
    sim_cards_columns = ["sim_id", "customer_id",
                         "sim_number", "activation_date", "status"]
    transactions_columns = ["transaction_id", "sim_id",
                            "transaction_date", "amount", "store_location"]

    customers_path = os.path.join(PROJECT_ROOT, "data", "customers.csv")
    sim_cards_path = os.path.join(PROJECT_ROOT, "data", "sim_cards.csv")
    transactions_path = os.path.join(PROJECT_ROOT, "data", "transactions.csv")

    # 2️⃣ EXTRACT
    logger.info("--- Stage 1: Extraction ---")
    customers_raw = read_csv_file(customers_path, customers_columns)
    sim_cards_raw = read_csv_file(sim_cards_path, sim_cards_columns)
    transactions_raw = read_csv_file(transactions_path, transactions_columns)

    # 3️⃣ TRANSFORM
    logger.info("--- Stage 2: Transformation ---")
    customers = clean_customers(customers_raw)
    sim_cards = clean_sim_cards(sim_cards_raw)
    transactions = clean_transactions(transactions_raw)

    # 4️⃣ LOAD (Warehouse Setup & Dimension Loading)
    logger.info("--- Stage 3: Loading Dimensions ---")
    engine = get_engine()

    # Pass engine to schema creator (Professional Design)
    create_warehouse_schema(engine)

    # Load Dimensions (SCD Type 0/1 logic via UPSERT)
    dim_date = build_dim_date()
    load_data_to_db(dim_date, "dim_date", engine)
    load_data_to_db(customers, "dim_customers", engine)
    load_data_to_db(sim_cards, "dim_sim_cards", engine)

    # 5️⃣ FACT TABLE & INCREMENTAL LOGIC
    logger.info("--- Stage 4: Processing Fact Table ---")
    fact = build_fact_table(transactions, sim_cards)

    # Simple Data Quality Check
    if fact["amount"].sum() <= 0:
        raise ValueError(
            "❌ Total revenue is zero or negative — data quality failed!")

    # Incremental filtering
    max_date, last_id = get_max_transaction_date(engine)

    if max_date:
        logger.info(f"Checking for new data since {max_date}...")
        fact["date_id"] = pd.to_datetime(fact["date_id"])

        # Filter: Only keep rows strictly newer than what we have
        fact = fact[
            (fact["date_id"] > pd.to_datetime(max_date)) |
            ((fact["date_id"] == pd.to_datetime(max_date))
             & (fact["transaction_id"] > last_id))
        ]

    if fact.empty:
        logger.info("No new transaction records found. Skipping fact load.")
    else:
        logger.info(f"Loading {len(fact)} new rows to fact_transactions...")
        load_data_to_db(fact, "fact_transactions", engine)

    # 6️⃣ ANALYTICS
    logger.info("--- Stage 5: Running Analytics ---")

    revenue_per_day_query = """
        SELECT d.date_id AS day, SUM(f.amount) AS total_revenue
        FROM fact_transactions f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.date_id ORDER BY d.date_id LIMIT 5;
    """

    revenue_per_city_query = """
        SELECT c.city, SUM(f.amount) AS total_revenue
        FROM fact_transactions f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        GROUP BY c.city ORDER BY total_revenue DESC;
    """

    rev_day = run_query(revenue_per_day_query, engine)
    rev_city = run_query(revenue_per_city_query, engine)

    # Print results to logs for visibility in Airflow
    logger.info(f"\nTop Cities by Revenue:\n{rev_city.to_string()}")

    logger.info("✅ ETL Pipeline completed successfully!")


if __name__ == "__main__":
    run_telecom_etl()
