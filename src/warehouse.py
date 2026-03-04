# src/warehouse.py
import pandas as pd
import logging
from sqlalchemy import text  # Required to execute raw SQL

# ✅ Import removed from here because 'engine' is now passed as a parameter
# This makes the code more modular and easier to test.


def create_warehouse_schema(engine):
    """
    Creates the necessary tables and constraints in PostgreSQL.
    Accepts an engine object from the caller (Pipeline or Main).
    """
    logging.info("Creating warehouse schema...")

    # Defines the structure and relationships (PK/FK) for the warehouse
    # This represents a classic Star Schema architecture
    create_tables_sql = """
    -- Create Dimension Tables
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        full_name VARCHAR(100),
        phone_number VARCHAR(20),
        city VARCHAR(50),
        registration_date DATE
    );

    CREATE TABLE IF NOT EXISTS dim_sim_cards (
        sim_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50) REFERENCES dim_customers(customer_id),
        sim_number VARCHAR(50),       
        status VARCHAR(20),
        activation_date DATE
    );

    CREATE TABLE IF NOT EXISTS dim_date (
        date_id DATE PRIMARY KEY,
        day INT,
        month INT,
        year INT,
        quarter INT,
        week INT,
        day_name VARCHAR(10),
        month_name VARCHAR(10),
        is_weekend BOOLEAN
    );

    -- Create Fact Table (Central table for metrics)
    CREATE TABLE IF NOT EXISTS fact_transactions (
        transaction_id VARCHAR(50) PRIMARY KEY,
        sim_id VARCHAR(50) REFERENCES dim_sim_cards(sim_id),
        customer_id VARCHAR(50) REFERENCES dim_customers(customer_id),
        date_id DATE REFERENCES dim_date(date_id),
        amount NUMERIC(12,2),
        store_location VARCHAR(100)
    );
    """

    # Executes the SQL within a transaction to ensure integrity
    # If one table fails, none are created (Atomicity)
    try:
        with engine.begin() as conn:
            conn.execute(text(create_tables_sql))
        logging.info("✅ Warehouse schema created/verified successfully.")
    except Exception as e:
        logging.error(f"❌ Failed to create warehouse schema: {e}")
        raise


def build_dim_date(start="2024-01-01", end="2027-12-31") -> pd.DataFrame:
    """Generates a dimension table for dates to support time-based analytics."""
    logging.info(f"Building dim_date from {start} to {end}")
    date_range = pd.date_range(start=start, end=end)
    dim_date = pd.DataFrame(
        {
            "date_id": date_range,
            "day": date_range.day,
            "month": date_range.month,
            "year": date_range.year,
            "quarter": date_range.quarter,
            "week": date_range.isocalendar().week,
            "day_name": date_range.day_name(),
            "month_name": date_range.month_name(),
            "is_weekend": date_range.weekday >= 5,
        }
    )
    # Ensure date_id is explicitly a date object for proper SQL loading
    dim_date["date_id"] = dim_date["date_id"].dt.date
    return dim_date


def build_fact_table(
    transactions_df: pd.DataFrame, sim_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Joins transactions with dimensions to create the Fact table.
    This links transaction events to specific customers and SIM IDs.
    """
    logging.info("Building fact_transactions by joining SIM and Transaction data")

    # Ensure datetime format for merging and safe date handling
    transactions_df["transaction_date"] = pd.to_datetime(
        transactions_df["transaction_date"]
    )

    # Join transactions with sim_cards to get the customer_id associated with that SIM
    fact = transactions_df.merge(
        sim_df[["sim_id", "customer_id"]], on="sim_id", how="left"
    )

    # Create date_id as a date object to match the dim_date Primary Key
    fact["date_id"] = fact["transaction_date"].dt.date

    # Select and order columns to match the SQL schema exactly
    return fact[
        [
            "transaction_id",
            "sim_id",
            "customer_id",
            "date_id",
            "amount",
            "store_location",
        ]
    ]
