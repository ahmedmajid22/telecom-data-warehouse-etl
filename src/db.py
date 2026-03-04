import logging
import os
import time
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.exc import OperationalError
import pandas as pd
from sqlalchemy.dialects.postgresql import insert

# Configure logging
logging.basicConfig(level=logging.INFO)

# DOCKER UPGRADE: Dynamic Connection String
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "ahmed")
DB_PASS = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "telecom_db")

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def get_engine():
    """Creates and returns a database engine with retry logic."""
    for i in range(10):
        try:
            engine = create_engine(DB_URL)
            # Test connection - using 'with' properly closes it immediately
            with engine.connect():
                logging.info("✅ Database connection successful!")
            return engine
        except OperationalError:
            logging.warning(
                f"⏳ Database not ready, waiting 3 seconds... (Attempt {i+1}/10)"
            )
            time.sleep(3)

    raise Exception("❌ Could not connect to database after multiple attempts.")


def get_max_transaction_date(engine):
    """Fetches the latest transaction date and ID for foolproof incremental loading."""
    query = """
        SELECT date_id, transaction_id
        FROM fact_transactions
        ORDER BY date_id DESC, transaction_id DESC
        LIMIT 1;
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            if result:
                return result[0], result[1]
            return None, None
    except Exception:
        logging.warning(
            "Could not fetch max date/id (table likely empty). Proceeding with full load."
        )
        return None, None


def load_data_to_db(df: pd.DataFrame, table_name: str, engine):
    """Loads a pandas DataFrame into a SQL table using UPSERT logic."""
    if df.empty:
        logging.info(f"DataFrame for {table_name} is empty. Skipping load.")
        return

    try:
        logging.info(
            f"Loading data into table: {table_name} using upsert logic")
        data = df.to_dict(orient="records")
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)
        stmt = insert(table).values(data)

        if table_name == "fact_transactions":
            conflict_target = "transaction_id"
        elif table_name == "dim_customers":
            conflict_target = "customer_id"
        elif table_name == "dim_sim_cards":
            conflict_target = "sim_id"
        elif table_name == "dim_date":
            conflict_target = "date_id"
        else:
            conflict_target = "id"

        upsert_stmt = stmt.on_conflict_do_nothing(
            index_elements=[conflict_target])

        with engine.begin() as conn:
            conn.execute(upsert_stmt)

        logging.info(
            f"Successfully processed {len(df)} rows into {table_name}")
    except Exception as e:
        logging.error(f"Error loading data to {table_name}: {e}")
        raise


def run_query(query: str, engine):
    """Executes a SQL query and returns result as DataFrame."""
    try:
        logging.info("Running analytics query")
        return pd.read_sql(query, engine)
    except Exception as e:
        logging.error(f"Error running query: {e}")
        raise
