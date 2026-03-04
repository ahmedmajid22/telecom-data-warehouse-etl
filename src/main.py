from src.warehouse import create_warehouse_schema
from src.db import get_engine
from src.pipeline import run_telecom_etl
import logging
import sys
import os

# 🚀 STEP 1: Fix Python Path
# This ensures that when you run 'python src/main.py',
# it can find 'src.db', 'src.extract', etc.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 🚀 STEP 2: Professional Imports
# We use absolute imports (src.module) which is the standard for Dockerized apps

# Configure logging for the console output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def main():
    """
    LOCAL TEST RUNNER
    Use this to verify your ETL logic before deploying to Airflow.
    """
    logger.info("--- Starting Local ETL Test ---")

    try:
        # 1. Check Database Connectivity
        logger.info("Checking database connection...")
        engine = get_engine()

        # 2. Ensure Schema Exists
        # Note: We pass the engine as per the new professional design
        logger.info("Ensuring warehouse schema is ready...")
        create_warehouse_schema(engine)

        # 3. Run the Full ETL Orchestration
        # This calls the same function the Airflow DAG will call
        logger.info("Triggering run_telecom_etl()...")
        run_telecom_etl()

        logger.info("--- ✅ Local ETL Test Completed Successfully ---")

    except Exception as e:
        logger.error(f"--- ❌ Local ETL Test Failed ---")
        logger.error(f"Error details: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
