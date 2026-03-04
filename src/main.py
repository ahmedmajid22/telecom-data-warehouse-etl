from src.warehouse import create_warehouse_schema
from src.db import get_engine
from src.pipeline import run_telecom_etl
import logging
import sys
import os

# STEP 1: Fix Python Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def main():
    """LOCAL TEST RUNNER"""
    logger.info("--- Starting Local ETL Test ---")

    try:
        logger.info("Checking database connection...")
        engine = get_engine()

        logger.info("Ensuring warehouse schema is ready...")
        create_warehouse_schema(engine)

        logger.info("Triggering run_telecom_etl()...")
        run_telecom_etl()

        logger.info("--- ✅ Local ETL Test Completed Successfully ---")

    except Exception as e:
        logger.error("--- ❌ Local ETL Test Failed ---")
        logger.error(f"Error details: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
