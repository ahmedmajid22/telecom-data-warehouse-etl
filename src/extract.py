# src/extract.py

import pandas as pd
import logging
import os

# ✅ Get logger (do NOT configure logging here)
logger = logging.getLogger(__name__)


def read_csv_file(file_path: str, required_columns: list):
    """
    Reads a CSV file and validates required columns.
    Raises:
        FileNotFoundError -> if file does not exist
        ValueError -> if required columns are missing
    """

    try:
        logger.info(f"Reading file: {file_path}")

        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Read CSV
        df = pd.read_csv(file_path)

        # Validate required columns
        missing_columns = [
            col for col in required_columns if col not in df.columns
        ]

        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")

        logger.info(
            f"Successfully loaded {file_path} with {len(df)} rows"
        )

        return df

    except Exception as e:
        logger.error(f"Error reading {file_path}: {str(e)}")
        raise
