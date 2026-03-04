# telecom_csv_pipeline.py
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
import sys
sys.path.append("/opt/airflow")


# 🚀 STEP 1: Fix Python Path
# Airflow needs to know where the 'src' folder is located inside the container
sys.path.append("/opt/airflow")

# 🚀 STEP 2: Import your ETL logic
try:
    from src.pipeline import run_telecom_etl
except ImportError as e:
    print(f"CRITICAL: Could not import src.pipeline! Error: {e}")
    raise

# 🚀 STEP 3: DAG Configuration
default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="telecom_csv_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["telecom", "etl"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_full_telecom_etl",
        python_callable=run_telecom_etl,
    )

    run_etl
