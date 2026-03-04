import pandas as pd
import logging


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning customers data")

    # Remove duplicates
    df = df.drop_duplicates()

    # Drop rows where customer_id is missing
    df = df.dropna(subset=["customer_id"])

    # Standardize phone numbers (remove spaces)
    df["phone_number"] = df["phone_number"].astype(str).str.replace(" ", "")

    # Convert registration_date to datetime
    df["registration_date"] = pd.to_datetime(
        df["registration_date"], errors="coerce")

    return df


def clean_sim_cards(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning sim cards data")

    df = df.drop_duplicates()
    df = df.dropna(subset=["sim_id", "customer_id"])

    df["activation_date"] = pd.to_datetime(
        df["activation_date"], errors="coerce")

    return df


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning transactions data")

    df = df.drop_duplicates()
    df = df.dropna(subset=["transaction_id", "sim_id"])

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], errors="coerce")

    # Remove negative or zero transactions
    df = df[df["amount"] > 0]

    return df


def build_customer_transactions(customers: pd.DataFrame, sim_cards: pd.DataFrame, transactions: pd.DataFrame) -> pd.DataFrame:
    logging.info("Joining datasets to create analytics view")

    # Join sim_cards with customers
    customer_sim = sim_cards.merge(
        customers,
        on="customer_id",
        how="left"
    )

    # Join transactions with the combined customer_sim data
    full_data = transactions.merge(
        customer_sim,
        on="sim_id",
        how="left"
    )

    return full_data
