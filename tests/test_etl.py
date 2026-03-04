import pandas as pd
import pytest
from src.transform import clean_customers


def test_clean_customers_logic():
    """
    Tests that clean_customers:
    1. Removes duplicate rows.
    2. Drops rows with missing customer_id.
    3. Removes spaces from phone numbers.
    4. Converts registration_date to datetime objects.
    """

    # 1. Create mock data that matches the columns your function expects
    raw_data = {
        # Duplicate (1), Valid (2), Missing (None)
        'customer_id': [1, 1, 2, None],
        'full_name': ['Ahmed One', 'Ahmed One', 'Bob', 'Charlie'],
        'phone_number': ['123 456', '123 456', '789 012', '345 678'],
        'registration_date': ['2024-01-01', '2024-01-01', '2024-02-01', '2024-03-01'],
        'city': ['Tripoli', 'Tripoli', 'London', 'New York']
    }
    df = pd.DataFrame(raw_data)

    # 2. Run the transformation function
    df_clean = clean_customers(df)

    # 3. Assertions (The "Checks")

    # Check 1: Should have exactly 2 rows (1 duplicate removed, 1 null ID removed)
    assert len(df_clean) == 2, f"Expected 2 rows, but got {len(df_clean)}"

    # Check 2: Phone numbers should not have spaces
    # '123 456' -> '123456'
    assert " " not in df_clean['phone_number'].iloc[0]
    assert df_clean['phone_number'].iloc[0] == '123456'

    # Check 3: registration_date should be converted to datetime
    assert pd.api.types.is_datetime64_any_dtype(df_clean['registration_date'])

    # Check 4: Ensure the specific customer_id we wanted to drop is gone
    assert None not in df_clean['customer_id'].values
