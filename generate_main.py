# generate_main.py

import pandas as pd
import sys
import config
from helpers import (
    process_columns,
    generate_data,
    randomize_old_data,
    get_match_columns,
    get_key_mappings,
    save_to_excel
)

def main():
    # Read the metadata for NewDataTable and OldDataTable
    try:
        df_new_meta = pd.read_excel(config.EXCEL_FILE, sheet_name='NewDataTable', skiprows=1)
        df_old_meta = pd.read_excel(config.EXCEL_FILE, sheet_name='OldDataTable', skiprows=1)
    except Exception as e:
        print(f"Error reading metadata Excel file: {e}")
        sys.exit(1)

    # Process column metadata
    new_columns_info = process_columns(df_new_meta)
    old_columns_info = process_columns(df_old_meta)

    # --- In-Line Validation Starts Here ---
    # Validate the number of key columns
    new_keys = get_key_mappings(new_columns_info)
    old_keys = get_key_mappings(old_columns_info)
    if len(new_keys) != len(old_keys):
        print("Error: Number of key columns differ between NewDataTable and OldDataTable.")
        sys.exit(1)

    # Validate the number of matching columns
    new_matches = get_match_columns(new_columns_info)
    old_matches = get_match_columns(old_columns_info)
    if len(new_matches) != len(old_matches):
        print("Error: Number of matching columns differ between NewDataTable and OldDataTable.")
        sys.exit(1)


    # Generate synthetic data
    df_new, df_old = generate_data(new_columns_info, old_columns_info, num_rows=15)

    # Identify matching columns
    matching_new = get_match_columns(new_columns_info)
    matching_old = get_match_columns(old_columns_info)

    # Randomize entries in OldDataTable's matching columns
    df_old = randomize_old_data(df_old, matching_old, num_entries=2)

    # Prepare dataframes for saving
    dataframes_to_save = {
        'NewDataTable': df_new,
        'OldDataTable': df_old
    }

    # Save the synthetic data to OUTPUT_EXCEL_FILE using the helper function
    save_to_excel(dataframes_to_save, config.OUTPUT_EXCEL_FILE)

if __name__ == "__main__":
    main()