# helpers.py

import pandas as pd
from faker import Faker
import random
import sys

def process_columns(df):
    """
    Process the column metadata from the DataFrame.

    Parameters:
    - df (pd.DataFrame): Metadata DataFrame.

    Returns:
    - list of dict: List containing column metadata.
    """
    columns = []
    for _, row in df.iterrows():
        columns.append({
            'column_name': row['Column Name'],
            'data_type': row['Data Type'],
            'key_column': str(row['Key Column']).strip().upper() == 'Y',
            'match_column': str(row['Match Column']).strip().upper() == 'Y',
            'match_type': row['Match Type'] if pd.notna(row['Match Type']) else '',
            'function': row['Function'] if pd.notna(row['Function']) else ''
        })
    return columns

def get_key_mappings(columns_info):
    """
    Retrieve key column names from column metadata.

    Parameters:
    - columns_info (list of dict): Column metadata.

    Returns:
    - list: List of key column names.
    """
    return [col['column_name'] for col in columns_info if col['key_column']]

def get_match_columns(columns_info):
    """
    Retrieve match column names from column metadata.

    Parameters:
    - columns_info (list of dict): Column metadata.

    Returns:
    - list: List of match column names.
    """
    return [col['column_name'] for col in columns_info if col['match_column']]

def get_key_match_columns(new_columns_info, old_columns_info):
    """
    Retrieve key and match columns for both NewDataTable and OldDataTable.

    Parameters:
    - new_columns_info (list of dict): Column metadata for NewDataTable.
    - old_columns_info (list of dict): Column metadata for OldDataTable.

    Returns:
    - tuple: (key_match_new, key_match_old)
    """
    key_match_new = [
        col['column_name'] for col in new_columns_info
        if col['key_column'] or col['match_column']
    ]

    key_match_old = [
        col['column_name'] for col in old_columns_info
        if col['key_column'] or col['match_column']
    ]

    return key_match_new, key_match_old

def generate_data(new_columns_info, old_columns_info, num_rows=15):
    """
    Generate synthetic data for both NewDataTable and OldDataTable.

    Parameters:
    - new_columns_info (list of dict): Column metadata for NewDataTable.
    - old_columns_info (list of dict): Column metadata for OldDataTable.
    - num_rows (int): Number of rows to generate.

    Returns:
    - tuple: (df_new, df_old) DataFrames for NewDataTable and OldDataTable.
    """
    fake = Faker()

    # Identify key and matching columns
    new_keys = [col for col in new_columns_info if col['key_column']]
    old_keys = [col for col in old_columns_info if col['key_column']]
    new_matches = [col for col in new_columns_info if col['match_column']]
    old_matches = [col for col in old_columns_info if col['match_column']]

    # Ensure the number of key and matching columns are the same
    if len(new_keys) != len(old_keys):
        print("Error: Number of key columns differ between tables.")
        sys.exit(1)
    if len(new_matches) != len(old_matches):
        print("Error: Number of matching columns differ between tables.")
        sys.exit(1)

    # Map key columns based on order
    key_pairs = zip(new_keys, old_keys)

    # Map matching columns based on order
    match_pairs = zip(new_matches, old_matches)

    data_new = {}
    data_old = {}

    # Generate data for key columns
    for new_col, old_col in key_pairs:
        data_type = new_col['data_type'].upper()

        if 'BIGINT' in data_type:
            values = [random.randint(1000, 9999) for _ in range(num_rows)]
        elif 'VARCHAR' in data_type:
            values = [fake.word() for _ in range(num_rows)]
        else:
            values = [fake.word() for _ in range(num_rows)]

        data_new[new_col['column_name']] = values
        data_old[old_col['column_name']] = values

    # Generate data for matching columns
    for new_col, old_col in match_pairs:
        data_type = new_col['data_type'].upper()

        if 'BIGINT' in data_type:
            values = [random.randint(1000, 9999) for _ in range(num_rows)]
        elif 'VARCHAR' in data_type:
            values = [fake.word() for _ in range(num_rows)]
        else:
            values = [fake.word() for _ in range(num_rows)]

        data_new[new_col['column_name']] = values
        data_old[old_col['column_name']] = values

    # Create DataFrames with common data
    df_new = pd.DataFrame(data_new)
    df_old = pd.DataFrame(data_old)

    # Generate data for extra columns in NewDataTable
    extra_new = [col for col in new_columns_info if col['column_name'] not in data_new]
    for col in extra_new:
        if 'BIGINT' in col['data_type'].upper():
            df_new[col['column_name']] = [random.randint(1000, 9999) for _ in range(num_rows)]
        elif 'VARCHAR' in col['data_type'].upper():
            df_new[col['column_name']] = [fake.word() for _ in range(num_rows)]
        else:
            df_new[col['column_name']] = [fake.word() for _ in range(num_rows)]
        # Functions are not applied here

    # Generate data for extra columns in OldDataTable
    extra_old = [col for col in old_columns_info if col['column_name'] not in data_old]
    for col in extra_old:
        if 'BIGINT' in col['data_type'].upper():
            df_old[col['column_name']] = [random.randint(1000, 9999) for _ in range(num_rows)]
        elif 'VARCHAR' in col['data_type'].upper():
            df_old[col['column_name']] = [fake.word() for _ in range(num_rows)]
        else:
            df_old[col['column_name']] = [fake.word() for _ in range(num_rows)]
        # Functions are not applied here

    return df_new, df_old

def randomize_old_data(df_old, matching_columns, num_entries=2):
    """
    Randomize a specified number of entries in the matching columns of the OldDataTable,
    restricted to the last 5 rows.

    Parameters:
    - df_old (pd.DataFrame): The OldDataTable DataFrame.
    - matching_columns (list): List of column names to randomize.
    - num_entries (int): Number of entries to randomize per column.

    Returns:
    - pd.DataFrame: The modified OldDataTable DataFrame with randomized entries.
    """
    from faker import Faker
    fake = Faker()

    # Get the indices of the last 5 rows
    last_five_indices = df_old.tail(5).index

    for col in matching_columns:
        if col in df_old.columns:
            # Select random indices within the last 5 rows
            indices = random.sample(list(last_five_indices), min(num_entries, len(last_five_indices)))
            for idx in indices:
                data_type = df_old[col].dtype
                if 'int' in str(data_type):
                    df_old.at[idx, col] = random.randint(1000, 9999)
                elif 'object' in str(data_type):
                    df_old.at[idx, col] = fake.word()
    return df_old

def apply_functions(df, columns_info):
    """
    Apply specified functions to DataFrame columns based on metadata.

    Parameters:
    - df (pd.DataFrame): The DataFrame to process.
    - columns_info (list of dict): Column metadata.

    Returns:
    - pd.DataFrame: The transformed DataFrame.
    """
    # Create a mapping from column names to functions
    function_map = {col['column_name']: col['function'] for col in columns_info if col['function']}

    for col in df.columns:
        function = function_map.get(col, None)
        if function:
            function = function.strip().upper()
            if function == 'UPPER(TRIM(X))':
                # Ensure the column is of string type before applying string operations
                df[col] = df[col].astype(str).str.strip().str.upper()
            elif function == 'CAST(X AS BIGINT)':
                # Attempt to cast the column to integers, coercing errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            else:
                print(f"Warning: Unrecognized function '{function}' for column '{col}'. No transformation applied.")
    return df

def save_to_excel(dataframes_dict, output_file):
    """
    Save multiple DataFrames to an Excel file with separate sheets.

    Parameters:
    - dataframes_dict (dict): Dictionary where keys are sheet names and values are DataFrames.
    - output_file (str): Path to the output Excel file.

    Returns:
    - None

    Exits the program if saving fails.
    """
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for sheet_name, df in dataframes_dict.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Data has been saved to '{output_file}'.")
    except Exception as e:
        print(f"Error saving data to Excel: {e}")
        sys.exit(1)


# helpers.py

def create_key_mappings(new_columns_info, old_columns_info):
    """
    Create a mapping between NewDataTable and OldDataTable key columns.
    
    Assumption:
    - The number of key columns in both tables is the same.
    - Mapping is based on the order of key columns.
    
    Parameters:
    - new_columns_info (list of dict): Column metadata for NewDataTable.
    - old_columns_info (list of dict): Column metadata for OldDataTable.
    
    Returns:
    - dict: Mapping from NewDataTable key columns to OldDataTable key columns.
    
    Exits the program if the number of key columns differ.
    """
    # Extract key columns
    new_keys = [col['column_name'] for col in new_columns_info if col['key_column']]
    old_keys = [col['column_name'] for col in old_columns_info if col['key_column']]
    
    # Validation: Ensure the number of key columns is the same
    if len(new_keys) != len(old_keys):
        print("Error: Number of key columns differ between NewDataTable and OldDataTable.")
        sys.exit(1)
    
    # Create mapping based on the order
    key_mapping = dict(zip(new_keys, old_keys))
    # print(key_mapping)
    print("Key Column Mapping:")
    for new_key, old_key in key_mapping.items():
        print(f"  NewDataTable '{new_key}' --> OldDataTable '{old_key}'")
    
    return key_mapping