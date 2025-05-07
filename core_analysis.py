# core_analysis.py

import pandas as pd
import numpy as np

def compare_primary_keys_and_rows_by_position(df_new_filtered, df_old_filtered):
    """
    Compare primary keys and entire rows of NewDataTable and OldDataTable, even if non-key column names differ,
    but appear in the same order. This function:
    - Renames `df_old_filtered` non-key columns to match `df_new_filtered` by position.
    - Computes:
      - OldDataTable Record Count
      - NewDataTable Record Count
      - OldDataTable Extra Records
      - NewDataTable Extra Records
      - OldDataTable Duplicate Records
      - NewDataTable Duplicate Records
      - Match Record Count (full row match)
      - Mismatch Record Count (full row mismatch or missing in other table)

    Parameters:
    - df_new_filtered (pd.DataFrame): Filtered NewDataTable with 'Primary_Key'.
    - df_old_filtered (pd.DataFrame): Filtered OldDataTable with 'Primary_Key'.
    - new_columns_info (list): Metadata for NewDataTable columns.
    - old_columns_info (list): Metadata for OldDataTable columns.

    Returns:
    - dict: Dictionary with computed metrics.
    """

    # Identify the primary key and non-key columns from df_new_filtered
    all_cols = df_new_filtered.columns.tolist()
    if 'Primary_Key' in all_cols:
        non_key_cols_new = [c for c in all_cols if c != 'Primary_Key']
    else:
        non_key_cols_new = all_cols

    # Similarly identify non-key columns in df_old_filtered
    old_all_cols = df_old_filtered.columns.tolist()
    if 'Primary_Key' in old_all_cols:
        non_key_cols_old = [c for c in old_all_cols if c != 'Primary_Key']
    else:
        non_key_cols_old = old_all_cols

    # Check if both have the same number of non-key columns
    if len(non_key_cols_new) != len(non_key_cols_old):
        raise ValueError("Number of non-key columns differ between NewDataTable and OldDataTable filtered DataFrames.")

    # Rename df_old_filtered's non-key columns by position to match df_new_filtered
    rename_map = {old_col: new_col for old_col, new_col in zip(non_key_cols_old, non_key_cols_new)}
    df_old_renamed = df_old_filtered.rename(columns=rename_map)

    # Record Counts
    old_record_count = len(df_old_renamed)
    new_record_count = len(df_new_filtered)

    # Duplicate Records
    old_duplicates = df_old_renamed.duplicated(subset='Primary_Key', keep=False).sum()
    new_duplicates = df_new_filtered.duplicated(subset='Primary_Key', keep=False).sum()

    # Merge DataFrames on Primary_Key
    merged = df_new_filtered.merge(
        df_old_renamed,
        on='Primary_Key',
        how='outer',
        indicator=True,
        suffixes=('_new', '_old')
    )

    # Extra Records
    new_extra_records = merged[merged['_merge'] == 'left_only'].shape[0]
    old_extra_records = merged[merged['_merge'] == 'right_only'].shape[0]

    # Determine which rows are present in both DataFrames
    both_mask = (merged['_merge'] == 'both')

    # Compare non-key columns for rows present in both tables to determine full matches
    full_match_mask = pd.Series(True, index=merged.index)

    # For each non-key column, we have col_new and col_old after merging
    # Since columns are now aligned by name, for each non-key column c:
    # - c_new = c + "_new"
    # - c_old = c + "_old"
    for c in non_key_cols_new:
        col_new = c + "_new"
        col_old = c + "_old"
        if col_new in merged.columns and col_old in merged.columns:
            full_match_mask &= (merged[col_new] == merged[col_old])
        else:
            # If columns missing, treat as mismatch
            full_match_mask &= False

    # Match Record Count: rows with _merge='both' and all columns matching
    match_record_count = merged[both_mask & full_match_mask].shape[0]

    # Mismatch Record Count:
    # 1. Records with _merge='left_only' or 'right_only' are mismatches
    # 2. Records with _merge='both' but not fully matching are also mismatches
    left_only_count = merged[merged['_merge'] == 'left_only'].shape[0]
    right_only_count = merged[merged['_merge'] == 'right_only'].shape[0]
    both_mismatch_count = merged[both_mask & ~full_match_mask].shape[0]

    mismatch_record_count = left_only_count + right_only_count + both_mismatch_count

    results = {
        "OldDataTable Record Count": old_record_count,
        "NewDataTable Record Count": new_record_count,
        "OldDataTable Extra Records": old_extra_records,
        "NewDataTable Extra Records": new_extra_records,
        "OldDataTable Duplicate Records": old_duplicates,
        "NewDataTable Duplicate Records": new_duplicates,
        "Match Record Count": match_record_count,
        "Mismatch Record Count": mismatch_record_count
    }

    return results

def generate_column_summary(df_new_filtered, df_old_filtered):
    """
    Generate a column-wise summary of matches and mismatches between NewDataTable and OldDataTable.
    This function handles columns with different names but aligned by position.
    
    Parameters:
    - df_new_filtered (pd.DataFrame): Filtered NewDataTable DataFrame with 'Primary_Key'.
    - df_old_filtered (pd.DataFrame): Filtered OldDataTable DataFrame with 'Primary_Key'.
    - new_columns_info (list of dict): Column metadata for NewDataTable.
    - old_columns_info (list of dict): Column metadata for OldDataTable.
    
    Returns:
    - pd.DataFrame: Column summary with specified metrics.
    """
    
    # Identify non-key columns in NewDataTable
    all_new_cols = df_new_filtered.columns.tolist()
    if 'Primary_Key' in all_new_cols:
        non_key_cols_new = [c for c in all_new_cols if c != 'Primary_Key']
    else:
        non_key_cols_new = all_new_cols
    
    # Identify non-key columns in OldDataTable
    all_old_cols = df_old_filtered.columns.tolist()
    if 'Primary_Key' in all_old_cols:
        non_key_cols_old = [c for c in all_old_cols if c != 'Primary_Key']
    else:
        non_key_cols_old = all_old_cols
    
    # Ensure the number and order of non-key columns are the same
    if len(non_key_cols_new) != len(non_key_cols_old):
        raise ValueError("Number of non-key columns differ between NewDataTable and OldDataTable.")
    
    # Rename OldDataTable's non-key columns to match NewDataTable's by position
    rename_map = {old_col: new_col for old_col, new_col in zip(non_key_cols_old, non_key_cols_new)}
    df_old_renamed = df_old_filtered.rename(columns=rename_map)
    
    # Merge DataFrames on Primary_Key with suffixes
    merged = df_new_filtered.merge(
        df_old_renamed,
        on='Primary_Key',
        how='outer',
        indicator=True,
        suffixes=('_new', '_old')
    )
    
    # Initialize summary list
    summary = []
    
    for c in non_key_cols_new:
        col_new = f"{c}_new"
        col_old = f"{c}_old"
        
        # Check if both columns exist
        if col_new not in merged.columns or col_old not in merged.columns:
            print(f"Warning: Columns {col_new} or {col_old} not found in merged DataFrame.")
            # Assign zero counts if columns are missing
            summary.append({
                "Column Name": c,
                "Matches": 0,
                "Mismatches": 0,
                "Not Null - Not Null": 0,
                "Not Null - Null": 0,
                "Null - Not Null": 0,
                "Null - Null": 0
            })
            continue
        
        # Define Matches: values are equal or both are null
        matches = (merged[col_new] == merged[col_old]) | (merged[col_new].isnull() & merged[col_old].isnull())
        matches_count = matches.sum()
        
        # Define Mismatches: values are not equal and not both null
        mismatches = ~matches
        mismatches_count = mismatches.sum()
        
        # Subcategories within Mismatches
        mismatched_rows = merged[mismatches]
        
        # Not Null - Not Null: Both not null and different
        not_null_not_null = mismatched_rows[col_new].notnull() & mismatched_rows[col_old].notnull()
        not_null_not_null_count = not_null_not_null.sum()
        
        # Not Null - Null: New has value, Old is null
        not_null_null = mismatched_rows[col_new].notnull() & mismatched_rows[col_old].isnull()
        not_null_null_count = not_null_null.sum()
        
        # Null - Not Null: New is null, Old has value
        null_not_null = mismatched_rows[col_new].isnull() & mismatched_rows[col_old].notnull()
        null_not_null_count = null_not_null.sum()
        
        # Null - Null: Both are null
        null_null = (merged[col_new].isnull()) & (merged[col_old].isnull())
        null_null_count = null_null.sum()
        
        # Append the metrics to the summary
        summary.append({
            "Column Name": c,
            "Matches": matches_count,
            "Mismatches": mismatches_count,
            "Not Null - Not Null": not_null_not_null_count,
            "Not Null - Null": not_null_null_count,
            "Null - Not Null": null_not_null_count,
            "Null - Null": null_null_count
        })
    
    # Convert summary list to DataFrame
    summary_df = pd.DataFrame(summary)
    
    return summary_df

# core_analysis.py




def get_matching_records(df_new_filtered, df_old_filtered):
    """
    Retrieve all matching records from NewDataTable that exactly match records in OldDataTable.
    Matching is based on 'Primary_Key' and identical values across all non-key columns.
    
    Parameters:
    - df_new_filtered (pd.DataFrame): Filtered NewDataTable DataFrame with 'Primary_Key'.
    - df_old_filtered (pd.DataFrame): Filtered OldDataTable DataFrame with 'Primary_Key'.
    - new_columns_info (list of dict): Column metadata for NewDataTable.
    - old_columns_info (list of dict): Column metadata for OldDataTable.
    
    Returns:
    - pd.DataFrame: DataFrame containing all matching records from NewDataTable.
    """
    
    # Identify non-key columns in NewDataTable
    all_new_cols = df_new_filtered.columns.tolist()
    if 'Primary_Key' in all_new_cols:
        non_key_cols_new = [c for c in all_new_cols if c != 'Primary_Key']
    else:
        non_key_cols_new = all_new_cols
    
    # Identify non-key columns in OldDataTable
    all_old_cols = df_old_filtered.columns.tolist()
    if 'Primary_Key' in all_old_cols:
        non_key_cols_old = [c for c in all_old_cols if c != 'Primary_Key']
    else:
        non_key_cols_old = all_old_cols
    
    # Ensure the number and order of non-key columns are the same
    if len(non_key_cols_new) != len(non_key_cols_old):
        raise ValueError("Number of non-key columns differ between NewDataTable and OldDataTable.")
    
    # Rename OldDataTable's non-key columns to match NewDataTable's by position
    rename_map = {old_col: new_col for old_col, new_col in zip(non_key_cols_old, non_key_cols_new)}
    df_old_renamed = df_old_filtered.rename(columns=rename_map)
    
    # Merge DataFrames on Primary_Key with suffixes
    merged = df_new_filtered.merge(
        df_old_renamed,
        on='Primary_Key',
        how='inner',  # Only keep records present in both tables
        suffixes=('_new', '_old')
    )
    
    # Initialize full_match_mask to True for all merged records
    full_match_mask = pd.Series(True, index=merged.index)
    
    # Iterate over non-key columns to update full_match_mask
    for c in non_key_cols_new:
        col_new = f"{c}_new"
        col_old = f"{c}_old"
        if col_new in merged.columns and col_old in merged.columns:
            # Compare values, considering NaNs as equal
            matches = (merged[col_new] == merged[col_old]) | (merged[col_new].isnull() & merged[col_old].isnull())
            full_match_mask &= matches
        else:
            # If columns are missing, treat as mismatches
            full_match_mask &= False
    
    # Filter merged DataFrame to only include fully matched records
    matched_records = merged[full_match_mask].copy()
    
    # Optionally, drop the '_old' suffixed columns to retain only NewDataTable's data
    # This depends on whether you want to keep OldDataTable's data in the output
    # For clarity, we'll keep only NewDataTable's columns
    columns_to_keep = [col for col in matched_records.columns if col.endswith('_new') or col == 'Primary_Key']
    matched_records = matched_records[columns_to_keep].rename(columns=lambda x: x.replace('_new', '') if x.endswith('_new') else x)
    
    return matched_records



def get_detailed_mismatched_records(df_new_filtered, df_old_filtered):
    """
    Retrieve all mismatched records between NewDataTable and OldDataTable.
    For each mismatch, provide the entire row with mismatched cells containing "O: value, N: value".
    
    Parameters:
    - df_new_filtered (pd.DataFrame): Filtered NewDataTable DataFrame with 'Primary_Key'.
    - df_old_filtered (pd.DataFrame): Filtered OldDataTable DataFrame with 'Primary_Key'.
    - new_columns_info (list of dict): Column metadata for NewDataTable.
    - old_columns_info (list of dict): Column metadata for OldDataTable.
    
    Returns:
    - pd.DataFrame: DataFrame containing all mismatched records with detailed information.
    """

    # Identify non-key columns in NewDataTable
    all_new_cols = df_new_filtered.columns.tolist()
    if 'Primary_Key' in all_new_cols:
        non_key_cols_new = [c for c in all_new_cols if c != 'Primary_Key']
    else:
        non_key_cols_new = all_new_cols
    
    # Identify non-key columns in OldDataTable
    all_old_cols = df_old_filtered.columns.tolist()
    if 'Primary_Key' in all_old_cols:
        non_key_cols_old = [c for c in all_old_cols if c != 'Primary_Key']
    else:
        non_key_cols_old = all_old_cols
    
    # Ensure the number and order of non-key columns are the same
    if len(non_key_cols_new) != len(non_key_cols_old):
        raise ValueError("Number of non-key columns differ between NewDataTable and OldDataTable.")
    
    # Rename OldDataTable's non-key columns to match NewDataTable's by position
    rename_map = {old_col: new_col for old_col, new_col in zip(non_key_cols_old, non_key_cols_new)}
    df_old_renamed = df_old_filtered.rename(columns=rename_map)
    
    # Merge DataFrames on Primary_Key with suffixes
    merged = df_new_filtered.merge(
        df_old_renamed,
        on='Primary_Key',
        how='outer',
        indicator=True,
        suffixes=('_new', '_old')   # Note: We'll actually reference columns explicitly
    )
    
    # Initialize a copy of the merged DataFrame to modify
    detailed_mismatches = merged.copy()
    
    # Track which rows have at least one mismatch
    mismatch_row_mask = pd.Series(False, index=merged.index)
    
    # Iterate over each non-key column to identify mismatches
    for c in non_key_cols_new:
        col_new = f"{c}_new"
        col_old = f"{c}_old"
        
        # Check if both columns exist in the merged DataFrame
        if col_new not in merged.columns or col_old not in merged.columns:
            print(f"Warning: Columns {col_new} or {col_old} not found in merged DataFrame.")
            # For missing columns, treat entire column as mismatched
            detailed_mismatches[c] = detailed_mismatches[col_new].apply(
                lambda x: f"O: {detailed_mismatches[col_old].iloc[x.name] if pd.notnull(detailed_mismatches[col_old].iloc[x.name]) else 'null'}, N: {x if pd.notnull(x) else 'null'}"
            )
            mismatch_row_mask = mismatch_row_mask | True
            continue  # Move to the next column
        
        # Create boolean masks for comparisons
        equal_mask = (merged[col_new] == merged[col_old])
        both_null_mask = merged[col_new].isnull() & merged[col_old].isnull()
        mismatch_mask = ~(equal_mask | both_null_mask)
        
        # Update the mismatch_row_mask
        mismatch_row_mask = mismatch_row_mask | mismatch_mask
        
        # Replace mismatched cells with "O: <old_value>, N: <new_value>"
        detailed_mismatches.loc[mismatch_mask, c] = (
            "O: " 
            + merged.loc[mismatch_mask, col_old].fillna('null').astype(str) 
            + ", N: " 
            + merged.loc[mismatch_mask, col_new].fillna('null').astype(str)
        )

        # For matched cells, retain the NewDataTable's value
        detailed_mismatches.loc[~mismatch_mask, c] = merged.loc[~mismatch_mask, col_new]
    
    # Handle records present only in NewDataTable or OldDataTable
    # Rows present only in NewDataTable: all old values are null -> "O: null, N: <value>"
    only_new_mask = merged['_merge'] == 'left_only'
    for c in non_key_cols_new:
        detailed_mismatches.loc[only_new_mask, c] = (
            "O: null, N: " 
            + merged.loc[only_new_mask, f"{c}_new"].fillna('null').astype(str)
        )
    
    # Rows present only in OldDataTable: all new values are null -> "O: <value>, N: null"
    only_old_mask = merged['_merge'] == 'right_only'
    for c in non_key_cols_new:
        detailed_mismatches.loc[only_old_mask, c] = (
            "O: " 
            + merged.loc[only_old_mask, f"{c}_old"].fillna('null').astype(str) 
            + ", N: null"
        )
    
    # Filter to include only rows with mismatches
    detailed_mismatches = detailed_mismatches[mismatch_row_mask | only_new_mask | only_old_mask].copy()
    
    # Select relevant columns: Primary_Key and non-key columns
    columns_to_keep = ['Primary_Key'] + non_key_cols_new
    detailed_mismatches = detailed_mismatches[columns_to_keep]
    
    return detailed_mismatches


# core_analysis.py



def get_extra_and_duplicate_records(df_new_filtered, df_old_filtered, table):
    """
    Retrieve Extra Records and Duplicate Records for a specified table.

    Parameters:
    - df_new_filtered (pd.DataFrame): Filtered NewDataTable with 'Primary_Key'.
    - df_old_filtered (pd.DataFrame): Filtered OldDataTable with 'Primary_Key'.
    - table (str): Specifies which table to analyze ('Old' or 'New').

    Returns:
    - dict: Dictionary containing two DataFrames:
        - 'Extra_Records': Records present only in the specified table.
        - 'Duplicate_Records': Duplicate records within the specified table based on 'Primary_Key'.
    """
    
    if table not in ['Old', 'New']:
        raise ValueError("Parameter 'table' must be either 'Old' or 'New'.")
    
    if table == 'Old':
        # Extra Records: Present in OldDataTable but not in NewDataTable
        extra_records = df_old_filtered.merge(
            df_new_filtered[['Primary_Key']],
            on='Primary_Key',
            how='left',
            indicator=True
        )
        # Filter records that are only in OldDataTable
        extra_records = extra_records[extra_records['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        # Duplicate Records: Duplicates within OldDataTable
        duplicate_records = df_old_filtered[df_old_filtered.duplicated(subset=['Primary_Key'], keep=False)]
        
    elif table == 'New':
        # Extra Records: Present in NewDataTable but not in OldDataTable
        extra_records = df_new_filtered.merge(
            df_old_filtered[['Primary_Key']],
            on='Primary_Key',
            how='left',
            indicator=True
        )
        # Filter records that are only in NewDataTable
        extra_records = extra_records[extra_records['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        # Duplicate Records: Duplicates within NewDataTable
        duplicate_records = df_new_filtered[df_new_filtered.duplicated(subset=['Primary_Key'], keep=False)]
    
    return {
        'Extra_Records': extra_records,
        'Duplicate_Records': duplicate_records
    }

# core_analysis.py

def compare_columns_and_generate_sheets(df_new_filtered, df_old_filtered):
    """
    Compare each column between OldDataTable and NewDataTable and prepare DataFrames for Excel sheets.
    Assumes that both tables have the same number of columns (excluding 'Primary_Key') and that
    corresponding columns are in the same order.

    Parameters:
    - df_new_filtered (pd.DataFrame): Filtered NewDataTable with 'Primary_Key'.
    - df_old_filtered (pd.DataFrame): Filtered OldDataTable with 'Primary_Key'.

    Returns:
    - dict: Dictionary where keys are column names from NewDataTable and values are DataFrames with:
        - Primary_Key
        - [ColumnName]_Expected
        - [ColumnName]_Actual
        - [ColumnName]_Comparison
    """
    comparison_sheets = {}

    # Identify columns to compare (excluding 'Primary_Key')
    new_columns = [col for col in df_new_filtered.columns if col != 'Primary_Key']
    old_columns = [col for col in df_old_filtered.columns if col != 'Primary_Key']

    # Check if the number of columns matches
    if len(new_columns) != len(old_columns):
        print("Error: The number of columns in NewDataTable and OldDataTable do not match.")
        return comparison_sheets

    # Create a mapping from OldDataTable columns to NewDataTable columns based on their positions
    rename_mapping = {old_col: new_col for old_col, new_col in zip(old_columns, new_columns)}

    # Rename the columns in OldDataTable
    df_old_filtered_renamed = df_old_filtered.rename(columns=rename_mapping)

    # Now, both DataFrames have identical column names, facilitating straightforward comparison
    columns_to_compare = new_columns  # Since names are now aligned

    for column in columns_to_compare:
        if column not in df_old_filtered_renamed.columns:
            print(f"Warning: Column '{column}' not found in OldDataTable after renaming. Skipping comparison for this column.")
            continue

        # Merge the two DataFrames on 'Primary_Key' for the current column
        merged_df = pd.merge(
            df_old_filtered_renamed[['Primary_Key', column]],
            df_new_filtered[['Primary_Key', column]],
            on='Primary_Key',
            how='inner',
            suffixes=('_Expected', '_Actual')
        )

        # Replace NaN with 'null' for clarity
        merged_df[column + '_Expected'] = merged_df[column + '_Expected'].fillna('null').astype(str)
        merged_df[column + '_Actual'] = merged_df[column + '_Actual'].fillna('null').astype(str)

        # Create '[ColumnName]_Comparison' column
        comparison_column = f"{column}_Comparison"
        merged_df[comparison_column] = 'E: ' + merged_df[column + '_Expected'] + ', A: ' + merged_df[column + '_Actual']

        # Select and rename relevant columns to match the required naming convention
        comparison_df = merged_df[['Primary_Key', column + '_Expected', column + '_Actual', comparison_column]].copy()
        comparison_df.rename(columns={
            column + '_Expected': f"{column}_Expected",
            column + '_Actual': f"{column}_Actual",
            comparison_column: f"{column}_Comparison"
        }, inplace=True)

        # Assign the DataFrame to the dictionary with the column name as the key
        comparison_sheets[column] = comparison_df

    return comparison_sheets