import pandas as pd
import sys
import psycopg2
import config
from my_table_fetcher import get_table_definitions_from_excel
from helpers import (
    process_columns,
    apply_functions,
    get_key_match_columns,
    get_match_columns,
    create_key_mappings,
    save_to_excel
)
from core_analysis import (
compare_primary_keys_and_rows_by_position, 
generate_column_summary,
get_matching_records,
get_detailed_mismatched_records,
get_extra_and_duplicate_records,
compare_columns_and_generate_sheets
)


def main():

    try:
        conn = psycopg2.connect(**config.DB_PARAMS)
        print("Connected to DB!")
    except Exception as e:
        print(f"Error: {e}")
        return

    excel_file = config.EXCEL_FILE  # e.g. "TableDefinitions.xlsx"
    sheet_name = "TABLE INFO"

    # 1. Get the dictionary of DataFrames
    tables_dict = get_table_definitions_from_excel(excel_file, sheet_name, conn)

    # 2. Close the connection if you're done
    conn.close()

    # 3. tables_dict now has table_name -> {"df_old": <DataFrame>, "df_new": <DataFrame>}
    # Example usage:
    for table_name, dfs in tables_dict.items():
        df_old_filtered = dfs["df_old"]
        df_new_filtered = dfs["df_new"]
        print(f"\nAnalyzing {table_name} => old: {len(df_old_filtered)} rows, new: {len(df_new_filtered)} rows")

        # Save filtered DataFrames to FILTERED_OUTPUT_EXCEL_FILE
        dataframes_to_save = {
            'NewDataTable': df_new_filtered,
            'OldDataTable': df_old_filtered
        }
        save_to_excel(dataframes_to_save, f"{table_name}"+config.FILTERED_OUTPUT_EXCEL_FILE)

        metrics = compare_primary_keys_and_rows_by_position(df_new_filtered, df_old_filtered)
        print("High-Level Metrics:")
        print(metrics)

        # Generate column summary: Detailed comparison per column
        column_summary = generate_column_summary(
            df_new_filtered,
            df_old_filtered

        )
        print("\nColumn Summary:")
        print(column_summary)


        matching_records = get_matching_records(
            df_new_filtered,
            df_old_filtered
 
        )
        print("\nNumber of Matching Records:", len(matching_records))
        print(matching_records)

        # Retrieve all detailed mismatched records
        mismatched_records = get_detailed_mismatched_records(
            df_new_filtered,
            df_old_filtered

        )
        print("\nNumber of Mismatched Records:", len(mismatched_records))
        print(mismatched_records)

    # Retrieve extra and duplicate records for OldDataTable
        old_extra_and_duplicates = get_extra_and_duplicate_records(
            df_new_filtered,
            df_old_filtered,
            'Old'
        )
        old_extra = old_extra_and_duplicates['Extra_Records']
        old_duplicates = old_extra_and_duplicates['Duplicate_Records']
        print("\nNumber of Old Table Extra Records:", len(old_extra))
        print("Number of Old Table Duplicate Records:", len(old_duplicates))

        # Retrieve extra and duplicate records for NewDataTable
        new_extra_and_duplicates = get_extra_and_duplicate_records(
            df_new_filtered,
            df_old_filtered,
            'New'
        )
        new_extra = new_extra_and_duplicates['Extra_Records']
        new_duplicates = new_extra_and_duplicates['Duplicate_Records']
        print("Number of New Table Extra Records:", len(new_extra))
        print("Number of New Table Duplicate Records:", len(new_duplicates))
        print(new_extra)
        print(new_duplicates)

        # **Call the Comparison Function**
        comparison_sheets = compare_columns_and_generate_sheets(
            df_new_filtered,
            df_old_filtered
        )
        print("\nNumber of Column Comparison Sheets:", len(comparison_sheets))

        # Prepare DataFrames to Save
        # Convert metrics dictionary to DataFrame with a single row
        metrics_df = pd.DataFrame([metrics])

        # Organize the summary and records DataFrames into a dictionary
        dataframes_to_save_summary = {
            'OldDataTable': df_old_filtered,                # Add OldDataTable first
            'NewDataTable': df_new_filtered,                # Add NewDataTable second
            'Summary': metrics_df,
            'Column_Summary': column_summary,
            'Matching_Records': matching_records,
            'Mismatched_Records': mismatched_records,
            'Old_Table_Extra_Records': old_extra,
            'New_Table_Extra_Records': new_extra,
            'Old_Table_Duplicate_Records': old_duplicates,
            'New_Table_Duplicate_Records': new_duplicates
        }

        # **Integrate Comparison Sheets**
        for sheet_name, df in comparison_sheets.items():
            # Ensure sheet names do not exceed Excel's limit (31 characters) and do not contain invalid characters
            safe_sheet_name = ''.join([c if c not in ['\\', '/', '*', '?', ':', '[', ']'] else '_' for c in sheet_name])
            if len(safe_sheet_name) > 31:
                safe_sheet_name = safe_sheet_name[:31]
            dataframes_to_save_summary[safe_sheet_name] = df

        # Save all summaries and records to ANALYSIS_OUTPUT_FILE
        save_to_excel(dataframes_to_save_summary, f"{table_name}"+config.ANALYSIS_OUTPUT_FILE)
        print(f"\nAnalysis metrics, column summary, matching records, mismatched records, extra records, duplicate records, and column comparisons have been saved to '{config.ANALYSIS_OUTPUT_FILE}'.")

if __name__ == "__main__":
    main()