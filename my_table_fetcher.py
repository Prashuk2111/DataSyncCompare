import pandas as pd
import sys

def get_table_definitions_from_excel(excel_file, sheet_name, conn):
    """
    Reads table definitions from Excel, for each flagged table:
      1) Executes the 'Old Table SQL' and 'New Table SQL' against the provided DB connection.
      2) Creates a composite primary key column in both resulting DataFrames.
      3) Returns a dictionary keyed by TableName, each containing two DataFrames:
         "df_old" and "df_new" with 'Primary_Key' columns added.

    Excel columns expected:
      - TableName
      - Flag
      - Old Table Expected        (SQL for old table)
      - Old Table Primary Keys    (comma-separated PK columns)
      - New Table Expected        (SQL for new table)
      - New Table Primary Keys    (comma-separated PK columns)

    Returns:
      dict: {
         table_name: {
            "df_old": <DataFrame>,
            "df_new": <DataFrame>
         },
         ...
      }
    """

    try:
        df_tables = pd.read_excel(excel_file, sheet_name=sheet_name)
    except Exception as e:
        print(f"Error reading Excel file '{excel_file}' with sheet '{sheet_name}': {e}")
        sys.exit(1)

    # Columns we expect
    required_cols = [
        "TableName",
        "Flag",
        "Old Table Expected",
        "Old Table Primary Keys",
        "New Table Expected",
        "New Table Primary Keys"
    ]
    for col in required_cols:
        if col not in df_tables.columns:
            print(f"Error: Required column '{col}' not found in '{excel_file}'.")
            sys.exit(1)

    # This dictionary will hold { table_name: {"df_old": <DataFrame>, "df_new": <DataFrame>} }
    table_data = {}

    for idx, row in df_tables.iterrows():
        flag_val = str(row["Flag"]).strip().upper()  # e.g. "TRUE", "YES", "1"
        if flag_val not in ["TRUE", "YES", "1"]:
            continue  # Skip rows not flagged as TRUE

        table_name = str(row["TableName"]).strip()

        # Extract queries
        old_sql = str(row["Old Table Expected"]).strip()
        new_sql = str(row["New Table Expected"]).strip()

        # Extract PK definitions
        old_pk_str = str(row["Old Table Primary Keys"]).strip()
        new_pk_str = str(row["New Table Primary Keys"]).strip()
        old_pk_cols = [c.strip() for c in old_pk_str.split(",")] if old_pk_str else []
        new_pk_cols = [c.strip() for c in new_pk_str.split(",")] if new_pk_str else []

        # If no PK columns, skip
        if not old_pk_cols or not new_pk_cols:
            print(f"[{table_name}] No PK columns specified. Skipping.")
            continue

        # Attempt to run each query
        try:
            print(type(old_sql))
            df_old = pd.read_sql_query(old_sql, conn)
            df_new = pd.read_sql_query(new_sql, conn)
        except Exception as e:
            print(f"[{table_name}] Error executing queries:\n{e}")
            continue

        # Validate that these PK columns exist
        missing_old = [col for col in old_pk_cols if col not in df_old.columns]
        missing_new = [col for col in new_pk_cols if col not in df_new.columns]

        if missing_old:
            print(f"[{table_name}] Missing old PK columns: {missing_old}. Skipping.")
            continue
        if missing_new:
            print(f"[{table_name}] Missing new PK columns: {missing_new}. Skipping.")
            continue

        # Build composite PK
        def build_pk(row, pk_cols):
            return "_".join(str(row[col]) for col in pk_cols)

        df_old["Primary_Key"] = df_old.apply(lambda r: build_pk(r, old_pk_cols), axis=1)
        df_new["Primary_Key"] = df_new.apply(lambda r: build_pk(r, new_pk_cols), axis=1)

        # Store in dictionary
        table_data[table_name] = {
            "df_old": df_old,
            "df_new": df_new
        }

        print(f"[{table_name}] -> df_old: {len(df_old)} rows, df_new: {len(df_new)} rows, PK columns created.")

    return table_data