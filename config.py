# config.py

import os


DB_PARAMS = {
    # 'dbname': os.getenv('DB_NAME'),
    'dbname': 'my_new_db',
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

EXCEL_FILE = 'TwoTableComparision_redefined_final.xlsx'          # Metadata Excel file
OUTPUT_EXCEL_FILE = 'TwoTableComparison_output.xlsx'       # Synthetic data Excel file
FILTERED_OUTPUT_EXCEL_FILE = 'Filtered_Tables.xlsx'        # Filtered data Excel file
POSTGRES_OUTPUT_FILE = 'TwoTableComparision_postgres.xlsx' # PostgreSQL output file (if needed)
ANALYSIS_OUTPUT_FILE = 'Analysis_Summary.xlsx'