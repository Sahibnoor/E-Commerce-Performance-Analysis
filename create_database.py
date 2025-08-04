# Day 1: create_database.py

import pandas as pd
import sqlite3
import os

# --- Configuration ---
# Path to the folder containing the CSV files
DATA_DIR = 'data/'
# Name of the SQLite database file to be created
DB_NAME = 'olist.db'
# List of CSV files to import into the database
# The key is the table name, the value is the CSV file name
TABLES = {
    'customers': 'olist_customers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'order_reviews': 'olist_order_reviews_dataset.csv',
    'orders': 'olist_orders_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'product_category_name_translation': 'product_category_name_translation.csv'
}

# --- Database Creation ---
def create_db_and_tables():
    """
    Creates an SQLite database and imports data from CSV files into it.
    """
    # Remove the old database file if it exists to start fresh
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
        print(f"Removed old database '{DB_NAME}'.")

    # Connect to the SQLite database (this will create the file)
    conn = sqlite3.connect(DB_NAME)
    print(f"Database '{DB_NAME}' created.")

    # Loop through the files and import them into the database
    for table_name, csv_file in TABLES.items():
        csv_path = os.path.join(DATA_DIR, csv_file)
        try:
            df = pd.read_csv(csv_path)
            # Convert datetime columns to a consistent string format for SQLite
            for col in df.select_dtypes(include=['datetime64[ns]']).columns:
                df[col] = df[col].astype(str)
            
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"Successfully imported '{csv_file}' into table '{table_name}'.")
        except FileNotFoundError:
            print(f"Error: The file '{csv_path}' was not found.")
        except Exception as e:
            print(f"An error occurred with file '{csv_file}': {e}")

    # Close the database connection
    conn.close()
    print("Database connection closed. All tables have been imported.")

if __name__ == '__main__':
    create_db_and_tables()