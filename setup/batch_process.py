import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

# Append the parent directory to sys.path to allow importing from the parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import settings_vendor_load as cfg
import MsrpVendorMapping

pwd_str = f"Pwd={cfg.password};"
# Use hardcoded connection string to match MsrpVendorMapping.py behavior
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};Server=35.172.243.170;Database=luxurymarket_p4;Uid=luxurysitescraper;{pwd_str}"
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

def get_brands_to_process():
    """
    Fetch the list of brands that need to be processed.
    This logic needs to be defined based on your requirements.
    For example, query a table that flags brands with new files or pending updates.
    Here is a placeholder query.
    """
    # Example: Select brands that have data in utb_RetailLoadInitial but not in utb_RetailLoadTemp (or some other logic)
    # For now, just a distinct list from Initial load assuming we want to process everything there.
    sql = "SELECT DISTINCT BrandID FROM utb_RetailLoadInitial" 
    try:
        df = pd.read_sql(sql, engine)
        return df['BrandID'].tolist()
    except Exception as e:
        print(f"Error fetching brands: {e}")
        return []

def clean_initial_load(brand_id):
    print(f"Cleaning Initial Load for Brand ID: {brand_id}")
    try:
        connection = engine.connect()
        sql = text(f"DELETE FROM utb_RetailLoadInitial WHERE BrandID = {brand_id}")
        connection.execute(sql)
        connection.commit()
        connection.close()
        print(f"Cleared Staging for Brand ID: {brand_id}")
    except Exception as e:
        print(f"Error cleaning Initial Load for {brand_id}: {e}")

def process_brand(brand_id):
    print(f"Processing Brand ID: {brand_id}")
    try:
        # 1. Initialize Temp Load (Clear old data for this brand)
        MsrpVendorMapping.initialize_load(brand_id)
        
        # 2. Create SQL for Transformation
        sql = MsrpVendorMapping.create_sql(brand_id)
        
        # 3. Execute Transformation SQL
        # Re-using the sql_execute logic from MsrpVendorMapping or defining here if not exposed
        MsrpVendorMapping.sql_execute(sql)
        
        # 4. Validate and Clean Data
        validate_sql = MsrpVendorMapping.validate_temp_load(brand_id)
        MsrpVendorMapping.sql_execute(validate_sql)
        
        # 5. Clear Staging Data
        clean_initial_load(brand_id)

        print(f"Successfully processed Brand ID: {brand_id}")
        
    except Exception as e:
        print(f"Error processing Brand ID {brand_id}: {e}")

def main():
    print("Starting batch process...")
    brand_ids = get_brands_to_process()
    
    if not brand_ids:
        print("No brands found to process.")
        return

    print(f"Found {len(brand_ids)} brands to process: {brand_ids}")
    
    for brand_id in brand_ids:
        process_brand(brand_id)
        
    print("Batch process completed.")

if __name__ == "__main__":
    main()
