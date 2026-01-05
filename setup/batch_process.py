import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
import requests
import io
import csv
import uuid
import datetime
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import urllib3

# Disable SSL warnings for unverified requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Append the parent directory to sys.path to allow importing from the parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import settings_vendor_load as cfg
import MsrpVendorMapping

pwd_str = f"Pwd={cfg.password};"
# Use hardcoded connection string to match MsrpVendorMapping.py behavior
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};Server=35.172.243.170;Database=luxurymarket_p4;Uid=luxurysitescraper;{pwd_str}"
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

def log(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def get_jobs_to_process():
    """
    Fetch pending jobs from utb_BrandScanJobs.
    Condition: ParsingEnd is NOT NULL (Done) AND HarvestStatus is NULL (Not processed)
    Order: Recent jobs first (ID DESC)
    """
    sql = """
    SELECT ID, BrandId, ParsingResultUrl, ScanUrl
    FROM utb_BrandScanJobs 
    WHERE ParsingEnd IS NOT NULL 
    AND (HarvestStatus IS NULL OR HarvestStatus = 'Pending')
    ORDER BY ID DESC
    """
    try:
        df = pd.read_sql(sql, engine)
        if not df.empty:
            return df.to_dict('records')
        return []
    except Exception as e:
        log(f"Error fetching jobs: {e}")
        return []

def mark_job_complete(job_id):
    log(f"Marking Job {job_id} as Completed...")
    try:
        connection = engine.connect()
        sql = text(f"UPDATE utb_BrandScanJobs SET HarvestStatus = 'Completed' WHERE ID = {job_id}")
        connection.execute(sql)
        connection.commit()
        connection.close()
        log(f"Job {job_id} Marked Completed.")
    except Exception as e:
        log(f"Error updating job status: {e}")

def mark_job_failed(job_id, reason="Error"):
    log(f"Marking Job {job_id} as Failed. Reason: {reason}")
    try:
        connection = engine.connect()
        # Escape single quotes in reason
        clean_reason = reason.replace("'", "''")
        # Update HarvestStatus to 'Failed' or 'Error'
        sql = text(f"UPDATE utb_BrandScanJobs SET HarvestStatus = 'Failed' WHERE ID = {job_id}")
        connection.execute(sql)
        connection.commit()
        connection.close()
        log(f"Job {job_id} Marked Failed.")
    except Exception as e:
        log(f"Error updating job failure status: {e}")

def download_and_preprocess_csv(url, output_path):
    # Filter out invalid "API" URLs
    if not url or not url.startswith('http'):
        log(f"Invalid URL detected: {url}")
        return False

    log(f"Downloading CSV from {url}")
    try:
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.3"}
        
        # Disable SSL verification to handle s3 buckets with dots in hostname
        response = session.get(url, headers=headers, allow_redirects=True, verify=False)
        response.raise_for_status()

        # Clean CSV text
        lines = response.text.strip().split('\n')
        csv_file = io.StringIO('\n'.join(lines))
        csv_reader = csv.reader(csv_file, quoting=csv.QUOTE_ALL)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as output_file:
            csv_writer = csv.writer(output_file, quoting=csv.QUOTE_ALL)
            for row in csv_reader:
                csv_writer.writerow(row)
        return True
    except Exception as e:
        log(f"Error downloading CSV: {e}")
        return False

def generate_column_names(csv_file_path):
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        try:
            first_row = next(csv_reader)
            num_columns = len(first_row)
        except StopIteration:
            return [] # Empty file

    column_names = ['BrandID'] + [f'F{i}' for i in range(num_columns)]
    return column_names

def clean_initial_load(brand_id):
    log(f"Cleaning Initial Load for Brand ID: {brand_id}")
    try:
        connection = engine.connect()
        sql = text(f"DELETE FROM utb_RetailLoadInitial WHERE BrandID = {brand_id}")
        connection.execute(sql)
        connection.commit()
        connection.close()
        log(f"Cleared Staging for Brand ID: {brand_id}")
    except Exception as e:
        log(f"Error cleaning Initial Load for {brand_id}: {e}")

def clear_temp_table():
    log("Clearing Temp Table before batch run...")
    try:
        connection = engine.connect()
        sql = text("DELETE FROM utb_RetailLoadTemp") 
        connection.execute(sql)
        connection.commit()
        connection.close()
        log("Temp Table Cleared.")
    except Exception as e:
        log(f"Error clearing Temp Table: {e}")

def process_job(job):
    job_id = job['ID']
    brand_id = job['BrandId']
    csv_url = job['ParsingResultUrl']
    scan_url = job['ScanUrl']

    log(f"Processing Job ID: {job_id} for Brand: {brand_id}")
    
    # 1. Clear Staging for this Brand (Clean start)
    clean_initial_load(brand_id)

    # 2. Download and Load to Initial
    temp_file = f"temp_{uuid.uuid4()}.csv"
    if download_and_preprocess_csv(csv_url, temp_file):
        try:
            df = pd.read_csv(temp_file, quotechar='"', header=None, encoding='utf-8', encoding_errors='replace')
            if df.empty:
                log("Downloaded CSV is empty.")
                os.remove(temp_file)
                mark_job_failed(job_id, "CSV Empty")
                return

            df.insert(0, 'BrandID', brand_id)
            df.columns = generate_column_names(temp_file)
            
            # Add ScanUrl as the last column (imitating Vendor_load.py logic)
            final_col_idx = int(list(df.columns)[-1].replace('F',''))
            df.insert(len(df.columns), f'F{final_col_idx+1}', scan_url)

            # Insert to Staging
            df.to_sql('utb_RetailLoadInitial', engine, if_exists='append', index=False)
            log("Loaded data to utb_RetailLoadInitial")
        except Exception as e:
            log(f"Error loading CSV to Staging: {e}")
            mark_job_failed(job_id, f"Staging Load Error: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            return
        
        if os.path.exists(temp_file):
            os.remove(temp_file)

        # 3. Transform and Validate
        try:
            # Skip Initialize Temp to aggregate data instead of clearing
            # MsrpVendorMapping.initialize_load(brand_id)
            
            # Transform
            sql = MsrpVendorMapping.create_sql(brand_id)
            MsrpVendorMapping.sql_execute(sql)
            
            # Validate
            validate_sql = MsrpVendorMapping.validate_temp_load(brand_id)
            MsrpVendorMapping.sql_execute(validate_sql)
            
            log(f"Successfully processed Brand ID: {brand_id}")
            
            # 4. Mark Job Complete
            mark_job_complete(job_id)

             # 5. Clear Staging Data (Cleanup after success)
            clean_initial_load(brand_id)

        except Exception as e:
            log(f"Error during transformation phase: {e}")
            mark_job_failed(job_id, f"Transformation Error: {e}")

    else:
        log("Skipping job due to download failure.")
        mark_job_failed(job_id, "Download Failed")

def main():
    log("Starting batch process...")
    
    # Data is aggregated in utb_RetailLoadTemp, so we do not clear it at start.
    # clear_temp_table() 

    jobs = get_jobs_to_process()
    
    if not jobs:
        log("No pending jobs found.")
        return

    log(f"Found {len(jobs)} jobs to process.")
    
    for job in jobs:
        process_job(job)
        
    log("Batch process completed.")

if __name__ == "__main__":
    main()
