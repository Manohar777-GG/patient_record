import pandas as pd
from pymongo import MongoClient
import os
import requests
from datetime import datetime

# --- Configuration for Healthcare Domain ---
config = {
    # Data source configuration: 'local_csv', 'csv' (URL), or 'api'
    'source': 'local_csv',
    'local_patient_data_path': './data/raw/patients_data.csv',

    # Old URLs kept for reference if needed
    'patient_data_url': "https://gist.githubusercontent.com/gmaheswaranmca/298eb53d2673999551c6c58e17814a2f/raw/e3328e1b621e25779c16d00070281b674b01e74f/patients.csv",
    'api_url': "http://localhost:5000/patients",

    # File paths for raw and processed data
    'raw_data_dir': "./data/raw/",
    'processed_data_dir': "./data/processed/",
    'patients_csv_file': "./data/raw/patients.csv",
    'processed_patients_file': "./data/processed/patients_clean.csv",
    'processed_department_summary_file': "./data/processed/department_summary.csv",

    # --- IMPORTANT: UPDATE THIS WITH YOUR CORRECT MONGODB ATLAS CONNECTION STRING ---
    # The format is: "mongodb+srv://YOUR_USERNAME:YOUR_PASSWORD@your_cluster..."
    'mongo_db_uri': "mongodb+srv://man123:manoj621@cluster0.dxrfdac.mongodb.net/?retryWrites=true&w=majority",

    # Data Lake (raw data) MongoDB configuration
    'lake_db_name': "healthcare_lake_db",
    'lake_patients_collection': "patients",

    # Data Warehouse (processed data) MongoDB configuration
    'warehouse_db_name': "healthcare_warehouse_db",
    'warehouse_patients_collection': "patients_clean",
    'warehouse_department_summary_collection': "department_summary"
}

# --- Utility Function ---
def setup_directories(config):
    """Creates the data directories if they don't exist."""
    os.makedirs(config['raw_data_dir'], exist_ok=True)
    os.makedirs(config['processed_data_dir'], exist_ok=True)
    print("Data directories are set up.")

# --- EXTRACT PHASE ---
def extract(config):
    """Extracts patient data from the source specified in the config."""
    source = config['source']
    df = None
    print(f"Attempting to extract data from source: '{source}'")

    if source == 'local_csv':
        path = config['local_patient_data_path']
        try:
            df = pd.read_csv(path)
            print(f"Successfully extracted patient data from local file: {path}")
        except FileNotFoundError:
            print(f"Error: Local file not found at {path}. Please ensure the file exists.")
        except Exception as e:
            print(f"Error reading local file {path}: {e}")

    elif source == 'csv':
        url = config['patient_data_url']
        try:
            df = pd.read_csv(url)
            print(f"Successfully extracted patient data from CSV URL.")
        except Exception as e:
            print(f"Error extracting data from {url}: {e}")

    elif source == 'api':
        url = config['api_url']
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for bad status codes
            patients = response.json()
            df = pd.DataFrame(patients)
            print(f"Successfully extracted patient data from API: {url}")
        except Exception as e:
            print(f"Error extracting data from API {url}: {e}")

    return df

# --- TRANSFORM PHASE ---
def transform(raw_patients_df):
    """
    Transforms raw patient data by cleaning, enriching, and aggregating it.
    """
    if raw_patients_df is None:
        print("Transformation skipped as there is no data to transform.")
        return [None, None]

    print("Starting data transformation...")
    patients_clean_df = raw_patients_df.copy()

    # Calculate Patient Age
    patients_clean_df['dob'] = pd.to_datetime(patients_clean_df['dob'], errors='coerce')
    today = datetime.now()
    patients_clean_df['age'] = (today - patients_clean_df['dob']).dt.days / 365.25
    patients_clean_df['age'] = patients_clean_df['age'].fillna(0).astype(int)
    print("Enrichment: Calculated patient ages.")

    # Impute missing billing_amount with the mean of the department
    patients_clean_df['billing_amount'] = patients_clean_df.groupby('department')['billing_amount'].transform(
        lambda x: x.fillna(x.mean())
    )
    patients_clean_df['billing_amount'] = patients_clean_df['billing_amount'].round(2)
    print("Cleaning: Imputed missing billing amounts based on department average.")

    # Aggregate Data: Create a department summary
    department_summary_df = patients_clean_df.groupby('department').agg(
        number_of_patients=('patient_id', 'count'),
        total_revenue=('billing_amount', 'sum'),
        average_billing=('billing_amount', 'mean')
    ).reset_index()
    department_summary_df['average_billing'] = department_summary_df['average_billing'].round(2)
    print("Aggregation: Created department financial and patient summary.")
    
    print("Transformation complete.")
    return [patients_clean_df, department_summary_df]

# --- LOAD PHASE ---
def load_to_lake(df, config):
    """Loads the raw DataFrame into the Data Lake."""
    mongo_db_uri = config['mongo_db_uri']
    client = MongoClient(mongo_db_uri)
    db = client[config['lake_db_name']]
    collection = db[config['lake_patients_collection']]
    
    collection.delete_many({})
    collection.insert_many(df.to_dict('records'))
    
    print(f"Raw patient data loaded successfully into MongoDB Lake: '{config['lake_db_name']}.{config['lake_patients_collection']}'")

def load_to_warehouse(df_patients, df_summary, config):
    """Loads the transformed DataFrames into the Data Warehouse."""
    mongo_db_uri = config['mongo_db_uri']
    client = MongoClient(mongo_db_uri)
    db = client[config['warehouse_db_name']]

    # Load cleaned patients data
    collection_patients = db[config['warehouse_patients_collection']]
    collection_patients.delete_many({})
    collection_patients.insert_many(df_patients.to_dict('records'))
    print(f"Cleaned patient data loaded successfully into Warehouse: '{config['warehouse_db_name']}.{config['warehouse_patients_collection']}'")

    # Load department summary data
    collection_summary = db[config['warehouse_department_summary_collection']]
    collection_summary.delete_many({})
    collection_summary.insert_many(df_summary.to_dict('records'))
    print(f"Department summary loaded successfully into Warehouse: '{config['warehouse_db_name']}.{config['warehouse_department_summary_collection']}'")

def load(config, load_fn, dfs, is_lake=False):
    """Generic loader function."""
    if is_lake:
        if dfs[0] is not None:
            load_fn(dfs[0], config)
    else:
        if dfs[0] is not None and dfs[1] is not None:
            load_fn(dfs[0], dfs[1], config)

# --- Main ETL Orchestrator ---
def main():
    """Orchestrates the entire ETL process."""
    setup_directories(config)
    
    raw_patients_df = extract(config)
    
    if raw_patients_df is None:
        print("ETL process halted due to extraction failure.")
        return

    raw_patients_df.to_csv(config['patients_csv_file'], index=False)
    
    load(config, load_to_lake, [raw_patients_df], is_lake=True)
    
    transformed_dfs = transform(raw_patients_df)
    
    patients_clean_df, department_summary_df = transformed_dfs

    if patients_clean_df is not None and department_summary_df is not None:
        patients_clean_df.to_csv(config['processed_patients_file'], index=False)
        department_summary_df.to_csv(config['processed_department_summary_file'], index=False)
        
        load(config, load_to_warehouse, transformed_dfs, is_lake=False)
    
    print("\nETL process for healthcare data completed successfully!")

if __name__ == "__main__":
    main()