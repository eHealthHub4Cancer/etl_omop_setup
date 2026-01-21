import pandas as pd
from dotenv import load_dotenv
import os
import hashlib
from datetime import datetime
import pandas as pd
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()

folder_path = os.getenv("cdmCsvFolder", "./csv_data")

seed = 42  # Fixed seed for reproducibility
max_id = 99999999

column = ['care_site_id', 'location_id', 'person_id', 'observation_period_id', 
                  'visit_occurrence_id', 'condition_occurrence_id', 'preceding_visit_occurrence_id', 'drug_exposure_id', 
                  'dose_era_id', 'drug_era_id','condition_era_id', 'device_exposure_id', 'measurement_id', 'visit_detail_id', 'preceding_visit_detail_id',
                  'procedure_occurrence_id', 'observation_id','specimen_id'
                  ]

def hash_to_int(value, seed, max_id):
    hash_object = hashlib.md5(f"{seed}_{value}".encode())
    hash_int = int(hash_object.hexdigest(), 16)
    return (hash_int % max_id) + 1  # Ensure it's within the range and not zero

def parse_dates_safely(df):
    """
    Parse date columns with proper format handling
    Handles both DD/MM/YYYY and YYYY-MM-DD formats
    """
    # Common date column patterns in medical data
    date_columns = [col for col in df.columns if any(x in col.lower() 
                    for x in ['date', '_dt', 'datetime', 'time', 'birth', 'death', 'start', 'end'])]
    
    for col in date_columns:
        if col in df.columns and df[col].dtype == 'object':
            try:
                # Try parsing with dayfirst=True for DD/MM/YYYY format
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce', format='mixed')
                logger.info(f"Parsed date column '{col}' successfully")
            except Exception as e:
                logger.warning(f"Could not parse date column '{col}': {e}")
    
    return df

for file in os.listdir(folder_path):
    file_name = file.lower()
    if file_name.endswith('.csv'):
        file_path = os.path.join(folder_path, file_name)
        df = pd.read_csv(file_path)
        for col in column:
            if col in df.columns:
                print(f"Hashing column '{col}' in file '{file_name}'")
                df[col] = df[col].apply(lambda x: hash_to_int(x, seed, max_id))

        # Handle date columns
        df = parse_dates_safely(df)
        
        # Save the modified DataFrame back to a CSV file
        df.to_csv(file_path, index=False)
        print(f"Finished processing file: {file_name}")
