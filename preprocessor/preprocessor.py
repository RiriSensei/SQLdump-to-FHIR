import pandas as pd
import sqlite3
import json
import os
import logging
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLPreprocessor:
    def __init__(self, sql_dump_path):
        self.sql_dump_path = sql_dump_path
        self.output_dir = 'intermediate'
        
    def extract_tables(self):
        #Extract tables from SQL dump using pandas with chunk processing
        conn = sqlite3.connect(self.sql_dump_path)
        
        # List of tables to process
        tables = ['tb_emr_surgery_info', 'tb_encounter', 'tb_person_mtr', 'tb_mig_implant_description']
        
        processed_data = {}
        for table in tables:
            logger.info(f"Processing table: {table}")
            
            total_rows = pd.read_sql_query(f"SELECT COUNT(*) FROM {table}", conn).iloc[0, 0]
            logger.info(f"Total rows in {table}: {total_rows}")
            
            # Process in chunks
            chunk_size = 100000
            processed_records = []
            
            for chunk in pd.read_sql_query(f"SELECT * FROM {table}", conn, chunksize=chunk_size):
                # Clean the chunk
                chunk = self.clean_dataframe(chunk)
                processed_records.extend(chunk.to_dict(orient='records'))
                logger.info(f"Processed {len(processed_records)} rows of {table}")
            
            processed_data[table] = processed_records
        
        conn.close()
        return processed_data
    
    def clean_dataframe(self, df):
        # basic data cleaning
        df = df.replace([pd.NA, pd.NaT, None], 0)
        df = df.where(pd.notnull(df), 0)
        
        date_columns = df.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        
        return df
    
    def export_preprocessed_data(self, processed_data):
        #export preprocessed data to JSON for JavaScript processing
        os.makedirs(self.output_dir, exist_ok=True)
        
        for table, data in processed_data.items():
            #data = make_json_serializable(data)
            output_file = os.path.join(self.output_dir, f'{table}_preprocessed.json')
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Exported {len(data)} records to {output_file}")
        
        # Create completion marker file -> this file is a marker whose existence is the entrypoint for the javascript code container
        with open(os.path.join(self.output_dir, 'preprocessing_complete.txt'), 'w') as f:
            f.write('Preprocessing completed successfully')
        
        return True
"""
def make_json_serializable(data):
    #Recursively make data JSON serializable
    if isinstance(data, dict):
        return {key: make_json_serializable(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [make_json_serializable(item) for item in data]
    elif isinstance(data, (np.float64, np.float32, float)) and (np.isnan(data) or np.isinf(data)):
        return 0
    elif isinstance(data, (np.int64, np.int32)):
        return int(data)
    elif isinstance(data, pd.Timestamp):
        return data.strftime('%Y-%m-%d')
    return data
"""
def main():
    logger.info("Starting preprocessing...")
    preprocessor = SQLPreprocessor('/app/input/task.sqlite')
    
    try:
        processed_data = preprocessor.extract_tables()
        preprocessor.export_preprocessed_data(processed_data)
        logger.info("Preprocessing completed successfully")
    except Exception as e:
        logger.error(f"Preprocessing failed: {e}")
        raise

if __name__ == '__main__':
    main()
