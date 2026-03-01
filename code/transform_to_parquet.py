import pandas as pd
import s3fs
import json
from datetime import datetime

# CONFIG
BUCKET_NAME = "zomato-data-platform"
RAW_FOLDER = f"{BUCKET_NAME}/raw/orders/"
PROCESSED_PATH = f"s3://{BUCKET_NAME}/processed/orders/"

def run_transformation():
    print("🚀 Starting Transformation: Raw -> Processed")
    
    # Initialize S3 File System
    fs = s3fs.S3FileSystem()
    
    try:
        # 1. List all JSON files in the raw folder
        # We look for files ending in .json
        file_list = fs.glob(f"s3://{RAW_FOLDER}*.json")
        
        if not file_list:
            print(f"❌ No JSON files found in s3://{RAW_FOLDER}. Check your S3 bucket!")
            return

        print(f"📂 Found {len(file_list)} JSON files. Loading data...")

        # 2. Read each file and combine them
        all_data = []
        for file_path in file_list:
            with fs.open(file_path, 'rb') as f:
                data = json.load(f)
                all_data.append(data)
        
        df = pd.DataFrame(all_data)

        # 3. DATA CLEANING & TYPE CASTING
        print("📊 Cleaning and transforming data...")
        df['order_time'] = pd.to_datetime(df['order_time'])
        df['amount'] = df['amount'].astype(float)
        df['transformed_at'] = datetime.now()

        # 4. WRITE TO S3 AS PARQUET
        file_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        target_file = f"{PROCESSED_PATH}orders_processed_{file_timestamp}.parquet"
        
        # Note: Index=False is important for clean SQL tables later
        df.to_parquet(target_file, engine='pyarrow', index=False)
        
        print(f"✅ SUCCESS: Transformed {len(df)} records into {target_file}")

    except Exception as e:
        print(f"⚠️ Error during transformation: {e}")

if __name__ == "__main__":
    run_transformation()