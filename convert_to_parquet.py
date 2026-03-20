import os
import pyarrow.csv as pv
import pyarrow.parquet as pq

DATASET_DIR = "Dataset"
PARQUET_DIR = "Dataset_Parquet"

if not os.path.exists(PARQUET_DIR):
    os.makedirs(PARQUET_DIR)

files_to_convert = [
    "articles.csv",
    "customers.csv",
    "transactions_train.csv"
]

for file in files_to_convert:
    csv_path = os.path.join(DATASET_DIR, file)
    parquet_filename = file.replace(".csv", ".parquet")
    parquet_path = os.path.join(PARQUET_DIR, parquet_filename)
    
    if os.path.exists(csv_path):
        print(f"Reading {csv_path}...")
        try:
            # For very large files like transactions_train.csv, pyarrow handles it much better than pandas
            parse_options = pv.ParseOptions(delimiter=",")
            table = pv.read_csv(csv_path, parse_options=parse_options)
            print(f"Writing to {parquet_path}...")
            pq.write_table(table, parquet_path, compression='snappy')
            print(f"Successfully converted {file} to Parquet.")
        except Exception as e:
            print(f"Error converting {file}: {e}")
    else:
        print(f"File {csv_path} not found.")

print("All tasks completed.")
