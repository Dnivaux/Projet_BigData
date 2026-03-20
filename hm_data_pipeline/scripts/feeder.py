import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, year, month, dayofmonth
import logging

logging.basicConfig(filename='feeder.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def main():
    parser = argparse.ArgumentParser(description="Ingest Data to Raw layer")
    parser.add_argument("--source", required=True, help="Path to source datasets folder")
    parser.add_argument("--destination", required=True, help="Path to raw layer")
    args = parser.parse_args()

    logging.info("Starting Feeder Job")
    spark = SparkSession.builder.appName("Feeder").getOrCreate()

    datasets = ["articles", "customers", "transactions_train"]
    
    for dataset in datasets:
        try:
            source_path = f"{args.source}/{dataset}.parquet"
            dest_path = f"{args.destination}/{dataset}"
            logging.info(f"Reading {dataset} from {source_path}")
            
            df = spark.read.parquet(source_path)
            
            # Add ingestion date columns for partitioning
            df = df.withColumn("ingestion_date", current_date()) \
                   .withColumn("year", year("ingestion_date")) \
                   .withColumn("month", month("ingestion_date")) \
                   .withColumn("day", dayofmonth("ingestion_date"))
            
            logging.info(f"Writing {dataset} to {dest_path} partitioned by year, month, day")
            df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(dest_path)
            logging.info(f"Successfully processed {dataset}")
        except Exception as e:
            logging.error(f"Error processing {dataset}: {str(e)}")

    spark.stop()
    logging.info("Feeder Job Completed")

if __name__ == "__main__":
    main()
