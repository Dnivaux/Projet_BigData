import argparse
from pyspark.sql import SparkSession
import logging

logging.basicConfig(filename='datamart.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def main():
    parser = argparse.ArgumentParser(description="Create Datamarts from Silver layer")
    parser.add_argument("--source", required=True, help="Path to silver layer")
    parser.add_argument("--db-url", required=True, help="JDBC URL for PostgreSQL")
    parser.add_argument("--db-user", required=True, help="Database User")
    parser.add_argument("--db-password", required=True, help="Database Password")
    args = parser.parse_args()

    logging.info("Starting Datamart Job")
    spark = SparkSession.builder.appName("Datamart").getOrCreate()

    try:
        logging.info("Reading Silver data")
        silver_df = spark.read.parquet(f"{args.source}/top_articles_by_age_group")
        
        # Prepare properties for JDBC
        properties = {
            "user": args.db_user,
            "password": args.db_password,
            "driver": "org.postgresql.Driver"
        }
        
        logging.info("Writing Datamart to PostgreSQL")
        # Write to PostgreSQL
        silver_df.write.jdbc(url=args.db_url, table="dm_top_articles", mode="overwrite", properties=properties)
        
        logging.info("Datamart Job Complete")
    except Exception as e:
        logging.error(f"Error in datamart creation: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    main()
