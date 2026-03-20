import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, rank, desc, sum as _sum
from pyspark.sql.window import Window
import logging

logging.basicConfig(filename='processor.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def main():
    parser = argparse.ArgumentParser(description="Process Data to Silver layer")
    parser.add_argument("--source", required=True, help="Path to raw layer")
    parser.add_argument("--destination", required=True, help="Path to silver layer")
    args = parser.parse_args()

    logging.info("Starting Processor Job")
    spark = SparkSession.builder.appName("Processor").getOrCreate()

    try:
        logging.info("Reading Raw data")
        articles = spark.read.parquet(f"{args.source}/articles")
        customers = spark.read.parquet(f"{args.source}/customers")
        transactions = spark.read.parquet(f"{args.source}/transactions_train")

        # Validation rules (5 rules)
        logging.info("Applying Validation Rules")
        # 1. Price > 0
        transactions = transactions.filter(col("price") > 0)
        # 2. Not null IDs
        transactions = transactions.filter(col("customer_id").isNotNull() & col("article_id").isNotNull())
        # 3. Valid customer age
        customers = customers.filter((col("age") > 0) & (col("age") < 120))
        # 4. Filter active customers
        customers = customers.filter(col("Active").isNotNull())
        # 5. Articles with valid product group
        articles = articles.filter(col("product_group_name").isNotNull())

        # Join, Aggregate, Window functions
        logging.info("Joining, Aggregating and Windowing")
        
        # We want to find the top products per customer age group
        # Create age groups
        customers = customers.withColumn("age_group", (col("age") / 10).cast("int") * 10)
        
        # Use cache since we use joined dataframe twice
        joined_df = transactions.join(customers, "customer_id", "inner") \
                               .join(articles, "article_id", "inner")
        joined_df.cache() # Optimize spark

        # Aggregation: count transactions per article in each age group
        agg_df = joined_df.groupBy("age_group", "article_id", "product_group_name") \
                          .agg(count("*").alias("purchase_count"))
        
        # Window function: rank the top articles within each age group
        windowSpec = Window.partitionBy("age_group").orderBy(desc("purchase_count"))
        ranked_df = agg_df.withColumn("rank", rank().over(windowSpec))
        
        # Keep top 10
        top_10_df = ranked_df.filter(col("rank") <= 10)
        
        # Partitioning by age_group for silver
        dest_path = f"{args.destination}/top_articles_by_age_group"
        logging.info(f"Writing Silver data to {dest_path}")
        top_10_df.write.mode("overwrite").partitionBy("age_group").parquet(dest_path)

        logging.info("Processor Job Complete")
    except Exception as e:
        logging.error(f"Error in processing: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    main()
