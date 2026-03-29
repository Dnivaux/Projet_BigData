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

        logging.info("Validation ragles apppliquer")
        # 1. Prix > 0
        transactions = transactions.filter(col("price") > 0)
        # 2. pas null IDs
        transactions = transactions.filter(col("customer_id").isNotNull() & col("article_id").isNotNull())
        # 3. Age client Valide
        customers = customers.filter((col("age") > 0) & (col("age") < 120))
        # 4. Filtre actif clients
        customers = customers.filter(col("Active").isNotNull())
        # 5. Articles avec groupe de produit
        articles = articles.filter(col("product_group_name").isNotNull())


        logging.info("Joining, Aggregating and Windowing")
        
        # Create age groups
        customers = customers.withColumn("age_group", (col("age") / 10).cast("int") * 10)

        joined_df = transactions.join(customers, "customer_id", "inner") \
                               .join(articles, "article_id", "inner")
        joined_df.cache() # Optimize spark

        agg_df = joined_df.groupBy("age_group", "article_id", "product_group_name") \
                          .agg(count("*").alias("purchase_count"))
        

        windowSpec = Window.partitionBy("age_group").orderBy(desc("purchase_count"))
        ranked_df = agg_df.withColumn("rank", rank().over(windowSpec))
        
        top_10_df = ranked_df.filter(col("rank") <= 10)
        
        dest_path = f"{args.destination}/top_articles_by_age_group"
        logging.info(f"Writing Silver data to {dest_path}")
        top_10_df.write.mode("overwrite").partitionBy("age_group").parquet(dest_path)

        logging.info("Processor Job Complete")
    except Exception as e:
        logging.error(f"Error in processing: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    main()
