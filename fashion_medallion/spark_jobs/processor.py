import argparse
from datetime import datetime

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_jobs.common import add_common_args, build_spark, parse_ingestion_date, setup_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Traitement raw -> silver")
    parser.add_argument("--raw-base-path", required=True, help="Chemin de base raw")
    parser.add_argument("--silver-base-path", required=True, help="Chemin de base silver")
    add_common_args(parser)
    return parser.parse_args()


def partition_path(base_path: str, table: str, dt: datetime) -> str:
    return f"{base_path}/{table}/year={dt.strftime('%Y')}/month={dt.strftime('%m')}/day={dt.strftime('%d')}"


def validate_transactions(df: DataFrame, logger) -> DataFrame:
    total_before = df.count()

    out = (
        df.withColumn("t_dat", F.to_date("t_dat", "yyyy-MM-dd"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("sales_channel_id", F.col("sales_channel_id").cast("int"))
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("article_id").isNotNull())
        .filter(F.col("t_dat").isNotNull())
        .filter((F.col("price") > 0) & (F.col("price") < 1))
        .filter(F.col("sales_channel_id").isin(1, 2))
        .dropDuplicates(["t_dat", "customer_id", "article_id", "price", "sales_channel_id"])
    )

    total_after = out.count()
    logger.info("Validation transactions: %s -> %s lignes", total_before, total_after)
    return out


def validate_customers(df: DataFrame, logger) -> DataFrame:
    df_cast = df.withColumn("age", F.col("age").cast("int"))
    median_age = df_cast.approxQuantile("age", [0.5], 0.05)[0]

    out = (
        df_cast.withColumn("age", F.when((F.col("age") < 14) | (F.col("age") > 100), None).otherwise(F.col("age")))
        .fillna({"age": int(median_age) if median_age else 35})
        .fillna({"club_member_status": "UNKNOWN", "fashion_news_frequency": "UNKNOWN"})
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )

    logger.info("Validation customers: median_age=%s", median_age)
    return out


def validate_articles(df: DataFrame, logger) -> DataFrame:
    out = (
        df.filter(F.col("article_id").isNotNull())
        .fillna({
            "product_type_name": "UNKNOWN",
            "department_name": "UNKNOWN",
            "colour_group_name": "UNKNOWN",
            "garment_group_name": "UNKNOWN",
        })
        .dropDuplicates(["article_id"])
    )

    logger.info("Validation articles terminee")
    return out


def main() -> None:
    args = parse_args()
    dt = parse_ingestion_date(args.ingestion_date)
    year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    logger = setup_logger(args.log_file, "processor")
    spark = build_spark("fashion-processor")

    try:
        raw_articles = partition_path(args.raw_base_path, "articles", dt)
        raw_customers = partition_path(args.raw_base_path, "customers", dt)
        raw_transactions = partition_path(args.raw_base_path, "transactions", dt)

        logger.info("Lecture raw partition du jour")
        articles_df = spark.read.parquet(raw_articles)
        customers_df = spark.read.parquet(raw_customers)
        transactions_df = spark.read.parquet(raw_transactions)

        articles_clean = validate_articles(articles_df, logger)
        customers_clean = validate_customers(customers_df, logger)
        transactions_clean = validate_transactions(transactions_df, logger)

        sales_enriched = (
            transactions_clean.alias("t")
            .join(articles_clean.alias("a"), on="article_id", how="left")
            .join(customers_clean.alias("c"), on="customer_id", how="left")
            .withColumn("ingestion_year", F.lit(int(year)))
            .withColumn("ingestion_month", F.lit(int(month)))
            .withColumn("ingestion_day", F.lit(int(day)))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # Aggregation metier: depenses et commandes client.
        customer_spend = (
            sales_enriched.groupBy("customer_id")
            .agg(
                F.count("article_id").alias("orders_count"),
                F.sum("price").alias("total_spend"),
                F.avg("price").alias("avg_basket_price"),
                F.max("t_dat").alias("last_purchase_date"),
            )
            .withColumn("ingestion_year", F.lit(int(year)))
            .withColumn("ingestion_month", F.lit(int(month)))
            .withColumn("ingestion_day", F.lit(int(day)))
        )

        weekly_article_sales = (
            sales_enriched.withColumn("week_start", F.date_trunc("week", F.col("t_dat")))
            .groupBy("week_start", "article_id", "department_name")
            .agg(F.count("customer_id").alias("units_sold"), F.sum("price").alias("revenue"))
            .withColumn("ingestion_year", F.lit(int(year)))
            .withColumn("ingestion_month", F.lit(int(month)))
            .withColumn("ingestion_day", F.lit(int(day)))
        )

        # Window function: top articles recents par client.
        recent_customer_articles = (
            sales_enriched.withColumn("rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.col("t_dat").desc())))
            .filter(F.col("rn") <= 12)
            .drop("rn")
            .withColumn("ingestion_year", F.lit(int(year)))
            .withColumn("ingestion_month", F.lit(int(month)))
            .withColumn("ingestion_day", F.lit(int(day)))
        )

        base_silver = args.silver_base_path
        sales_enriched.write.mode("overwrite").partitionBy("ingestion_year", "ingestion_month", "ingestion_day").parquet(
            f"{base_silver}/fact_sales_enriched"
        )
        customer_spend.write.mode("overwrite").partitionBy("ingestion_year", "ingestion_month", "ingestion_day").parquet(
            f"{base_silver}/agg_customer_spend"
        )
        weekly_article_sales.write.mode("overwrite").partitionBy(
            "ingestion_year", "ingestion_month", "ingestion_day"
        ).parquet(f"{base_silver}/agg_weekly_article_sales")
        recent_customer_articles.write.mode("overwrite").partitionBy(
            "ingestion_year", "ingestion_month", "ingestion_day"
        ).parquet(f"{base_silver}/customer_recent_articles")

        logger.info("Processor termine sans erreur")
    except Exception as exc:
        logger.error("Erreur processor: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
