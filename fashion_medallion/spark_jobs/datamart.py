import argparse

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_jobs.common import add_common_args, build_spark, parse_ingestion_date, setup_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Creation des datamarts depuis silver")
    parser.add_argument("--silver-base-path", required=True, help="Chemin de base silver")
    parser.add_argument("--datamart-base-path", required=True, help="Chemin de base des datamarts")
    add_common_args(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dt = parse_ingestion_date(args.ingestion_date)

    logger = setup_logger(args.log_file, "datamart")
    spark = build_spark("fashion-datamart")

    try:
        logger.info("Lecture silver")
        sales = spark.read.parquet(f"{args.silver_base_path}/fact_sales_enriched")
        customer_spend = spark.read.parquet(f"{args.silver_base_path}/agg_customer_spend")
        weekly_sales = spark.read.parquet(f"{args.silver_base_path}/agg_weekly_article_sales")

        logger.info("Creation datamart RFM")
        rfm = (
            customer_spend.withColumn(
                "days_since_last_purchase",
                F.datediff(F.to_date(F.lit(dt.strftime("%Y-%m-%d"))), F.col("last_purchase_date")),
            )
            .withColumn("recency_score", F.ntile(5).over(Window.orderBy(F.col("days_since_last_purchase").desc())))
            .withColumn("frequency_score", F.ntile(5).over(Window.orderBy(F.col("orders_count"))))
            .withColumn("monetary_score", F.ntile(5).over(Window.orderBy(F.col("total_spend"))))
            .withColumn("rfm_segment", F.concat_ws("", F.col("recency_score"), F.col("frequency_score"), F.col("monetary_score")))
        )

        logger.info("Creation datamart top articles hebdo")
        top_weekly_articles = (
            weekly_sales.withColumn(
                "rank_in_week",
                F.row_number().over(Window.partitionBy("week_start", "department_name").orderBy(F.col("units_sold").desc())),
            )
            .filter(F.col("rank_in_week") <= 20)
            .orderBy("week_start", "department_name", "rank_in_week")
        )

        logger.info("Creation datamart ventes par canal")
        sales_channel = (
            sales.groupBy("sales_channel_id", F.date_trunc("month", F.col("t_dat")).alias("month_start"))
            .agg(F.count("article_id").alias("units_sold"), F.sum("price").alias("revenue"))
            .orderBy("month_start", "sales_channel_id")
        )

        out = args.datamart_base_path
        rfm.write.mode("overwrite").parquet(f"{out}/dm_customer_rfm")
        top_weekly_articles.write.mode("overwrite").parquet(f"{out}/dm_top_weekly_articles")
        sales_channel.write.mode("overwrite").parquet(f"{out}/dm_sales_channel_monthly")

        logger.info("Datamart termine sans erreur")
    except Exception as exc:
        logger.error("Erreur datamart: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
