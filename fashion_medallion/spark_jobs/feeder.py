import argparse

from spark_jobs.common import add_common_args, build_spark, parse_ingestion_date, setup_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingestion brute vers couche raw")
    parser.add_argument("--articles-path", required=True, help="Chemin source articles.csv")
    parser.add_argument("--customers-path", required=True, help="Chemin source customers.csv")
    parser.add_argument("--transactions-path", required=True, help="Chemin source transactions_train.csv")
    parser.add_argument("--raw-base-path", required=True, help="Chemin de base raw")
    add_common_args(parser)
    return parser.parse_args()


def write_raw_table(spark, source_path: str, target_base: str, table_name: str, year: str, month: str, day: str, logger):
    logger.info("Lecture source %s", source_path)
    df = spark.read.option("header", True).option("inferSchema", True).csv(source_path)

    target_path = f"{target_base}/{table_name}/year={year}/month={month}/day={day}"
    logger.info("Ecriture raw %s -> %s", table_name, target_path)
    df.write.mode("overwrite").parquet(target_path)
    logger.info("Termine %s: %s lignes", table_name, df.count())


def main() -> None:
    args = parse_args()
    dt = parse_ingestion_date(args.ingestion_date)
    year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

    logger = setup_logger(args.log_file, "feeder")
    spark = build_spark("fashion-feeder")

    try:
        logger.info("Demarrage feeder pour date %s", dt.strftime("%Y-%m-%d"))
        write_raw_table(spark, args.articles_path, args.raw_base_path, "articles", year, month, day, logger)
        write_raw_table(spark, args.customers_path, args.raw_base_path, "customers", year, month, day, logger)
        write_raw_table(
            spark,
            args.transactions_path,
            args.raw_base_path,
            "transactions",
            year,
            month,
            day,
            logger,
        )
        logger.info("Feeder termine sans erreur")
    except Exception as exc:
        logger.error("Erreur feeder: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
